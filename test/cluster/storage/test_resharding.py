#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for resharding behavior when disk space is limited."""

import asyncio
import logging
import pytest
import time

from typing import Callable

from test.cluster.util import reconnect_driver
from test.pylib.manager_client import ManagerClient, wait_for_cql_and_get_hosts
from test.cluster.storage.conftest import space_limited_servers

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_resharding_with_critical_disk_utilization(manager: ManagerClient, volumes_factory: Callable) -> None:
    """Verify node behavior when resharding hits critical disk utilization.

    Start a node with 2 shards on a 300M volume, fill it with ~120M of
    SSTable data, then restart with 3 shards to trigger resharding.
    Resharding temporarily doubles the data on disk (reads shared SSTables
    and writes new single-shard SSTables before deleting originals), which
    should push the disk past the critical utilization threshold.

    We then check whether:
    - The node starts successfully.
    - All previously written data is still readable.
    - The table accepts new writes.
    """
    cmdline = [
        "--smp=2",
        "--disk-space-monitor-normal-polling-interval-in-seconds", "1",
        "--disk-space-monitor-high-polling-interval-in-seconds", "1",
        "--critical-disk-utilization-level", "0.75",
        "--commitlog-segment-size-in-mb", "2",
        "--schema-commitlog-segment-size-in-mb", "4",
    ]

    async with space_limited_servers(manager, volumes_factory, ["300M"], cmdline=cmdline) as servers:
        server = servers[0]
        cql = manager.get_cql()

        ks = "test_ks"
        await cql.run_async(
            f"CREATE KEYSPACE {ks} WITH replication ="
            f" {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
            f" AND tablets = {{'enabled': false}}")

        await cql.run_async(
            f"CREATE TABLE {ks}.test_tbl (pk int PRIMARY KEY, v text)"
            f" WITH compaction = {{'class': 'SizeTieredCompactionStrategy'}}"
            f" AND compression = {{'sstable_compression': ''}}")

        await manager.api.disable_autocompaction(server.ip_addr, ks)

        # Write ~120M of data: ~120K rows with ~1KB values, flushing every
        # 30K rows to create multiple SSTables.
        value = "x" * 1020
        n_rows = 120_000
        batch_size = 30_000

        logger.info("Inserting %d rows (~120M of data)", n_rows)
        for batch_start in range(0, n_rows, batch_size):
            batch_end = min(batch_start + batch_size, n_rows)
            await asyncio.gather(*[
                cql.run_async(f"INSERT INTO {ks}.test_tbl (pk, v) VALUES ({k}, '{value}')")
                for k in range(batch_start, batch_end)
            ])
            await manager.api.flush_keyspace(server.ip_addr, ks)
            logger.info("Flushed batch [%d, %d)", batch_start, batch_end)

        # Verify data before restart
        rows = await cql.run_async(f"SELECT count(*) FROM {ks}.test_tbl")
        assert rows[0].count == n_rows, f"Expected {n_rows} rows before restart, got {rows[0].count}"
        logger.info("Verified %d rows before restart", n_rows)

        # Stop the node and change shard count to trigger resharding on restart
        logger.info("Stopping node and changing shard count from 2 to 3")
        await manager.server_stop_gracefully(server.server_id, timeout=120)
        await manager.server_update_cmdline(server.server_id, ["--smp=3"])

        # Start the node — resharding should be triggered for the user table
        logger.info("Starting node with --smp=3, expecting resharding")
        await manager.server_start(server.server_id)
        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 120)

        # Check that the table accepts writes
        new_pk = n_rows + 1  # certainly not in the original dataset
        await cql.run_async(f"INSERT INTO {ks}.test_tbl (pk, v) VALUES ({new_pk}, 'after_resharding')")
        rows = await cql.run_async(f"SELECT v FROM {ks}.test_tbl WHERE pk = {new_pk}")
        assert len(rows) == 1 and rows[0].v == "after_resharding", "Failed to read back post-resharding write"
        logger.info("Post-resharding write succeeded")

        # Check that all data survived — both the original rows and the new one
        rows = await cql.run_async(f"SELECT count(*) FROM {ks}.test_tbl")
        assert rows[0].count == n_rows + 1, f"Expected {n_rows + 1} rows after restart, got {rows[0].count}"
        logger.info("Verified %d rows after restart", rows[0].count)
