#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for moving the distributed system keyspaces from vnodes to tablets."""

import logging
import time

import pytest

from test.cluster.system_ks_tablets_util import (
    maintenance_session, table_ids, keyspace_uses_tablets, tablets_ks_options, wait_for_tables,
    enable_trace_and_audit_probes)
from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
async def probes(manager: ScyllaClusterManager):
    await enable_trace_and_audit_probes(manager, await manager.running_servers())


async def recreate_system_distributed_with_tablets(manager: ScyllaClusterManager, servers, already_dropped: bool = False) -> dict[str, str]:
    """Drop system_distributed, recreate it with tablets and restart the
    nodes to create its tables.
    Return the table IDs from before the drop."""
    old_ids = await table_ids(manager.get_cql(), "system_distributed") if not already_dropped else None
    ms = await maintenance_session(manager, servers[0])
    try:
        if not already_dropped:
            ms.session.execute("DROP KEYSPACE system_distributed")
        ms.session.execute(f"CREATE KEYSPACE system_distributed {tablets_ks_options(3)}")
    finally:
        ms.close()
    # Only startup creates the tables, and only startup installs the virtual
    # reader of view_build_status on each node's table object, so every node
    # needs a restart, not only the one that creates the tables.
    await manager.rolling_restart(servers)
    cql = await reconnect_driver(manager)
    await manager.get_ready_cql(servers)
    await wait_for_tables(manager, "system_distributed")
    return old_ids


@pytest.mark.prepare_3_racks_cluster
async def test_view_build_status_with_tablets(manager: ScyllaClusterManager):
    """Read system_distributed.view_build_status after system_distributed
    switches to tablets. The table is a virtual facade over
    system.view_build_status_v2 whose reader used the static schema sharder,
    which does not exist for tablet tables."""
    servers = await manager.running_servers()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv AS SELECT * FROM {ks}.t "
                            "WHERE v IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (v, pk)")

        async def view_built():
            rows = await cql.run_async(f"SELECT status FROM system.view_build_status_v2 WHERE keyspace_name = '{ks}' ALLOW FILTERING")
            return True if len(rows) == len(servers) and all(r.status == "SUCCESS" for r in rows) else None
        await wait_for(view_built, time.time() + 60)

        await recreate_system_distributed_with_tablets(manager, servers)
        cql = manager.get_cql()
        assert await keyspace_uses_tablets(cql, "system_distributed")

        rows = await cql.run_async(f"SELECT host_id, status FROM system_distributed.view_build_status WHERE keyspace_name = '{ks}' AND view_name = 'mv'")
        v2_rows = await cql.run_async(f"SELECT host_id, status FROM system.view_build_status_v2 WHERE keyspace_name = '{ks}' AND view_name = 'mv'")
        assert sorted(rows) == sorted(v2_rows)
        assert len(rows) == len(servers)

