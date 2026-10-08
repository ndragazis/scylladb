#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for moving the distributed system keyspaces from vnodes to tablets."""

import asyncio
import logging
import time

import pytest
from cassandra import Unauthorized

from test.cluster.system_ks_tablets_util import (
    SYSTEM_KS_TABLES, maintenance_session, table_ids, keyspace_uses_tablets, tablets_ks_options,
    wait_for_tables, traced_query, audited_statement, read_system_distributed, row_count,
    assert_survives_rolling_restart, enable_trace_and_audit_probes, AUDIT_PROBE_KS,
    replication_class, repair_on_all_nodes, check_keyspace_works, migrate_to_tablets)
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


async def drop_and_recreate(manager: ScyllaClusterManager, servers, ks: str) -> dict[str, str]:
    """Drop `ks` over the maintenance socket and recreate it with tablets.
    Return the table IDs from before the drop."""
    old_ids = await table_ids(manager.get_cql(), ks)
    assert set(old_ids) == SYSTEM_KS_TABLES[ks]
    ms = await maintenance_session(manager, servers[0])
    try:
        ms.session.execute(f"DROP KEYSPACE {ks}")
        ms.session.execute(f"CREATE KEYSPACE {ks} {tablets_ks_options(3)}")
    finally:
        ms.close()
    return old_ids


@pytest.mark.prepare_3_racks_cluster
async def test_drop_recreate_system_traces(manager: ScyllaClusterManager):
    """system_traces cannot be dropped over a regular connection, but can over
    the maintenance socket. Once recreated with tablets, the first traced query
    recreates its tables with the original IDs, without a restart."""
    servers = await manager.running_servers()
    cql = manager.get_cql()

    with pytest.raises(Unauthorized, match="Cannot DROP"):
        await cql.run_async("DROP KEYSPACE system_traces")

    old_ids = await drop_and_recreate(manager, servers, "system_traces")
    await wait_for_tables(manager, "system_traces", trigger=lambda: asyncio.to_thread(traced_query, cql))

    assert await keyspace_uses_tablets(cql, "system_traces")
    assert await table_ids(cql, "system_traces") == old_ids
    traced_query(cql)
    await assert_survives_rolling_restart(manager, servers, "system_traces", old_ids)


@pytest.mark.prepare_3_racks_cluster
async def test_drop_recreate_audit(manager: ScyllaClusterManager):
    """audit can be dropped over a regular connection. Once recreated with
    tablets, the first audited statement recreates audit_log with its
    original ID, without a restart."""
    servers = await manager.running_servers()
    cql = manager.get_cql()
    old_ids = await table_ids(cql, "audit")
    assert set(old_ids) == SYSTEM_KS_TABLES["audit"]

    await cql.run_async("DROP KEYSPACE audit")
    await cql.run_async(f"CREATE KEYSPACE audit {tablets_ks_options(3)}")
    await wait_for_tables(manager, "audit", trigger=lambda: cql.run_async(f"INSERT INTO {AUDIT_PROBE_KS}.t (pk) VALUES (1)"))

    assert await keyspace_uses_tablets(cql, "audit")
    assert await table_ids(cql, "audit") == old_ids
    await audited_statement(cql)
    await assert_survives_rolling_restart(manager, servers, "audit", old_ids)


@pytest.mark.prepare_3_racks_cluster
async def test_drop_recreate_system_distributed(manager: ScyllaClusterManager):
    """system_distributed tables come back only when a node starts. Their CDC
    content does not come back until the next CDC generation, which a
    topology change publishes into the new tablet keyspace."""
    servers = await manager.running_servers()
    cql = manager.get_cql()
    cdc_gens_before = await row_count(cql, "system_distributed", "cdc_generation_timestamps")
    logger.info(f"CDC generations published before the drop: {cdc_gens_before}")

    old_ids = await table_ids(cql, "system_distributed")
    try:
        await cql.run_async("DROP KEYSPACE system_distributed")
        dropped = True
    except Unauthorized as e:
        logger.info(f"DROP KEYSPACE system_distributed rejected over a regular connection: {e}")
        dropped = False
    logger.info(f"Dropped over a regular connection: {dropped}")

    await recreate_system_distributed_with_tablets(manager, servers, already_dropped=dropped)
    cql = manager.get_cql()

    assert await keyspace_uses_tablets(cql, "system_distributed")
    ids = await table_ids(cql, "system_distributed")
    assert ids == old_ids
    await read_system_distributed(cql)

    if cdc_gens_before:
        assert await row_count(cql, "system_distributed", "cdc_generation_timestamps") == 0, \
            "CDC generations reappeared without a topology change"
        servers.append(await manager.server_add(property_file=servers[0].property_file()))
        cql = await reconnect_driver(manager)
        await manager.get_ready_cql(servers)

        async def republished():
            return True if await row_count(cql, "system_distributed", "cdc_generation_timestamps") > 0 else None
        await wait_for(republished, time.time() + 60)

    await assert_survives_rolling_restart(manager, servers, "system_distributed", ids)


async def seed(manager: ScyllaClusterManager, ks: str) -> None:
    """Put some data into `ks` the way Scylla normally does."""
    cql = manager.get_cql()
    if ks == "system_traces":
        for _ in range(5):
            traced_query(cql)
    elif ks == "audit":
        for _ in range(5):
            await audited_statement(cql)
    else:
        # A materialized view gives view_build_status content to serve;
        # the CDC tables already hold the generation published at bootstrap.
        await cql.run_async("CREATE KEYSPACE seed_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
        await cql.run_async("CREATE TABLE seed_ks.t (pk int PRIMARY KEY, v int)")
        await cql.run_async("CREATE MATERIALIZED VIEW seed_ks.mv AS SELECT * FROM seed_ks.t "
                            "WHERE v IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (v, pk)")


async def row_counts(cql, ks: str) -> dict[str, int]:
    return {t: await row_count(cql, ks, t) for t in sorted(SYSTEM_KS_TABLES[ks])}


def assert_no_rows_lost(before: dict[str, int], after: dict[str, int]) -> None:
    # Scylla keeps writing to these tables during the test, so they may grow.
    for t, n in before.items():
        assert after[t] >= n, f"{t}: {n} rows before, {after[t]} after"


@pytest.mark.prepare_3_racks_cluster
@pytest.mark.parametrize("ks", ["system_traces", "audit", "system_distributed"])
async def test_migrate_system_ks_to_tablets(manager: ScyllaClusterManager, ks: str):
    """Convert the keyspace to NetworkTopologyStrategy if needed, repair, and
    run the vnodes-to-tablets migration. The keyspace stays usable while the
    migration is in progress, and keeps its data and table IDs."""
    servers = await manager.running_servers()
    cql = manager.get_cql()
    await seed(manager, ks)
    before = await row_counts(cql, ks)
    old_ids = await table_ids(cql, ks)
    logger.info(f"{ks} rows before the migration: {before}")

    if await replication_class(cql, ks) != "org.apache.cassandra.locator.NetworkTopologyStrategy":
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = "
                            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
        await repair_on_all_nodes(manager, servers, ks)

    async def still_works(cql):
        await check_keyspace_works(cql, ks)
    cql = await migrate_to_tablets(manager, servers, ks, between_restarts=still_works)

    assert await table_ids(cql, ks) == old_ids
    assert_no_rows_lost(before, await row_counts(cql, ks))
    await assert_survives_rolling_restart(manager, servers, ks, old_ids)

