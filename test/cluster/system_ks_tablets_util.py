#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Helpers for tests that move the distributed system keyspaces
(system_traces, audit, system_distributed) from vnodes to tablets."""

import glob
import logging
import os
import shutil
import time

from cassandra.cluster import Session
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement, ConsistencyLevel

from test.cluster.util import reconnect_driver
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

SYSTEM_KS_TABLES = {
    "system_traces": {"sessions", "sessions_time_idx", "events", "node_slow_log", "node_slow_log_time_idx"},
    "audit": {"audit_log"},
    "system_distributed": {"view_build_status", "cdc_streams_descriptions_v2", "cdc_generation_timestamps",
                           "snapshots", "snapshot_keyspaces", "snapshot_tables", "snapshot_tablets",
                           "snapshot_nodes", "snapshot_sstables", "snapshot_remote_locations"},
}


def tablets_ks_options(rf: int) -> str:
    """Replication options for recreating a system keyspace with tablets.
    A single initial tablet per table, since these tables hold very little data."""
    return (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
            " AND tablets = {'enabled': true, 'initial': 1}")


class MaintenanceSession:
    """A CQL session over a node's maintenance socket, which bypasses
    the access rules (e.g. the ban on dropping system_traces)."""

    def __init__(self, cluster, session: Session):
        self.cluster = cluster
        self.session = session

    def close(self):
        safe_driver_shutdown(self.cluster)


async def maintenance_session(manager: ScyllaClusterManager, server: ServerInfo, timeout: int = 60) -> MaintenanceSession:
    socket_path = await manager.server_get_maintenance_socket_path(server.server_id)
    endpoint = UnixSocketEndPoint(socket_path)

    async def try_connect():
        c = manager.con_gen([endpoint], load_balancing_policy=WhiteListRoundRobinPolicy([endpoint]))
        try:
            s = c.connect()
            s.execute("SELECT key FROM system.local LIMIT 1")
            return MaintenanceSession(c, s)
        except Exception:
            safe_driver_shutdown(c)
            return None
    return await wait_for(try_connect, time.time() + timeout)


async def table_ids(cql: Session, ks: str) -> dict[str, str]:
    rows = await cql.run_async(f"SELECT table_name, id FROM system_schema.tables WHERE keyspace_name = '{ks}'")
    return {r.table_name: str(r.id) for r in rows}


async def keyspace_uses_tablets(cql: Session, ks: str) -> bool:
    rows = await cql.run_async(f"SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{ks}'")
    return len(rows) == 1 and rows[0].initial_tablets is not None


async def replication_class(cql: Session, ks: str) -> str:
    rows = await cql.run_async(f"SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'")
    assert len(rows) == 1, f"Keyspace {ks} not found"
    return rows[0].replication["class"]


async def row_count(cql: Session, ks: str, table: str) -> int:
    stmt = SimpleStatement(f"SELECT * FROM {ks}.{table}", consistency_level=ConsistencyLevel.ALL)
    return len(await cql.run_async(stmt))


AUDIT_PROBE_KS = "audit_probe"


async def enable_trace_and_audit_probes(manager: ScyllaClusterManager, servers: list[ServerInfo]) -> None:
    """Make every node write to all tables of system_traces and audit.

    The slow-query log is the only writer of system_traces.node_slow_log and
    node_slow_log_time_idx; a threshold of 1us logs every traced query.
    The test suite uses AllowAllAuthenticator, so DCL statements fail as
    anonymous; audit a dedicated keyspace instead."""
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE IF NOT EXISTS {AUDIT_PROBE_KS} WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await cql.run_async(f"CREATE TABLE IF NOT EXISTS {AUDIT_PROBE_KS}.t (pk int PRIMARY KEY)")
    for s in servers:
        await manager.server_update_config(s.server_id, config_options={
            "audit_keyspaces": AUDIT_PROBE_KS, "audit_categories": "DCL,DDL,AUTH,ADMIN,DML"})
        await manager.api.client.post("/storage_service/slow_query", host=s.ip_addr,
                                      params={"enable": "true", "threshold": "1", "fast": "false"})


def traced_query(cql: Session) -> None:
    """Run a traced query and wait until its trace is readable."""
    rs = cql.execute("SELECT key FROM system.local", trace=True)
    trace = rs.get_query_trace(max_wait_sec=30)
    assert trace.events, "The traced query has no trace events"


async def audited_statement(cql: Session) -> int:
    """Run a statement that the audit probe config logs and return the
    number of audit rows seen afterwards."""
    before = await row_count(cql, "audit", "audit_log")
    await cql.run_async(f"INSERT INTO {AUDIT_PROBE_KS}.t (pk) VALUES (1)")

    async def grown():
        n = await row_count(cql, "audit", "audit_log")
        return n if n > before else None
    return await wait_for(grown, time.time() + 30)


async def read_system_distributed(cql: Session) -> None:
    for table in ("view_build_status", "cdc_streams_descriptions_v2", "cdc_generation_timestamps"):
        await cql.run_async(f"SELECT * FROM system_distributed.{table}")


async def check_keyspace_works(cql: Session, ks: str) -> None:
    """Exercise the keyspace the way Scylla and its clients use it."""
    if ks == "system_traces":
        traced_query(cql)
    elif ks == "audit":
        await audited_statement(cql)
    else:
        await read_system_distributed(cql)


async def wait_for_tables(manager: ScyllaClusterManager, ks: str, trigger=None, timeout: int = 60) -> None:
    """Wait for all tables of `ks` to exist on every node, calling `trigger`
    on every attempt. For system_traces and audit, Scylla recreates a missing
    table lazily on the first write that hits it."""
    cql = manager.get_cql()
    async def all_present():
        if trigger:
            try:
                await trigger()
            except Exception as e:
                logger.info(f"Trigger for {ks} failed (expected while tables are missing): {e}")
        present = set((await table_ids(cql, ks)).keys())
        return True if SYSTEM_KS_TABLES[ks] <= present else None
    await wait_for(all_present, time.time() + timeout)
    # The driver saw the tables on one node; make every node apply them.
    for s in await manager.running_servers():
        await read_barrier(manager.api, s.ip_addr)


async def assert_survives_rolling_restart(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str,
                                          expected_ids: dict[str, str]) -> Session:
    """Restart every node and verify that startup code neither recreates
    nor converts the keyspace back to vnodes."""
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]
    await manager.rolling_restart(servers)
    cql, _ = await manager.get_ready_cql(servers)
    for log, mark in zip(logs, marks):
        assert not await log.grep(f"Creating keyspace {ks}$", from_mark=mark), \
            f"A node recreated keyspace {ks} on restart"
    assert await keyspace_uses_tablets(cql, ks), f"Keyspace {ks} no longer uses tablets after restart"
    assert await table_ids(cql, ks) == expected_ids, f"Table IDs of {ks} changed after restart"
    await check_keyspace_works(cql, ks)
    return cql


async def migrate_to_tablets(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str,
                             between_restarts=None) -> Session:
    """Run the vnodes-to-tablets migration of `ks`: create the tablet maps,
    switch every node to tablets with a restart, then finalize. Call
    `between_restarts(cql)` after each restart, while the migration is in
    progress."""
    await manager.api.create_vnode_tablet_migration(servers[0].ip_addr, ks)
    for s in servers:
        await manager.api.upgrade_node_to_tablets(s.ip_addr)
        await manager.server_restart(s.server_id)
        cql = await reconnect_driver(manager)
        await manager.get_ready_cql(servers)
        if between_restarts:
            await between_restarts(cql)
    await manager.api.finalize_vnode_tablet_migration(servers[0].ip_addr, ks)
    cql = manager.get_cql()

    async def finalized():
        return True if await keyspace_uses_tablets(cql, ks) else None
    await wait_for(finalized, time.time() + 60)
    return cql


async def repair_on_all_nodes(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str) -> None:
    for s in servers:
        await manager.api.repair_and_wait(s.ip_addr, ks)


def snapshot_dirs(workdir: str, ks: str, table: str, tag: str) -> list[str]:
    return glob.glob(os.path.join(workdir, "data", ks, f"{table}-*", "snapshots", tag))


async def restore_from_snapshot(manager: ScyllaClusterManager, servers: list[ServerInfo], ks: str, tag: str) -> None:
    """Copy each node's snapshot of `ks` into the upload directory of the
    recreated tables and load it with load-and-stream, which sends every
    partition to its current replicas regardless of the old token ownership."""
    # Every node must have applied the creation of the tables before
    # receiving streamed sstables, not only the coordinator the driver uses.
    for s in servers:
        await read_barrier(manager.api, s.ip_addr)
    for s in servers:
        workdir = await manager.server_get_workdir(s.server_id)
        for table in SYSTEM_KS_TABLES[ks]:
            sources = snapshot_dirs(workdir, ks, table, tag)
            assert len(sources) == 1, f"Expected one snapshot of {ks}.{table} on node {s.server_id}, found {sources}"
            # The recreated table has the same ID, hence the same directory.
            upload = os.path.join(os.path.dirname(os.path.dirname(sources[0])), "upload")
            os.makedirs(upload, exist_ok=True)
            for f in os.listdir(sources[0]):
                if f != "manifest.json" and f != "schema.cql":
                    shutil.copy(os.path.join(sources[0], f), upload)
            await manager.api.load_new_sstables(s.ip_addr, ks, table, load_and_stream=True)
