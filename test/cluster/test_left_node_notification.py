#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import pytest
import asyncio

from test.pylib.manager_client import ManagerClient
from test.cluster.util import check_token_ring_and_group0_consistency

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_left_node_notification(manager: ManagerClient) -> None:
    """
    Create a 3-node multi-DC cluster with 1 node in dc1 and 2 nodes in dc2.
    Then decommission both dc2 nodes, ensuring the topology remains consistent
    and the remaining node belongs to dc1 and there are only two 'left the cluster'
    notifications issued.
    """
    # Bootstrap 1 node in dc1 with storage_service debug logging (observer node)
    logger.info("Bootstrapping dc1 node with storage_service=debug")
    dc1_node = await manager.server_add(cmdline=["--logger-log-level", "storage_service=debug"],
                                        property_file={"dc": "dc1", "rack": "r1"})

    # Bootstrap 2 nodes in dc2 (same rack to avoid inter-rack tablet migration issues)
    logger.info("Bootstrapping dc2 nodes")
    dc2_node_a = await manager.server_add(property_file={"dc": "dc2", "rack": "r1"})
    dc2_node_b = await manager.server_add(property_file={"dc": "dc2", "rack": "r1"})

    # Ensure ring and group0 are consistent before operations
    await check_token_ring_and_group0_consistency(manager)

    # Decommission both dc2 nodes
    logger.info(f"Decommissioning dc2 node {dc2_node_b}")
    await manager.decommission_node(dc2_node_b.server_id)
    await check_token_ring_and_group0_consistency(manager)

    logger.info(f"Decommissioning dc2 node {dc2_node_a}")
    await manager.decommission_node(dc2_node_a.server_id)
    await check_token_ring_and_group0_consistency(manager)

    # Verify only dc1 node remains running
    running = await manager.running_servers()
    assert len(running) == 1, f"Expected 1 running server, found {len(running)}: {running}"
    assert running[0].datacenter == "dc1", f"Remaining node should be in dc1, got {running[0].datacenter}"
    logger.info("Successfully decommissioned both dc2 nodes; dc1 node remains running")

    # Check the remaining node's log contains exactly two 'Notify node … has left the cluster'
    log = await manager.server_open_log(dc1_node.server_id)
    left_msgs = await log.grep(r"Notify node .* has left the cluster")
    assert len(left_msgs) == 2, f"Expected exactly 2 'left the cluster' notifications, got {len(left_msgs)}"
