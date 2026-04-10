"""Regression test for idempotent lease completion events.

Issues fixed:
1. control-plane node 完成路径未 await 异步执行 (adapters.py)

History: Previously execute_control_plane_node was not async, causing coroutine
warnings and incorrect return values.
"""

import pytest
import asyncio
import sys
import warnings
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from app.harness_lab.fleet.lease_manager import LeaseManager
from app.harness_lab.types import (
    LeaseCompletionRequest,
    LeaseFailureRequest,
    WorkerLease,
    TaskAttempt,
    WorkerEventBatch,
    ResearchRun,
    ResearchSession,
    TaskGraph,
    TaskNode,
)
from app.harness_lab.utils import utc_now


class MockDatabase:
    """Mock database that tracks events."""
    
    def __init__(self):
        self.events = []
        self.leases = {}
        self.attempts = {}
        
    def append_event(self, event_type, payload, session_id=None, run_id=None):
        self.events.append({
            "event_type": event_type,
            "payload": payload,
            "session_id": session_id,
            "run_id": run_id,
        })
        
    def get_lease(self, lease_id):
        if lease_id not in self.leases:
            raise ValueError(f"Lease not found: {lease_id}")
        return self.leases[lease_id]
    
    def upsert_lease(self, lease):
        self.leases[lease.lease_id] = lease
        
    def get_attempt(self, attempt_id):
        if attempt_id not in self.attempts:
            raise ValueError(f"Attempt not found: {attempt_id}")
        return self.attempts[attempt_id]
    
    def upsert_attempt(self, attempt):
        self.attempts[attempt.attempt_id] = attempt


def create_mock_lease(status="completed"):
    """Create a mock lease with required fields."""
    now = datetime.now(timezone.utc).isoformat()
    lease = MagicMock(spec=WorkerLease)
    lease.lease_id = "lease_001"
    lease.worker_id = "worker_001"
    lease.run_id = "run_001"
    lease.task_node_id = "node_001"
    lease.attempt_id = "attempt_001"
    lease.status = status
    lease.expires_at = now
    lease.heartbeat_at = now
    lease.created_at = now
    lease.updated_at = now
    return lease


def create_mock_run():
    """Create a mock run."""
    run = MagicMock(spec=ResearchRun)
    run.run_id = "run_001"
    run.session_id = "session_001"
    run.status = "running"
    run.execution_trace = None
    run.result = {}
    return run


def create_mock_session(node_kind="execution"):
    """Create a mock session with task graph."""
    node = MagicMock(spec=TaskNode)
    node.node_id = "node_001"
    node.kind = node_kind
    node.status = "completed" if node_kind == "execution" else "running"
    
    task_graph = MagicMock(spec=TaskGraph)
    task_graph.nodes = [node]
    
    session = MagicMock(spec=ResearchSession)
    session.session_id = "session_001"
    session.task_graph = task_graph
    return session


@pytest.fixture
def mock_deps():
    """Create mock dependencies."""
    db = MockDatabase()
    db.leases["lease_001"] = create_mock_lease("completed")
    
    attempt = MagicMock(spec=TaskAttempt)
    attempt.attempt_id = "attempt_001"
    attempt.status = "completed"
    db.attempts["attempt_001"] = attempt
    
    coordination = MagicMock()
    coordination.advance_after_lease_transition = AsyncMock()
    
    constraints = MagicMock()
    constraints.worker_matches_node = MagicMock(return_value=True)
    constraint_result = MagicMock()
    constraint_result.queue_shard = None
    constraints.constraint_for_node = MagicMock(return_value=constraint_result)
    
    context = MagicMock()
    execution = MagicMock()
    utilities = MagicMock()
    utilities.utc_datetime = MagicMock(return_value=datetime.now(timezone.utc))
    
    return {
        "db": db,
        "coordination": coordination,
        "constraints": constraints,
        "context": context,
        "execution": execution,
        "utilities": utilities,
        "worker_registry": MagicMock(),
        "dispatch_queue": MagicMock(),
        "orchestrator": MagicMock(),
    }


@pytest.mark.asyncio
async def test_idempotent_complete_event_recorded(mock_deps):
    """Test that duplicate complete calls record ignored event with correct prefix."""
    deps = mock_deps
    run = create_mock_run()
    session = create_mock_session("execution")
    
    manager = LeaseManager(
        database=deps["db"],
        coordination=deps["coordination"],
        constraints=deps["constraints"],
        context=deps["context"],
        execution=deps["execution"],
        utilities=deps["utilities"],
        worker_registry=deps["worker_registry"],
        dispatch_queue=deps["dispatch_queue"],
        orchestrator=deps["orchestrator"],
    )
    
    manager._get_run = MagicMock(return_value=run)
    manager._get_session = MagicMock(return_value=session)
    manager._get_node = MagicMock(return_value=session.task_graph.nodes[0])
    
    request = LeaseCompletionRequest(
        worker_event_batch=WorkerEventBatch(lease_id="lease_001", emitted_at=utc_now()),
        summary="Test completion",
    )
    
    await manager.complete_lease("lease_001", request)
    
    event_types = [e["event_type"] for e in deps["db"].events]
    assert "lease.complete_ignored" in event_types


@pytest.mark.asyncio
async def test_idempotent_fail_event_recorded(mock_deps):
    """Test that fail on already-completed lease records ignored event with correct prefix."""
    deps = mock_deps
    run = create_mock_run()
    session = create_mock_session("execution")
    
    manager = LeaseManager(
        database=deps["db"],
        coordination=deps["coordination"],
        constraints=deps["constraints"],
        context=deps["context"],
        execution=deps["execution"],
        utilities=deps["utilities"],
        worker_registry=deps["worker_registry"],
        dispatch_queue=deps["dispatch_queue"],
        orchestrator=deps["orchestrator"],
    )
    
    manager._get_run = MagicMock(return_value=run)
    manager._get_session = MagicMock(return_value=session)
    
    request = LeaseFailureRequest(
        error="Something went wrong",
        worker_event_batch=WorkerEventBatch(lease_id="lease_001", emitted_at=utc_now()),
    )
    
    await manager.fail_lease("lease_001", request)
    
    event_types = [e["event_type"] for e in deps["db"].events]
    assert "lease.fail_ignored" in event_types


@pytest.mark.asyncio
async def test_control_plane_node_execution_awaited(mock_deps):
    """Test that control-plane node execution is properly awaited (no coroutine warnings)."""
    deps = mock_deps
    
    # Use active lease to trigger normal completion path
    deps["db"].leases["lease_001"] = create_mock_lease("leased")
    deps["db"].attempts["attempt_001"].status = "leased"
    
    run = create_mock_run()
    session = create_mock_session("context")  # control-plane type
    
    call_log = []
    
    async def mock_execute_control_plane(node, run_arg, session_arg):
        call_log.append("executed")
        await asyncio.sleep(0)
        return {"result": "success"}
    
    deps["execution"].execute_control_plane_node = mock_execute_control_plane
    
    manager = LeaseManager(
        database=deps["db"],
        coordination=deps["coordination"],
        constraints=deps["constraints"],
        context=deps["context"],
        execution=deps["execution"],
        utilities=deps["utilities"],
        worker_registry=deps["worker_registry"],
        dispatch_queue=deps["dispatch_queue"],
        orchestrator=deps["orchestrator"],
    )
    
    manager._get_run = MagicMock(return_value=run)
    manager._get_session = MagicMock(return_value=session)
    manager._get_node = MagicMock(return_value=session.task_graph.nodes[0])
    
    request = LeaseCompletionRequest(
        worker_event_batch=WorkerEventBatch(lease_id="lease_001", emitted_at=utc_now()),
        summary="Test completion",
    )
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        await manager.complete_lease("lease_001", request)
        
        await_warnings = [x for x in w if "never awaited" in str(x.message).lower()]
        assert len(await_warnings) == 0, f"Got 'never awaited' warnings"
    
    assert "executed" in call_log
