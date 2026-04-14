"""Protocols for Runtime/Fleet interface extraction.

These protocols define the boundary between fleet coordination
and runtime services, enabling LeaseManager migration.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from ..types import (
    DispatchConstraint,
    DispatchEnvelope,
    ResearchRun,
    ResearchSession,
    TaskNode,
    WorkerSnapshot,
)


@runtime_checkable
class RunCoordinationProtocol(Protocol):
    """Protocol for advancing run state after attempt transitions."""

    def advance_after_lease_transition(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
    ) -> ResearchRun:
        """Advance run state after lease completion/failure/release.
        
        This replaces the implicit coupling to RunCoordinator.after_lease_transition().
        """
        ...

    def mark_ready_nodes(self, session: ResearchSession, run_id: str) -> bool:
        """Mark nodes ready for dispatch and return if any changed."""
        ...

    def record_handoffs(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
    ) -> List[Any]:
        """Record handoff packets for a completed node.
        
        This enables fleet layer to trigger handoff recording without
        depending on RuntimeService private methods.
        
        Args:
            run: ResearchRun being processed
            session: ResearchSession containing task graph
            node: TaskNode that was completed
            
        Returns:
            List of HandoffPacket objects created
        """
        ...


@runtime_checkable
class DispatchConstraintProtocol(Protocol):
    """Protocol for calculating dispatch constraints and blockers."""

    def constraint_for_node(
        self,
        node: TaskNode,
        session: ResearchSession,
    ) -> DispatchConstraint:
        """Calculate dispatch constraints for a task node.
        
        Replaces RuntimeService._dispatch_constraint_for_node().
        """
        ...

    def worker_matches_node(
        self,
        worker: WorkerSnapshot,
        session: ResearchSession,
        node: TaskNode,
    ) -> bool:
        """Check if worker can execute the given node.
        
        Replaces RuntimeService._worker_matches_node().
        """
        ...

    def list_dispatch_blockers(
        self,
        run: ResearchRun,
        session: ResearchSession,
    ) -> List[Dict[str, Any]]:
        """List reasons why ready nodes cannot be dispatched.
        
        Replaces RuntimeService._dispatch_blockers_for_run().
        """
        ...


@runtime_checkable
class DispatchContextProtocol(Protocol):
    """Protocol for building dispatch context/envelope."""

    def build_dispatch(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
        worker: WorkerSnapshot,
        lease_id: str,
        attempt_id: str,
    ) -> DispatchEnvelope:
        """Build dispatch envelope for worker.
        
        Replaces LeaseManager.create_dispatch() inner logic.
        """
        ...

    def get_approval_token(self, run_id: str) -> Optional[str]:
        """Get approval token for run if approved."""
        ...


@runtime_checkable
class TaskExecutionProtocol(Protocol):
    """Protocol for task execution and result handling."""

    def apply_execution_success(
        self,
        run: ResearchRun,
        session: ResearchSession,
        event_batch: Optional[Any],  # WorkerEventBatch
    ) -> None:
        """Apply successful execution results to run."""
        ...

    def apply_execution_failure(
        self,
        run: ResearchRun,
        session: ResearchSession,
        error: str,
        event_batch: Optional[Any],  # WorkerEventBatch
    ) -> None:
        """Apply failed execution results to run."""
        ...

    async def execute_control_plane_node(
        self,
        node: TaskNode,
        run: ResearchRun,
        session: ResearchSession,
    ) -> Dict[str, Any]:
        """Execute control plane node (context, planning, etc.)."""
        ...


@runtime_checkable
class UtilityProtocol(Protocol):
    """Protocol for utility functions currently in RuntimeService."""

    def lease_expiry(self) -> str:
        """Generate lease expiry timestamp."""
        ...

    def utc_datetime(self, value: str) -> Any:  # datetime
        """Parse UTC datetime string."""
        ...

    def tool_risk_level(self, tool_name: Optional[str]) -> str:
        """Get risk level for tool."""
        ...

    def artifact_ref(self, run: ResearchRun, artifact_type: str) -> Optional[str]:
        """Get artifact reference for run."""
        ...
