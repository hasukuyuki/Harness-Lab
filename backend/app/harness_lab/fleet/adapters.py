"""Adapters exposing RuntimeService capabilities through fleet protocols.

These adapters wrap RuntimeService to implement the protocols defined
in protocols.py, enabling LeaseManager to depend on interfaces rather
than concrete RuntimeService.

Key refactoring: RuntimeConstraintAdapter now uses DispatchConstraintCalculator
instead of RuntimeService private methods, removing fleet's dependency on
runtime internals.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .constraints import DispatchConstraintCalculator
from .protocols import (
    DispatchConstraintProtocol,
    DispatchContextProtocol,
    RunCoordinationProtocol,
    TaskExecutionProtocol,
    UtilityProtocol,
)
from ..types import (
    DispatchConstraint,
    DispatchEnvelope,
    ResearchRun,
    ResearchSession,
    TaskNode,
    WorkerSnapshot,
)

if TYPE_CHECKING:
    from ..runtime.service import RuntimeService


class RuntimeCoordinationAdapter(RunCoordinationProtocol):
    """Adapter for run coordination operations."""

    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime

    def advance_after_lease_transition(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
    ) -> ResearchRun:
        """Delegate to RunCoordinator."""
        return self.runtime.run_coordinator.after_lease_transition(run, session, node)

    def mark_ready_nodes(self, session: ResearchSession, run_id: str) -> bool:
        """Delegate to RunCoordinator."""
        return self.runtime.run_coordinator.mark_ready_nodes(session, run_id)

    def record_handoffs(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
    ) -> List[Any]:
        """Delegate to RuntimeService._record_handoffs_for_node.
        
        This enables fleet layer to trigger handoff recording without
        depending on runtime internals directly.
        """
        return self.runtime._record_handoffs_for_node(run, session, node)


class RuntimeConstraintAdapter(DispatchConstraintProtocol):
    """Adapter for dispatch constraint operations.
    
    Uses DispatchConstraintCalculator (fleet layer) instead of RuntimeService
    private methods, ensuring fleet layer is self-contained.
    """

    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime
        self._calculator = getattr(
            runtime,
            "dispatch_constraint_calculator",
            DispatchConstraintCalculator(
                tool_gateway=runtime.tool_gateway,
                worker_registry=runtime.worker_registry,
            ),
        )

    def constraint_for_node(
        self,
        node: TaskNode,
        session: ResearchSession,
    ) -> DispatchConstraint:
        """Use DispatchConstraintCalculator (fleet layer)."""
        return self._calculator.constraint_for_node(session, node)

    def worker_matches_node(
        self,
        worker: WorkerSnapshot,
        session: ResearchSession,
        node: TaskNode,
    ) -> bool:
        """Use DispatchConstraintCalculator (fleet layer)."""
        return self._calculator.worker_matches_node(worker, session, node)

    def list_dispatch_blockers(
        self,
        run: ResearchRun,
        session: ResearchSession,
    ) -> List[Dict[str, Any]]:
        """Use DispatchConstraintCalculator (fleet layer)."""
        return self._calculator.dispatch_blockers_for_run(run, session)


class RuntimeDispatchContextAdapter(DispatchContextProtocol):
    """Adapter for building dispatch context.
    
    Uses DispatchConstraintCalculator for constraints, removing dependency
    on RuntimeService private methods.
    """

    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime
        self._calculator = getattr(
            runtime,
            "dispatch_constraint_calculator",
            DispatchConstraintCalculator(
                tool_gateway=runtime.tool_gateway,
                worker_registry=runtime.worker_registry,
            ),
        )

    def build_dispatch(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
        worker: WorkerSnapshot,
        lease_id: str,
        attempt_id: str,
    ) -> DispatchEnvelope:
        """Build dispatch envelope using RuntimeService capabilities."""
        from ..utils import new_id, utc_now
        from ..types import SandboxSpec

        now = utc_now()
        
        # Get approval token
        approval_token = self.get_approval_token(run.run_id)
        
        # Determine sandbox requirements
        requires_sandbox = self.runtime.tool_gateway.requires_sandbox(
            session.intent_declaration.suggested_action
        )
        sandbox_spec = (
            self.runtime.tool_gateway.sandbox_spec_for(
                session.intent_declaration.suggested_action,
                approval_token=approval_token,
            )
            if requires_sandbox
            else None
        )
        
        # Get mission phase
        mission_phase = self.runtime.mission_phase_snapshot(run.run_id).phase
        
        # Get handoff packet ref
        handoff_packet_ref = next(
            (
                packet["id"]
                for packet in reversed(self.runtime.run_handoffs(run.run_id))
                if packet.get("task_node_id") == node.node_id
            ),
            None,
        )
        
        # Get constraints using fleet-layer calculator
        constraints = self._calculator.constraint_for_node(session, node)
        
        # Get final verdict
        final_verdict = self.runtime._stored_final_verdict(run)
        
        return DispatchEnvelope(
            lease_id=lease_id,
            attempt_id=attempt_id,
            run_id=run.run_id,
            task_node_id=node.node_id,
            task_node=node,
            intent=session.intent_declaration,
            mission_id=run.mission_id,
            context_packet_ref=self.runtime._artifact_ref(run, "context_bundle"),
            prompt_frame_ref=self.runtime._artifact_ref(run, "prompt_frame"),
            policy_verdicts=run.execution_trace.policy_verdicts if run.execution_trace else [],
            budget={
                "total_token_estimate": run.prompt_frame.total_token_estimate if run.prompt_frame else 0,
                "truncated_blocks": run.prompt_frame.truncated_blocks if run.prompt_frame else [],
            },
            tool_policy={
                "tool_name": session.intent_declaration.suggested_action.tool_name,
                "risk_level": self.runtime._tool_risk_level(
                    session.intent_declaration.suggested_action.tool_name
                ),
                "approval_required": final_verdict.decision == "approval_required",
            },
            approval_token=approval_token,
            agent_role=node.agent_role,
            handoff_packet_ref=handoff_packet_ref,
            mission_phase=mission_phase,
            sandbox_mode=sandbox_spec.sandbox_mode if sandbox_spec else "host_local",
            sandbox_spec=sandbox_spec,
            network_policy=sandbox_spec.network_policy if sandbox_spec else "none",
            requires_sandbox=requires_sandbox,
            required_labels=constraints.required_labels,
            preferred_labels=constraints.preferred_labels,
            queue_shard=constraints.queue_shard,
            lease_timeout_seconds=self.runtime.lease_timeout_seconds,
            heartbeat_interval_seconds=max(1, self.runtime.lease_timeout_seconds // 3),
            run_status_hint=run.status,
            created_at=now,
        )

    def get_approval_token(self, run_id: str) -> Optional[str]:
        """Get approval token for run."""
        return self.runtime._approval_token_for_run(run_id)


class RuntimeTaskExecutionAdapter(TaskExecutionProtocol):
    """Adapter for task execution operations.
    
    Note: This adapter contains the logic formerly in LeaseManager._apply_execution_success/_failure.
    It directly uses RuntimeService methods, eliminating the circular dependency on LeaseManager.
    """

    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime

    def apply_execution_success(
        self,
        run: ResearchRun,
        session: ResearchSession,
        event_batch: Optional[Any],
    ) -> None:
        """Apply successful execution results to run.
        
        Migrated from LeaseManager._apply_execution_success.
        """
        latest_tool_call = None
        if event_batch and event_batch.tool_calls:
            latest_tool_call = event_batch.tool_calls[-1]
        elif run.execution_trace and run.execution_trace.tool_calls:
            latest_tool_call = run.execution_trace.tool_calls[-1]
        
        if not latest_tool_call:
            return
        
        self.runtime._merge_run_result(
            run,
            {
                "summary": "Execution finished; awaiting review and learning stages.",
                "output": latest_tool_call.output,
                "final_action": session.intent_declaration.suggested_action.model_dump(),
                "final_verdict": run.result.get("final_verdict", {}),
                "context_selection_summary": run.result.get("context_selection_summary", {}),
            },
        )

    def apply_execution_failure(
        self,
        run: ResearchRun,
        session: ResearchSession,
        error: str,
        event_batch: Optional[Any],
    ) -> None:
        """Apply failed execution results to run.
        
        Migrated from LeaseManager._apply_execution_failure.
        """
        from ..utils import utc_now
        
        recovery_summary = error
        if event_batch and event_batch.recovery_events:
            recovery_summary = event_batch.recovery_events[-1].summary
        elif run.execution_trace and run.execution_trace.recovery_events:
            recovery_summary = run.execution_trace.recovery_events[-1].summary
        
        if event_batch and not event_batch.recovery_events:
            event_batch.recovery_events.append(
                self.runtime._new_recovery_event("tool_failure", recovery_summary)
            )
        
        # Check if recovery node exists
        if run.execution_trace and self.runtime.orchestrator.has_node_kind(session.task_graph, "recovery"):
            run.execution_trace.status = "recovering"
            run.status = "recovering"
            session.status = "running"
        else:
            if run.execution_trace:
                run.execution_trace.status = "failed"
            run.status = "failed"
            session.status = "failed"
            self.runtime.database.append_event(
                "run.failed",
                {"summary": "Run failed during execution.", "reason": recovery_summary},
                session_id=session.session_id,
                run_id=run.run_id,
            )
        
        self.runtime._merge_run_result(run, {"summary": "Run failed during execution.", "reason": recovery_summary})

    async def execute_control_plane_node(
        self,
        node: TaskNode,
        run: ResearchRun,
        session: ResearchSession,
    ) -> Dict[str, Any]:
        """Execute control plane node (context, planning, etc.)."""
        return await self.runtime._execute_task_node(
            node=node,
            run=run,
            session=session,
            context_summary=run.result.get("context_selection_summary"),
            final_verdict=self.runtime._stored_final_verdict(run),
        )


class RuntimeUtilityAdapter(UtilityProtocol):
    """Adapter for utility functions."""

    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime

    def lease_expiry(self) -> str:
        """Generate lease expiry timestamp."""
        from ..utils import utc_now
        from datetime import timedelta
        return (datetime.now(timezone.utc) + timedelta(seconds=self.runtime.lease_timeout_seconds)).isoformat()

    def utc_datetime(self, value: str) -> datetime:
        """Parse UTC datetime string."""
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def tool_risk_level(self, tool_name: Optional[str]) -> str:
        """Delegate to RuntimeService."""
        return self.runtime._tool_risk_level(tool_name)

    def artifact_ref(self, run: ResearchRun, artifact_type: str) -> Optional[str]:
        """Delegate to RuntimeService private method."""
        return self.runtime._artifact_ref(run, artifact_type)


def create_protocol_adapters(runtime: RuntimeService) -> Dict[str, Any]:
    """Factory function creating all protocol adapters for a RuntimeService.
    
    Usage:
        adapters = create_protocol_adapters(runtime_service)
        lease_manager = LeaseManager(
            database=db,
            coordination=adapters["coordination"],
            constraints=adapters["constraints"],
            context=adapters["context"],
            execution=adapters["execution"],
            utilities=adapters["utilities"],
            ...
        )
    """
    return {
        "coordination": RuntimeCoordinationAdapter(runtime),
        "constraints": RuntimeConstraintAdapter(runtime),
        "context": RuntimeDispatchContextAdapter(runtime),
        "execution": RuntimeTaskExecutionAdapter(runtime),
        "utilities": RuntimeUtilityAdapter(runtime),
    }
