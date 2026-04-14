from __future__ import annotations
import json
from collections import Counter
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..types import (
    DispatchEnvelope,
    FleetStatusReport,
    LeaseCompletionRequest,
    LeaseFailureRequest,
    LeaseReleaseRequest,
    LeaseSweepReport,
    LeaseTransitionContext,
    QueueShardStatus,
    ResearchRun,
    ResearchSession,
    RunCoordinationSnapshot,
    StuckRunSummary,
    TaskAttempt,
    TaskNode,
    WorkerEventBatch,
    WorkerHealthSummary,
    WorkerHeartbeatRequest,
    WorkerLease,
    WorkerPollRequest,
    WorkerPollResponse,
)
from ..utils import new_id, utc_now

if TYPE_CHECKING:
    from .service import RuntimeService


class RunCoordinator:
    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime

    def coordination_snapshot(self, run: ResearchRun, session: ResearchSession) -> RunCoordinationSnapshot:
        task_graph = self.runtime._task_graph(session)
        counts = Counter(node.status for node in task_graph.nodes)
        node_ids_by_status: Dict[str, List[str]] = {}
        for node in task_graph.nodes:
            node_ids_by_status.setdefault(node.status, []).append(node.node_id)
        mission = self.runtime.database.get_mission_by_run(run.run_id)
        return RunCoordinationSnapshot(
            run_id=run.run_id,
            mission_status=mission.status if mission else None,
            counts_by_status=dict(sorted(counts.items())),
            node_ids_by_status=node_ids_by_status,
            dispatch_blockers=self.runtime.dispatch_constraint_calculator.dispatch_blockers_for_run(run, session),
            active_lease_id=run.active_lease_id,
            current_attempt_id=run.current_attempt_id,
            updated_at=utc_now(),
        )

    def timeline_summary(self, run_id: str) -> Dict[str, Any]:
        attempts = self.runtime.database.list_attempts(run_id=run_id)
        leases = {lease.lease_id: lease for lease in self.runtime.database.list_leases(run_id=run_id)}
        attempt_counts = Counter(attempt.status for attempt in attempts)
        lease_counts = Counter(lease.status for lease in leases.values())
        entries = []
        for attempt in attempts:
            lease = leases.get(attempt.lease_id or "")
            entries.append(
                {
                    "attempt_id": attempt.attempt_id,
                    "task_node_id": attempt.task_node_id,
                    "worker_id": attempt.worker_id,
                    "lease_id": attempt.lease_id,
                    "attempt_status": attempt.status,
                    "lease_status": lease.status if lease else None,
                    "retry_index": attempt.retry_index,
                    "started_at": attempt.started_at or (lease.created_at if lease else attempt.created_at),
                    "finished_at": attempt.finished_at,
                    "last_heartbeat_at": lease.heartbeat_at if lease else None,
                    "summary": attempt.summary,
                    "error": attempt.error,
                }
            )
        return {
            "entries": entries,
            "attempts_by_status": dict(sorted(attempt_counts.items())),
            "leases_by_status": dict(sorted(lease_counts.items())),
            "latest_transition_at": entries[-1]["finished_at"] if entries else None,
        }

    async def after_lease_transition(self, run: ResearchRun, session: ResearchSession, node: TaskNode) -> ResearchRun:
        run.active_lease_id = None
        run.current_attempt_id = None
        self.mark_ready_nodes(session, run.run_id)
        task_graph = self.runtime._task_graph(session)
        mission = self.runtime.database.get_mission_by_run(run.run_id)
        if self.runtime.orchestrator.is_terminal(task_graph):
            if self.runtime.orchestrator.has_failed_nodes(task_graph):
                if run.status != "failed":
                    self.mark_run_failed(run, session, "workflow_failed", "One or more task nodes failed.")
                    mission = self.runtime.database.get_mission_by_run(run.run_id)
            elif run.status not in {"completed", "awaiting_approval"}:
                run.status = "completed"
                run.execution_trace.status = "completed"
                self.runtime._merge_run_result(
                    run,
                    {
                    "summary": "Harness Lab run completed with a replayable trace.",
                    "output": run.result.get("output", {}),
                    "final_action": session.intent_declaration.suggested_action.model_dump(),
                    "completed_nodes": [item.node_id for item in task_graph.nodes if item.status == "completed"],
                    "context_selection_summary": run.result.get("context_selection_summary", {}),
                    "final_verdict": run.result.get("final_verdict", {}),
                    },
                )
                session.status = "completed"
                self.runtime.database.append_event(
                    "run.completed",
                    {
                        "summary": run.result["summary"],
                        "tool_name": session.intent_declaration.suggested_action.tool_name,
                        "completed_nodes": len([item for item in task_graph.nodes if item.status == "completed"]),
                    },
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
                now = utc_now()
                run.execution_trace.updated_at = now
                run.updated_at = now
                session.updated_at = now
                self.runtime._persist_run(run)
                self.runtime._persist_session(session)
                self.persist_replay(run)
                if mission:
                    mission.status = "completed"
                    mission.updated_at = now
                    self.runtime.database.upsert_mission(mission)
                return self.runtime.get_run(run.run_id)
        elif run.status not in {"awaiting_approval", "recovering", "failed"}:
            run.status = "queued"
            if run.execution_trace:
                run.execution_trace.status = "queued"
                run.execution_trace.updated_at = utc_now()
            session.status = "running"
            session.updated_at = utc_now()
        run.updated_at = utc_now()
        self.runtime._persist_session(session)
        self.runtime._persist_run(run)
        if mission:
            if run.status == "awaiting_approval":
                mission.status = "awaiting_approval"
            elif run.status in {"failed", "cancelled"}:
                mission.status = "failed"
            else:
                mission.status = "running"
            mission.updated_at = utc_now()
            self.runtime.database.upsert_mission(mission)
        if run.status in {"awaiting_approval", "failed", "completed"}:
            self.persist_replay(run)
        return self.runtime.get_run(run.run_id)

    def mark_ready_nodes(self, session: ResearchSession, run_id: str) -> bool:
        if not session.task_graph:
            return False
        changed = False
        for node in self.runtime.orchestrator.next_wave(session.task_graph):
            if node.status != "planned":
                continue
            node.status = "ready"
            node.metadata["ready_at"] = utc_now()
            changed = True
            constraints = self.runtime.dispatch_constraint_calculator.constraint_for_node(session, node)
            self.runtime.dispatch_queue.enqueue_ready_task(run_id, node.node_id, shard=constraints.queue_shard)
            self.runtime.database.append_event(
                "task.ready",
                {
                    "node_id": node.node_id,
                    "label": node.label,
                    "kind": node.kind,
                    "queue_shard": constraints.queue_shard,
                    "required_labels": constraints.required_labels,
                    "preferred_labels": constraints.preferred_labels,
                },
                session_id=session.session_id,
                run_id=run_id,
            )
        return changed

    def mark_run_failed(self, run: ResearchRun, session: ResearchSession, kind: str, reason: str) -> None:
        self.runtime._mark_run_failed_impl(run, session, kind, reason)

    def persist_replay(self, run: ResearchRun) -> None:
        self.runtime._persist_replay_impl(run)


class LocalWorkerAdapter:
    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime

    async def drain_run(self, run_id: str) -> ResearchRun:
        while True:
            run = self.runtime.get_run(run_id)
            if run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                return run
            workers = [item for item in self.runtime.worker_registry.list_workers() if item.state in {"idle", "registering"}]
            if not workers:
                workers = [self.runtime.worker_registry.ensure_default_worker()]
            dispatches: List[DispatchEnvelope] = []
            for worker in workers:
                response = self.runtime.lease_manager.poll_worker(worker.worker_id, WorkerPollRequest(max_tasks=1))
                dispatches.extend(response.dispatches)
            if not dispatches:
                return self.runtime.get_run(run_id)
            for dispatch in dispatches:
                await self.execute_leased_task(dispatch.lease_id)

    async def execute_leased_task(self, lease_id: str) -> ResearchRun:
        lease = self.runtime.database.get_lease(lease_id)
        if lease.status not in {"leased", "running"}:
            return self.runtime.get_run(lease.run_id)
        run = self.runtime.get_run(lease.run_id)
        session = self.runtime.get_session(run.session_id)
        node = self.runtime._task_node_by_id(session.task_graph, lease.task_node_id)
        attempt = self.runtime.database.get_attempt(lease.attempt_id)
        now = utc_now()
        lease.status = "running"
        lease.heartbeat_at = now
        lease.expires_at = self.runtime._lease_expiry()
        lease.updated_at = now
        self.runtime.database.upsert_lease(lease)
        attempt.status = "running"
        attempt.started_at = attempt.started_at or now
        attempt.updated_at = now
        self.runtime.database.upsert_attempt(attempt)
        run.status = "running"
        run.active_lease_id = lease.lease_id
        run.current_attempt_id = attempt.attempt_id
        if run.execution_trace:
            run.execution_trace.status = "running"
            run.execution_trace.updated_at = now
        self.runtime.orchestrator.mark_node_status(
            session.task_graph,
            node.node_id,
            "running",
            {"worker_id": lease.worker_id, "lease_id": lease.lease_id, "attempt_id": attempt.attempt_id, "started_at": now},
        )
        self.runtime.database.append_event(
            "task.started",
            {
                "node_id": node.node_id,
                "label": node.label,
                "kind": node.kind,
                "role": node.role,
                "agent_role": node.agent_role,
                "worker_id": lease.worker_id,
                "lease_id": lease.lease_id,
                "attempt_id": attempt.attempt_id,
            },
            session_id=session.session_id,
            run_id=run.run_id,
        )
        outcome = await self.runtime._execute_task_node(
            node=node,
            run=run,
            session=session,
            context_summary=run.result.get("context_selection_summary"),
            final_verdict=self.runtime._stored_final_verdict(run),
        )
        finished_at = utc_now()
        if outcome["status"] == "completed":
            attempt.status = "completed"
            attempt.summary = outcome.get("reason") or node.label
            lease.status = "completed"
            self.runtime._record_handoffs_for_node(run, session, node)
            self.runtime.database.append_event(
                "lease.completed",
                {
                    "worker_id": lease.worker_id,
                    "lease_id": lease.lease_id,
                    "attempt_id": attempt.attempt_id,
                    "task_node_id": node.node_id,
                    "summary": attempt.summary,
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
        elif outcome["status"] == "awaiting_approval":
            attempt.status = "blocked"
            attempt.summary = outcome.get("reason")
            lease.status = "released"
            self.runtime.database.append_event(
                "lease.released",
                {
                    "worker_id": lease.worker_id,
                    "lease_id": lease.lease_id,
                    "attempt_id": attempt.attempt_id,
                    "task_node_id": node.node_id,
                    "reason": outcome.get("reason"),
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
        else:
            attempt.status = "failed"
            attempt.error = outcome.get("reason")
            lease.status = "failed"
            self.runtime.database.append_event(
                "lease.failed",
                {
                    "worker_id": lease.worker_id,
                    "lease_id": lease.lease_id,
                    "attempt_id": attempt.attempt_id,
                    "task_node_id": node.node_id,
                    "error": attempt.error,
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
        attempt.finished_at = finished_at
        attempt.updated_at = finished_at
        self.runtime.database.upsert_attempt(attempt)
        lease.heartbeat_at = finished_at
        lease.updated_at = finished_at
        self.runtime.database.upsert_lease(lease)
        if lease.status in {"completed", "failed", "released", "expired"}:
            self.runtime.dispatch_queue.clear_lease(lease.lease_id)
        self.runtime._release_worker_assignment(lease.worker_id, error=outcome.get("release_error"))
        return await self.runtime.run_coordinator.after_lease_transition(run, session, node)
