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
            dispatch_blockers=self.runtime._dispatch_blockers_for_run(run, session),
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
            constraints = self.runtime._dispatch_constraint_for_node(session, node)
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


class LeaseManager:
    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime

    def stuck_runs(self) -> List[StuckRunSummary]:
        threshold = max(45, self.runtime.lease_timeout_seconds * 2)
        now = datetime.now(timezone.utc)
        stuck: List[StuckRunSummary] = []
        for run in self.runtime.list_runs(limit=500):
            if run.status not in {"queued", "running"}:
                continue
            try:
                session = self.runtime.get_session(run.session_id)
            except ValueError:
                continue
            snapshot = self.runtime.run_coordinator.coordination_snapshot(run, session)
            ready_count = snapshot.counts_by_status.get("ready", 0)
            leased_count = snapshot.counts_by_status.get("leased", 0) + snapshot.counts_by_status.get("running", 0)
            updated_at = self.runtime._utc_datetime(run.updated_at)
            age_seconds = max(0, int((now - updated_at).total_seconds()))
            if run.active_lease_id or leased_count > 0 or ready_count > 0 or age_seconds < threshold:
                continue
            stuck.append(
                StuckRunSummary(
                    run_id=run.run_id,
                    session_id=run.session_id,
                    status=run.status,
                    mission_status=snapshot.mission_status,
                    reason="no_ready_nodes_and_no_active_lease",
                    age_seconds=age_seconds,
                    updated_at=run.updated_at,
                )
            )
        return stuck

    def run_status_summary(self, run: ResearchRun, session: ResearchSession) -> Dict[str, Any]:
        if run.status in {"completed", "failed", "cancelled"}:
            return {
                "kind": "terminal",
                "reason": run.result.get("summary", f"Run is {run.status}."),
                "status": run.status,
            }
        if run.status == "awaiting_approval":
            approvals = self.runtime.database.list_approvals(run_id=run.run_id)
            pending = next((item for item in approvals if item.status == "pending"), approvals[0] if approvals else None)
            return {
                "kind": "awaiting_approval",
                "reason": pending.summary if pending else "Approval is required.",
                "approval_id": pending.approval_id if pending else None,
            }
        if run.active_lease_id:
            return {
                "kind": "active_lease",
                "reason": "Worker currently owns an active lease.",
                "lease_id": run.active_lease_id,
                "attempt_id": run.current_attempt_id,
            }
        snapshot = self.runtime.run_coordinator.coordination_snapshot(run, session)
        ready_count = snapshot.counts_by_status.get("ready", 0)
        if ready_count > 0:
            blockers = snapshot.dispatch_blockers
            if blockers:
                return {
                    "kind": "dispatch_blocked",
                    "reason": blockers[0]["kind"],
                    "ready_node_ids": snapshot.node_ids_by_status.get("ready", []),
                    "dispatch_blockers": blockers,
                }
            return {
                "kind": "awaiting_dispatch",
                "reason": f"{ready_count} ready node(s) are waiting for a worker poll.",
                "ready_node_ids": snapshot.node_ids_by_status.get("ready", []),
            }
        blocked_ids = snapshot.node_ids_by_status.get("blocked", [])
        if blocked_ids:
            return {
                "kind": "blocked",
                "reason": f"{len(blocked_ids)} task node(s) are blocked.",
                "blocked_node_ids": blocked_ids,
            }
        stuck = next((item for item in self.stuck_runs() if item.run_id == run.run_id), None)
        if stuck:
            return {
                "kind": "stuck_candidate",
                "reason": stuck.reason,
                "age_seconds": stuck.age_seconds,
            }
        return {"kind": "in_progress", "reason": "Run is progressing through the task graph."}

    def rebuild_dispatch_state(self) -> None:
        self.runtime.dispatch_queue.reset()
        for run in self.runtime.list_runs(limit=500):
            if run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                continue
            try:
                session = self.runtime.get_session(run.session_id)
            except ValueError:
                continue
            if session.task_graph:
                for node in session.task_graph.nodes:
                    if node.status == "ready":
                        constraints = self.runtime._dispatch_constraint_for_node(session, node)
                        self.runtime.dispatch_queue.enqueue_ready_task(
                            run.run_id,
                            node.node_id,
                            shard=constraints.queue_shard,
                        )
            for lease in self.runtime.database.list_leases(run_id=run.run_id):
                if lease.status in {"leased", "running"}:
                    self.runtime.dispatch_queue.track_lease_expiry(
                        lease.lease_id,
                        self.runtime._utc_datetime(lease.expires_at).timestamp(),
                    )

    def execution_plane_status(self) -> Dict[str, Any]:
        postgres_ready = True
        redis_ready = True
        try:
            self.runtime.database.ping()
        except Exception:  # noqa: BLE001
            postgres_ready = False
        try:
            self.runtime.dispatch_queue.ping()
        except Exception:  # noqa: BLE001
            redis_ready = False
        all_leases = self.runtime.database.list_leases()
        active_leases = [lease for lease in all_leases if lease.status in {"leased", "running"}]
        stale_leases = [
            lease
            for lease in active_leases
            if self.runtime._utc_datetime(lease.expires_at) < datetime.now(timezone.utc)
        ]
        workers = self.runtime.worker_service.list_workers()
        worker_count_by_state = dict(sorted(Counter(worker.state for worker in workers).items()))
        workers_by_role = dict(sorted(Counter(worker.role_profile or "general" for worker in workers).items()))
        draining_workers = [worker.worker_id for worker in workers if worker.drain_state == "draining"]
        leases_by_status = dict(sorted(Counter(lease.status for lease in all_leases).items()))
        missions_running = len(self.runtime.database.list_missions(status="running"))
        stuck_runs = self.stuck_runs()
        offline_workers = [worker.worker_id for worker in workers if worker.state == "offline"]
        unhealthy_workers = [worker.worker_id for worker in workers if worker.state == "unhealthy"]
        active_workers = [worker.worker_id for worker in workers if worker.state in {"leased", "executing"}]
        sandbox = self.runtime.sandbox_status()
        sandbox_failures = len([event for event in self.runtime.list_events(limit=1_000) if event.event_type == "sandbox.failed"])
        return {
            "storage_backend": self.runtime.database.backend_name,
            "postgres_ready": postgres_ready,
            "redis_ready": redis_ready,
            "ready_queue_depth": self.runtime.dispatch_queue.ready_queue_depth() if redis_ready else 0,
            "queue_depth_by_shard": self.runtime.dispatch_queue.queue_depth_by_shard() if redis_ready else {},
            "active_leases": len(active_leases),
            "stale_leases": len(stale_leases),
            "reclaimed_leases": self.runtime.reclaimed_lease_count,
            "lease_reclaim_rate": round(self.runtime.reclaimed_lease_count / max(1, len(all_leases)), 3),
            "late_callback_count": self.runtime.late_callback_count,
            "worker_count_by_state": worker_count_by_state,
            "workers_by_role": workers_by_role,
            "draining_workers": draining_workers,
            "missions_running": missions_running,
            "leases_by_status": leases_by_status,
            "last_sweep_at": self.runtime.last_lease_sweep_at,
            "offline_workers": offline_workers,
            "unhealthy_workers": unhealthy_workers,
            "active_workers": active_workers,
            "stuck_runs": [item.model_dump() for item in stuck_runs],
            "sandbox_backend": sandbox.sandbox_backend if sandbox else None,
            "docker_ready": sandbox.docker_ready if sandbox else False,
            "sandbox_image_ready": sandbox.sandbox_image_ready if sandbox else False,
            "sandbox_active_runs": sandbox.sandbox_active_runs if sandbox else 0,
            "sandbox_failures": sandbox_failures,
            "sandbox_fallback_mode": sandbox.fallback_mode if sandbox else True,
            "sandbox_last_probe_error": sandbox.last_probe_error if sandbox else "Sandbox manager is not configured.",
        }

    def fleet_status(self) -> FleetStatusReport:
        execution = self.execution_plane_status()
        return FleetStatusReport(
            worker_count=sum(execution["worker_count_by_state"].values()),
            active_workers=execution["active_workers"],
            draining_workers=execution["draining_workers"],
            offline_workers=execution["offline_workers"],
            unhealthy_workers=execution["unhealthy_workers"],
            workers_by_role=execution["workers_by_role"],
            queue_depth_by_shard=execution["queue_depth_by_shard"],
            lease_reclaim_rate=execution["lease_reclaim_rate"],
            stuck_run_count=len(execution["stuck_runs"]),
            late_callback_count=execution["late_callback_count"],
        )

    def queue_status(self) -> List[QueueShardStatus]:
        return [QueueShardStatus(**payload) for payload in self.runtime.dispatch_queue.inspect_queues()]

    def list_leases(
        self,
        run_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[WorkerLease]:
        self.reclaim_stale_leases()
        return self.runtime.database.list_leases(run_id=run_id, worker_id=worker_id, status=status)

    def worker_health_summary(self, worker_id: str) -> WorkerHealthSummary:
        worker = self.runtime.worker_service.get_worker(worker_id)
        recent_leases = self.runtime.database.list_leases(worker_id=worker_id)[-5:]
        matching_events = []
        for event in reversed(self.runtime.database.list_events(limit=500)):
            payload = event.payload or {}
            if payload.get("worker_id") == worker_id:
                matching_events.append(event)
            elif payload.get("lease_id") and any(lease.lease_id == payload.get("lease_id") for lease in recent_leases):
                matching_events.append(event)
            if len(matching_events) >= 10:
                break
        error_events = [
            {"event_type": event.event_type, "created_at": event.created_at, "payload": event.payload}
            for event in matching_events
            if "error" in event.event_type or "failed" in event.event_type or "expired" in event.event_type
        ][:5]
        return WorkerHealthSummary(
            worker_id=worker.worker_id,
            derived_state=worker.state,
            active_lease_count=len([lease for lease in recent_leases if lease.status in {"leased", "running"}]),
            recent_lease_ids=[lease.lease_id for lease in recent_leases],
            recent_error_events=error_events,
            last_event_types=[event.event_type for event in matching_events[:5]],
            last_heartbeat_at=worker.heartbeat_at,
            current_run_id=worker.current_run_id,
            current_task_node_id=worker.current_task_node_id,
        )

    def poll_worker(self, worker_id: str, request: Optional[WorkerPollRequest] = None) -> WorkerPollResponse:
        request = request or WorkerPollRequest()
        self.reclaim_stale_leases()
        worker = self.runtime.worker_service.get_worker(worker_id)
        if worker.drain_state == "draining":
            return WorkerPollResponse(dispatches=[])
        dispatches: List[DispatchEnvelope] = []
        max_checks = max(self.runtime.dispatch_queue.ready_queue_depth(), max(1, request.max_tasks) * 4)
        checked = 0
        while len(dispatches) < max(1, request.max_tasks) and checked < max_checks:
            candidate = self.next_dispatch_for_worker(worker)
            checked += 1
            if candidate is None:
                break
            dispatches.append(candidate)
            worker = self.runtime.worker_service.get_worker(worker_id)
        return WorkerPollResponse(dispatches=dispatches)

    def heartbeat_lease(self, lease_id: str, request: WorkerHeartbeatRequest) -> WorkerLease:
        lease = self.runtime.database.get_lease(lease_id)
        if lease.status not in {"leased", "running"}:
            return lease
        lease.status = "running"
        lease.heartbeat_at = utc_now()
        lease.expires_at = self.runtime._lease_expiry()
        lease.updated_at = lease.heartbeat_at
        self.runtime.database.upsert_lease(lease)
        self.runtime.dispatch_queue.track_lease_expiry(lease.lease_id, self.runtime._utc_datetime(lease.expires_at).timestamp())
        self.runtime.database.append_event(
            "lease.heartbeat",
            {
                "worker_id": lease.worker_id,
                "lease_id": lease.lease_id,
                "attempt_id": lease.attempt_id,
                "task_node_id": lease.task_node_id,
                "status": lease.status,
            },
            session_id=self.runtime.get_run(lease.run_id).session_id,
            run_id=lease.run_id,
        )
        worker = self.runtime.worker_service.heartbeat(
            lease.worker_id,
            WorkerHeartbeatRequest(
                state=request.state or "executing",
                lease_count=request.lease_count or 1,
                current_run_id=lease.run_id,
                current_task_node_id=lease.task_node_id,
                current_lease_id=request.current_lease_id or lease.lease_id,
                last_error=request.last_error,
            ),
        )
        worker.current_lease_id = lease.lease_id
        worker.current_run_id = lease.run_id
        worker.current_task_node_id = lease.task_node_id
        self.runtime.worker_service._persist_worker(worker)
        return lease

    def submit_worker_events(self, lease_id: str, batch: WorkerEventBatch) -> WorkerLease:
        lease = self.runtime.database.get_lease(lease_id)
        run = self.runtime.get_run(lease.run_id)
        session = self.runtime.get_session(run.session_id)
        self.runtime._apply_worker_batch(run, batch, session=session)
        self.runtime._persist_run(run)
        self.runtime._persist_session(session)
        return self.runtime.database.get_lease(lease_id)

    def _apply_execution_success(self, run: ResearchRun, session: ResearchSession, batch: Optional[WorkerEventBatch]) -> None:
        latest_tool_call = None
        if batch and batch.tool_calls:
            latest_tool_call = batch.tool_calls[-1]
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

    def _apply_execution_failure(
        self,
        run: ResearchRun,
        session: ResearchSession,
        request_error: str,
        batch: Optional[WorkerEventBatch],
    ) -> None:
        recovery_summary = request_error
        if batch and batch.recovery_events:
            recovery_summary = batch.recovery_events[-1].summary
        elif run.execution_trace and run.execution_trace.recovery_events:
            recovery_summary = run.execution_trace.recovery_events[-1].summary
        if batch and not batch.recovery_events:
            batch.recovery_events.append(
                self.runtime._new_recovery_event("tool_failure", recovery_summary)
            )
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

    async def _apply_control_plane_node_semantics(
        self,
        node: TaskNode,
        run: ResearchRun,
        session: ResearchSession,
    ) -> Dict[str, Any]:
        return await self.runtime._execute_task_node(
            node=node,
            run=run,
            session=session,
            context_summary=run.result.get("context_selection_summary"),
            final_verdict=self.runtime._stored_final_verdict(run),
        )

    async def complete_lease(self, lease_id: str, request: LeaseCompletionRequest) -> ResearchRun:
        lease = self.runtime.database.get_lease(lease_id)
        if lease.status not in {"leased", "running"}:
            run = self.runtime.get_run(lease.run_id)
            session = self.runtime.get_session(run.session_id)
            self.runtime.database.append_event(
                "lease.complete_ignored",
                {
                    "worker_id": lease.worker_id,
                    "lease_id": lease_id,
                    "attempt_id": lease.attempt_id,
                    "task_node_id": lease.task_node_id,
                    "status": lease.status,
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self.runtime.late_callback_count += 1
            return self.runtime.get_run(lease.run_id)
        run = self.runtime.get_run(lease.run_id)
        session = self.runtime.get_session(run.session_id)
        if request.worker_event_batch:
            self.runtime._apply_worker_batch(run, request.worker_event_batch, session=session)
        node = self.runtime._task_node_by_id(session.task_graph, lease.task_node_id)
        attempt_summary = request.summary or node.label
        if node.kind == "execution":
            self._apply_execution_success(run, session, request.worker_event_batch)
            self.runtime.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {"completed_at": utc_now(), "summary": request.summary or node.label, "worker_lease_id": lease.lease_id},
            )
            task_event_type = "task.completed"
            task_event_payload = {
                "node_id": node.node_id,
                "label": node.label,
                "agent_role": node.agent_role,
                "attempt_id": lease.attempt_id,
                "lease_id": lease.lease_id,
            }
            self.runtime._record_handoffs_for_node(run, session, node)
        else:
            outcome = await self._apply_control_plane_node_semantics(node, run, session)
            attempt_summary = request.summary or outcome.get("reason") or outcome.get("status") or node.label
            task_event_type = None
            task_event_payload = None
        attempt = self.runtime.database.get_attempt(lease.attempt_id)
        now = utc_now()
        attempt.status = "completed"
        attempt.summary = attempt_summary
        attempt.finished_at = now
        attempt.updated_at = now
        self.runtime.database.upsert_attempt(attempt)
        lease.status = "completed"
        lease.heartbeat_at = now
        lease.updated_at = now
        self.runtime.database.upsert_lease(lease)
        self.runtime.dispatch_queue.clear_lease(lease.lease_id)
        context = LeaseTransitionContext(
            lease=lease,
            attempt=attempt,
            run=run,
            session=session,
            task_node=node,
            timestamp=now,
        )
        self.runtime.database.append_event(
            "lease.completed",
            {
                "worker_id": context.lease.worker_id,
                "lease_id": context.lease.lease_id,
                "attempt_id": context.attempt.attempt_id,
                "task_node_id": context.task_node.node_id,
                "summary": context.attempt.summary,
            },
            session_id=session.session_id,
            run_id=run.run_id,
        )
        if task_event_type and task_event_payload:
            self.runtime.database.append_event(
                task_event_type,
                task_event_payload,
                session_id=session.session_id,
                run_id=run.run_id,
            )
        self.runtime._release_worker_assignment(lease.worker_id)
        return await self.runtime.run_coordinator.after_lease_transition(run, session, node)

    async def fail_lease(self, lease_id: str, request: LeaseFailureRequest) -> ResearchRun:
        lease = self.runtime.database.get_lease(lease_id)
        if lease.status not in {"leased", "running"}:
            run = self.runtime.get_run(lease.run_id)
            session = self.runtime.get_session(run.session_id)
            self.runtime.database.append_event(
                "lease.fail_ignored",
                {
                    "worker_id": lease.worker_id,
                    "lease_id": lease_id,
                    "attempt_id": lease.attempt_id,
                    "task_node_id": lease.task_node_id,
                    "status": lease.status,
                    "error": request.error,
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self.runtime.late_callback_count += 1
            return self.runtime.get_run(lease.run_id)
        run = self.runtime.get_run(lease.run_id)
        session = self.runtime.get_session(run.session_id)
        if request.worker_event_batch:
            self.runtime._apply_worker_batch(run, request.worker_event_batch, session=session)
        node = self.runtime._task_node_by_id(session.task_graph, lease.task_node_id)
        if node.kind == "execution":
            self._apply_execution_failure(run, session, request.error, request.worker_event_batch)
        self.runtime.orchestrator.mark_node_status(
            session.task_graph,
            node.node_id,
            "failed",
            {"completed_at": utc_now(), "reason": request.error, "worker_lease_id": lease.lease_id},
        )
        attempt = self.runtime.database.get_attempt(lease.attempt_id)
        now = utc_now()
        attempt.status = "failed"
        attempt.error = request.error
        attempt.finished_at = now
        attempt.updated_at = now
        self.runtime.database.upsert_attempt(attempt)
        lease.status = "failed"
        lease.heartbeat_at = now
        lease.updated_at = now
        self.runtime.database.upsert_lease(lease)
        self.runtime.dispatch_queue.clear_lease(lease.lease_id)
        context = LeaseTransitionContext(
            lease=lease,
            attempt=attempt,
            run=run,
            session=session,
            task_node=node,
            timestamp=now,
        )
        self.runtime.database.append_event(
            "lease.failed",
            {
                "worker_id": context.lease.worker_id,
                "lease_id": context.lease.lease_id,
                "attempt_id": context.attempt.attempt_id,
                "task_node_id": context.task_node.node_id,
                "error": request.error,
            },
            session_id=session.session_id,
            run_id=run.run_id,
        )
        self.runtime.database.append_event(
            "task.failed",
            {
                "node_id": node.node_id,
                "label": node.label,
                "reason": request.error,
                "attempt_id": attempt.attempt_id,
                "lease_id": lease.lease_id,
            },
            session_id=session.session_id,
            run_id=run.run_id,
        )
        self.runtime._release_worker_assignment(lease.worker_id)
        return await self.runtime.run_coordinator.after_lease_transition(run, session, node)

    async def release_lease(self, lease_id: str, request: LeaseReleaseRequest) -> ResearchRun:
        lease = self.runtime.database.get_lease(lease_id)
        run = self.runtime.get_run(lease.run_id)
        session = self.runtime.get_session(run.session_id)
        if lease.status in {"completed", "failed", "released", "expired"}:
            self.runtime.database.append_event(
                "lease.release_ignored",
                {
                    "worker_id": lease.worker_id,
                    "lease_id": lease_id,
                    "attempt_id": lease.attempt_id,
                    "task_node_id": lease.task_node_id,
                    "status": lease.status,
                    "reason": request.reason,
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self.runtime.late_callback_count += 1
            return run
        node = self.runtime._task_node_by_id(session.task_graph, lease.task_node_id)
        if node.status in {"leased", "running"}:
            node.status = "ready"
            node.metadata["release_reason"] = request.reason or "lease_released"
            constraints = self.runtime._dispatch_constraint_for_node(session, node)
            self.runtime.dispatch_queue.enqueue_ready_task(run.run_id, node.node_id, shard=constraints.queue_shard)
        attempt = self.runtime.database.get_attempt(lease.attempt_id)
        now = utc_now()
        attempt.status = "released"
        attempt.error = request.reason
        attempt.finished_at = now
        attempt.updated_at = now
        self.runtime.database.upsert_attempt(attempt)
        lease.status = "released"
        lease.heartbeat_at = now
        lease.updated_at = now
        self.runtime.database.upsert_lease(lease)
        self.runtime.dispatch_queue.clear_lease(lease.lease_id)
        self.runtime.database.append_event(
            "lease.released",
            {
                "worker_id": lease.worker_id,
                "lease_id": lease.lease_id,
                "attempt_id": attempt.attempt_id,
                "task_node_id": lease.task_node_id,
                "reason": request.reason,
            },
            session_id=session.session_id,
            run_id=run.run_id,
        )
        self.runtime._release_worker_assignment(lease.worker_id)
        return await self.runtime.run_coordinator.after_lease_transition(run, session, node)

    def reclaim_stale_leases(self) -> LeaseSweepReport:
        now = datetime.now(timezone.utc)
        expired_lease_ids = set(self.runtime.dispatch_queue.pop_expired_leases(now.timestamp()))
        active_leases = self.runtime.database.list_leases()
        scanned = len(active_leases)
        for lease in active_leases:
            if lease.status in {"leased", "running"} and self.runtime._utc_datetime(lease.expires_at) < now:
                expired_lease_ids.add(lease.lease_id)

        reclaimed: List[WorkerLease] = []
        for lease_id in expired_lease_ids:
            try:
                lease = self.runtime.database.get_lease(lease_id)
            except ValueError:
                continue
            if lease.status not in {"leased", "running"}:
                continue
            current_time = utc_now()
            lease.status = "expired"
            lease.heartbeat_at = current_time
            lease.updated_at = current_time
            self.runtime.database.upsert_lease(lease)
            self.runtime.dispatch_queue.clear_lease(lease.lease_id)
            attempt = self.runtime.database.get_attempt(lease.attempt_id)
            attempt.status = "expired"
            attempt.error = "lease expired before completion"
            attempt.finished_at = current_time
            attempt.updated_at = current_time
            self.runtime.database.upsert_attempt(attempt)
            try:
                run = self.runtime.get_run(lease.run_id)
                session = self.runtime.get_session(run.session_id)
                node = self.runtime._task_node_by_id(session.task_graph, lease.task_node_id)
                if node.status in {"leased", "running"}:
                    node.status = "ready"
                    node.metadata["reclaimed_from_lease_id"] = lease.lease_id
                    node.metadata["retry_index"] = attempt.retry_index + 1
                    constraints = self.runtime._dispatch_constraint_for_node(session, node)
                    self.runtime.dispatch_queue.enqueue_ready_task(
                        run.run_id,
                        node.node_id,
                        shard=constraints.queue_shard,
                    )
                if run.status not in {"completed", "failed", "awaiting_approval", "cancelled"}:
                    run.status = "queued"
                    run.active_lease_id = None
                    run.current_attempt_id = None
                    run.updated_at = current_time
                    if run.execution_trace:
                        run.execution_trace.status = "queued"
                        run.execution_trace.updated_at = current_time
                    session.status = "running"
                    session.updated_at = current_time
                    self.runtime._persist_run(run)
                    self.runtime._persist_session(session)
                mission = self.runtime.database.get_mission_by_run(run.run_id)
                if mission and mission.status not in {"completed", "failed"}:
                    mission.status = "running"
                    mission.updated_at = current_time
                    self.runtime.database.upsert_mission(mission)
                self.runtime.database.append_event(
                    "lease.expired",
                    {
                        "worker_id": lease.worker_id,
                        "lease_id": lease.lease_id,
                        "attempt_id": attempt.attempt_id,
                        "task_node_id": lease.task_node_id,
                    },
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
            except ValueError:
                pass
            self.runtime._release_worker_assignment(lease.worker_id, error="lease expired")
            reclaimed.append(lease)
        self.runtime.reclaimed_lease_count += len(reclaimed)
        report = LeaseSweepReport(scanned=scanned, reclaimed=len(reclaimed), expired_lease_ids=sorted(expired_lease_ids))
        self.runtime.last_lease_sweep_at = utc_now()
        self.runtime.last_lease_sweep_report = report
        return report

    def next_dispatch_for_worker(self, worker) -> Optional[DispatchEnvelope]:
        inspected = 0
        max_checks = max(1, self.runtime.dispatch_queue.ready_queue_depth())
        while inspected < max_checks:
            queue_snapshot = self.runtime.dispatch_queue.inspect_queues(limit=1)
            eligible_shards = [
                shard["shard"]
                for shard in queue_snapshot
                if self._worker_can_poll_shard(worker, str(shard["shard"]))
            ]
            candidate = self.runtime.dispatch_queue.pop_ready_task(shards=eligible_shards or None)
            if candidate is None:
                return None
            run_id, task_node_id, shard = candidate
            inspected += 1
            try:
                run = self.runtime.get_run(run_id)
                session = self.runtime.get_session(run.session_id)
                node = self.runtime._task_node_by_id(session.task_graph, task_node_id)
            except ValueError:
                continue
            if run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                continue
            changed = self.runtime.run_coordinator.mark_ready_nodes(session, run.run_id)
            if changed:
                self.runtime._persist_session(session)
            if node.status != "ready":
                continue
            if not self.runtime._worker_matches_node(worker, session, node):
                constraints = self.runtime._dispatch_constraint_for_node(session, node)
                self.runtime.dispatch_queue.requeue_ready_task(run.run_id, node.node_id, shard=constraints.queue_shard or shard)
                continue
            try:
                return self.create_dispatch(run, session, node, worker.worker_id)
            except ValueError:
                continue
        return None

    def _worker_can_poll_shard(self, worker, shard: str) -> bool:
        if worker.drain_state == "draining":
            return False
        parts = shard.split("/")
        role = parts[0] if parts else None
        labels = parts[2:] if len(parts) > 2 else []
        if worker.role_profile and role and worker.role_profile != role:
            return False
        worker_labels = set(worker.labels or [])
        if labels and "unlabeled" not in labels and not all(label in worker_labels for label in labels):
            return False
        return True

    def create_dispatch(self, run: ResearchRun, session: ResearchSession, node: TaskNode, worker_id: str) -> DispatchEnvelope:
        now = utc_now()
        with self.runtime.database.transaction() as conn:
            locked_run_row = self.runtime.database.fetchone(
                "SELECT payload_json FROM runs WHERE run_id = ? FOR UPDATE",
                (run.run_id,),
                conn=conn,
            )
            locked_session_row = self.runtime.database.fetchone(
                "SELECT payload_json FROM sessions WHERE session_id = ? FOR UPDATE",
                (run.session_id,),
                conn=conn,
            )
            if not locked_run_row or not locked_session_row:
                raise ValueError("Run or session not found during lease claim")
            run = ResearchRun(**json.loads(locked_run_row["payload_json"]))
            session = ResearchSession(**json.loads(locked_session_row["payload_json"]))
            node = self.runtime._task_node_by_id(session.task_graph, node.node_id)
            if node.status != "ready":
                raise ValueError(f"Task node is no longer ready: {node.node_id}")
            retry_index = len(
                [item for item in self.runtime.database.list_attempts(run_id=run.run_id, conn=conn) if item.task_node_id == node.node_id]
            )
            attempt = TaskAttempt(
                attempt_id=new_id("attempt"),
                run_id=run.run_id,
                task_node_id=node.node_id,
                worker_id=worker_id,
                lease_id=None,
                status="leased",
                retry_index=retry_index,
                summary=None,
                error=None,
                started_at=None,
                finished_at=None,
                created_at=now,
                updated_at=now,
            )
            lease = WorkerLease(
                lease_id=new_id("lease"),
                worker_id=worker_id,
                run_id=run.run_id,
                task_node_id=node.node_id,
                attempt_id=attempt.attempt_id,
                status="leased",
                approval_token=self.runtime._approval_token_for_run(run.run_id),
                expires_at=self.runtime._lease_expiry(),
                heartbeat_at=now,
                created_at=now,
                updated_at=now,
            )
            attempt.lease_id = lease.lease_id
            self.runtime.database.upsert_attempt(attempt, conn=conn)
            self.runtime.database.upsert_lease(lease, conn=conn)
            self.runtime.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "leased",
                {"worker_id": worker_id, "lease_id": lease.lease_id, "attempt_id": attempt.attempt_id, "leased_at": now},
            )
            run.assigned_worker_id = worker_id
            run.current_attempt_id = attempt.attempt_id
            run.active_lease_id = lease.lease_id
            run.status = "queued"
            run.updated_at = now
            self.runtime._persist_session(session, conn=conn)
            self.runtime._persist_run(run, conn=conn)
            mission = self.runtime.database.get_mission_by_run(run.run_id, conn=conn)
            if mission:
                mission.status = "running"
                mission.updated_at = now
                self.runtime.database.upsert_mission(mission, conn=conn)
            worker = self.runtime.worker_service.get_worker(worker_id)
            worker.state = "leased"
            worker.current_run_id = run.run_id
            worker.current_task_node_id = node.node_id
            worker.current_lease_id = lease.lease_id
            worker.lease_count += 1
            worker.heartbeat_at = now
            worker.updated_at = now
            self.runtime.worker_service._persist_worker(worker, conn=conn)
            self.runtime.database.append_event(
                "lease.created",
                {
                    "worker_id": worker_id,
                    "lease_id": lease.lease_id,
                    "task_node_id": node.node_id,
                    "attempt_id": attempt.attempt_id,
                },
                session_id=session.session_id,
                run_id=run.run_id,
                conn=conn,
            )
        self.runtime.dispatch_queue.track_lease_expiry(lease.lease_id, self.runtime._utc_datetime(lease.expires_at).timestamp())
        approval_token = lease.approval_token
        requires_sandbox = self.runtime.tool_gateway.requires_sandbox(session.intent_declaration.suggested_action)
        constraints = self.runtime._dispatch_constraint_for_node(session, node)
        mission_phase = self.runtime.mission_phase_snapshot(run.run_id).phase
        handoff_packet_ref = next(
            (
                packet["id"]
                for packet in reversed(self.runtime.run_handoffs(run.run_id))
                if packet.get("task_node_id") == node.node_id
            ),
            None,
        )
        sandbox_spec = (
            self.runtime.tool_gateway.sandbox_spec_for(
                session.intent_declaration.suggested_action,
                approval_token=approval_token,
            )
            if requires_sandbox
            else None
        )
        return DispatchEnvelope(
            lease_id=lease.lease_id,
            attempt_id=attempt.attempt_id,
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
                "risk_level": self.runtime._tool_risk_level(session.intent_declaration.suggested_action.tool_name),
                "approval_required": self.runtime._stored_final_verdict(run).decision == "approval_required",
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


class LocalWorkerAdapter:
    def __init__(self, runtime: RuntimeService) -> None:
        self.runtime = runtime

    async def drain_run(self, run_id: str) -> ResearchRun:
        while True:
            run = self.runtime.get_run(run_id)
            if run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                return run
            workers = [item for item in self.runtime.worker_service.list_workers() if item.state in {"idle", "registering"}]
            if not workers:
                workers = [self.runtime.worker_service.ensure_default_worker()]
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
