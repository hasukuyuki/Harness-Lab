"""Lease Manager - decoupled from RuntimeService using protocols.

Depends on protocol interfaces rather than concrete RuntimeService.
Part of the fleet module for unified worker fleet coordination.
"""

from __future__ import annotations

import json
import inspect
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .protocols import (
    DispatchConstraintProtocol,
    DispatchContextProtocol,
    RunCoordinationProtocol,
    TaskExecutionProtocol,
    UtilityProtocol,
)
from ..storage import PlatformStore
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
    StuckRunSummary,
    TaskAttempt,
    TaskNode,
    WorkerEventBatch,
    WorkerHealthSummary,
    WorkerHeartbeatRequest,
    WorkerLease,
    WorkerPollRequest,
    WorkerPollResponse,
    WorkerSnapshot,
)
from ..utils import new_id, utc_now


class LeaseManager:
    """Lease manager depending on protocols, not RuntimeService.
    
    This version can be instantiated independently and migrated to fleet/.
    """

    def __init__(
        self,
        database: PlatformStore,
        coordination: RunCoordinationProtocol,
        constraints: DispatchConstraintProtocol,
        context: DispatchContextProtocol,
        execution: TaskExecutionProtocol,
        utilities: UtilityProtocol,
        worker_registry: Any,  # WorkerRegistry
        dispatch_queue: Any,  # DispatchQueue
        orchestrator: Any,  # OrchestratorService
        lease_timeout_seconds: int = 30,
    ) -> None:
        self.database = database
        self.coordination = coordination
        self.constraints = constraints
        self.context = context
        self.execution = execution
        self.utilities = utilities
        self.worker_registry = worker_registry
        self.dispatch_queue = dispatch_queue
        self.orchestrator = orchestrator
        self.lease_timeout_seconds = lease_timeout_seconds
        self.reclaimed_lease_count = 0
        self.last_lease_sweep_at: Optional[str] = None
        self.last_lease_sweep_report = LeaseSweepReport()
        self.late_callback_count = 0

    def _runtime(self):
        """Access the backing RuntimeService when adapters are in use."""
        for adapter in (self.coordination, self.constraints, self.context, self.execution, self.utilities):
            runtime = getattr(adapter, "runtime", None)
            if runtime is not None:
                return runtime
        return None

    # =========================================================================
    # Lease lifecycle operations
    # =========================================================================

    def poll_worker(self, worker_id: str, request: Optional[WorkerPollRequest] = None) -> WorkerPollResponse:
        """Poll for tasks assigned to worker."""
        from ..types import WorkerPollRequest
        request = request or WorkerPollRequest()
        self.reclaim_stale_leases()
        
        worker = self.worker_registry.get_worker(worker_id)
        if worker.drain_state == "draining":
            return WorkerPollResponse(dispatches=[])
        
        dispatches: List[DispatchEnvelope] = []
        max_checks = max(self.dispatch_queue.ready_queue_depth(), max(1, request.max_tasks) * 4)
        checked = 0
        
        while len(dispatches) < max(1, request.max_tasks) and checked < max_checks:
            candidate = self._next_dispatch_for_worker(worker)
            checked += 1
            if candidate is None:
                break
            dispatches.append(candidate)
            worker = self.worker_registry.get_worker(worker_id)
        
        return WorkerPollResponse(dispatches=dispatches)

    def heartbeat_lease(self, lease_id: str, request: WorkerHeartbeatRequest) -> WorkerLease:
        """Process lease heartbeat."""
        lease = self.database.get_lease(lease_id)
        if lease.status not in {"leased", "running"}:
            return lease
        
        lease.status = "running"
        lease.heartbeat_at = utc_now()
        lease.expires_at = self.utilities.lease_expiry()
        lease.updated_at = lease.heartbeat_at
        self.database.upsert_lease(lease)
        self.dispatch_queue.track_lease_expiry(
            lease.lease_id,
            self.utilities.utc_datetime(lease.expires_at).timestamp()
        )
        
        # Get run for event
        run = self._get_run(lease.run_id)
        
        self.database.append_event(
            "lease.heartbeat",
            {
                "worker_id": lease.worker_id,
                "lease_id": lease.lease_id,
                "attempt_id": lease.attempt_id,
                "task_node_id": lease.task_node_id,
                "status": lease.status,
            },
            session_id=run.session_id if run else None,
            run_id=lease.run_id,
        )
        
        # Update worker via registry
        self.worker_registry.heartbeat(
            lease.worker_id,
            request,
        )
        
        return lease

    async def complete_lease(self, lease_id: str, request: LeaseCompletionRequest) -> ResearchRun:
        """Complete a lease."""
        lease = self.database.get_lease(lease_id)
        if lease.status not in {"leased", "running"}:
            self._record_ignored_completion(lease, "lease.complete_ignored")
            self.late_callback_count += 1
            return self._get_run(lease.run_id)
        
        run = self._get_run(lease.run_id)
        session = self._get_session(run.session_id)
        
        if request.worker_event_batch:
            self._apply_worker_batch(run, request.worker_event_batch, session=session)
        
        node = self._get_node(session.task_graph, lease.task_node_id)
        
        if node.kind == "execution":
            self.execution.apply_execution_success(run, session, request.worker_event_batch)
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {"completed_at": utc_now(), "summary": request.summary or node.label, "worker_lease_id": lease.lease_id},
            )
            # TODO: handoff recording
        else:
            outcome = self.execution.execute_control_plane_node(node, run, session)
            if inspect.isawaitable(outcome):
                outcome = await outcome
            # TODO: handle outcome
        
        # Update attempt
        attempt = self.database.get_attempt(lease.attempt_id)
        now = utc_now()
        attempt.status = "completed"
        attempt.summary = request.summary or node.label
        attempt.finished_at = now
        attempt.updated_at = now
        self.database.upsert_attempt(attempt)
        
        # Update lease
        lease.status = "completed"
        lease.heartbeat_at = now
        lease.updated_at = now
        self.database.upsert_lease(lease)
        self.dispatch_queue.clear_lease(lease.lease_id)
        
        self._append_lease_event("lease.completed", lease, run, session)
        
        return await self.coordination.advance_after_lease_transition(run, session, node)

    async def fail_lease(self, lease_id: str, request: LeaseFailureRequest) -> ResearchRun:
        """Fail a lease."""
        lease = self.database.get_lease(lease_id)
        if lease.status not in {"leased", "running"}:
            self._record_ignored_completion(lease, "lease.fail_ignored", error=request.error)
            self.late_callback_count += 1
            return self._get_run(lease.run_id)
        
        run = self._get_run(lease.run_id)
        session = self._get_session(run.session_id)
        
        if request.worker_event_batch:
            self._apply_worker_batch(run, request.worker_event_batch, session=session)
        
        node = self._get_node(session.task_graph, lease.task_node_id)
        
        if node.kind == "execution":
            self.execution.apply_execution_failure(run, session, request.error, request.worker_event_batch)
        
        self.orchestrator.mark_node_status(
            session.task_graph,
            node.node_id,
            "failed",
            {"completed_at": utc_now(), "reason": request.error, "worker_lease_id": lease.lease_id},
        )
        
        # Update attempt
        attempt = self.database.get_attempt(lease.attempt_id)
        now = utc_now()
        attempt.status = "failed"
        attempt.error = request.error
        attempt.finished_at = now
        attempt.updated_at = now
        self.database.upsert_attempt(attempt)
        
        # Update lease
        lease.status = "failed"
        lease.heartbeat_at = now
        lease.updated_at = now
        self.database.upsert_lease(lease)
        self.dispatch_queue.clear_lease(lease.lease_id)
        
        self._append_lease_event("lease.failed", lease, run, session, error=request.error)
        
        return await self.coordination.advance_after_lease_transition(run, session, node)

    async def release_lease(self, lease_id: str, request: LeaseReleaseRequest) -> ResearchRun:
        """Release a lease."""
        lease = self.database.get_lease(lease_id)
        run = self._get_run(lease.run_id)
        session = self._get_session(run.session_id)
        
        if lease.status in {"completed", "failed", "released", "expired"}:
            self._record_ignored_completion(lease, "lease.release_ignored", reason=request.reason)
            self.late_callback_count += 1
            return run
        
        node = self._get_node(session.task_graph, lease.task_node_id)
        
        if node.status in {"leased", "running"}:
            node.status = "ready"
            node.metadata["release_reason"] = request.reason or "lease_released"
            constraints = self.constraints.constraint_for_node(node, session)
            self.dispatch_queue.enqueue_ready_task(run.run_id, node.node_id, shard=constraints.queue_shard)
        
        # Update attempt
        attempt = self.database.get_attempt(lease.attempt_id)
        now = utc_now()
        attempt.status = "released"
        attempt.error = request.reason
        attempt.finished_at = now
        attempt.updated_at = now
        self.database.upsert_attempt(attempt)
        
        # Update lease
        lease.status = "released"
        lease.heartbeat_at = now
        lease.updated_at = now
        self.database.upsert_lease(lease)
        self.dispatch_queue.clear_lease(lease.lease_id)
        
        self._append_lease_event("lease.released", lease, run, session, reason=request.reason)
        
        return await self.coordination.advance_after_lease_transition(run, session, node)

    # =========================================================================
    # Lease maintenance operations
    # =========================================================================

    def reclaim_stale_leases(self) -> LeaseSweepReport:
        """Reclaim expired leases."""
        now = datetime.now(timezone.utc)
        expired_lease_ids = set(self.dispatch_queue.pop_expired_leases(now.timestamp()))
        active_leases = self.database.list_leases()
        scanned = len(active_leases)
        
        for lease in active_leases:
            if lease.status in {"leased", "running"} and self.utilities.utc_datetime(lease.expires_at) < now:
                expired_lease_ids.add(lease.lease_id)
        
        reclaimed: List[WorkerLease] = []
        for lease_id in expired_lease_ids:
            try:
                lease = self.database.get_lease(lease_id)
            except ValueError:
                continue
            if lease.status not in {"leased", "running"}:
                continue
            
            if self._reclaim_lease(lease):
                reclaimed.append(lease)
        
        self.reclaimed_lease_count += len(reclaimed)
        report = LeaseSweepReport(
            scanned=scanned,
            reclaimed=len(reclaimed),
            expired_lease_ids=sorted(expired_lease_ids)
        )
        self.last_lease_sweep_at = utc_now()
        self.last_lease_sweep_report = report
        return report

    def list_leases(
        self,
        run_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[WorkerLease]:
        """List leases after first reclaiming stale entries."""
        self.reclaim_stale_leases()
        return self.database.list_leases(run_id=run_id, worker_id=worker_id, status=status)

    def rebuild_dispatch_state(self) -> None:
        """Rebuild ready queue and lease expiry index from durable state."""
        runtime = self._runtime()
        self.dispatch_queue.reset()
        runs = runtime.list_runs(limit=500) if runtime is not None else []
        for run in runs:
            if run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                continue
            try:
                session = runtime.get_session(run.session_id)
            except ValueError:
                continue
            if session.task_graph:
                for node in session.task_graph.nodes:
                    if node.status != "ready":
                        continue
                    constraints = self.constraints.constraint_for_node(node, session)
                    self.dispatch_queue.enqueue_ready_task(
                        run.run_id,
                        node.node_id,
                        shard=constraints.queue_shard,
                    )
            for lease in self.database.list_leases(run_id=run.run_id):
                if lease.status in {"leased", "running"}:
                    self.dispatch_queue.track_lease_expiry(
                        lease.lease_id,
                        self.utilities.utc_datetime(lease.expires_at).timestamp(),
                    )

    def execution_plane_status(self) -> Dict[str, Any]:
        """Return execution-plane health and fleet metrics."""
        runtime = self._runtime()
        postgres_ready = True
        redis_ready = True
        try:
            self.database.ping()
        except Exception:  # noqa: BLE001
            postgres_ready = False
        try:
            self.dispatch_queue.ping()
        except Exception:  # noqa: BLE001
            redis_ready = False

        all_leases = self.database.list_leases()
        active_leases = [lease for lease in all_leases if lease.status in {"leased", "running"}]
        stale_leases = [
            lease
            for lease in active_leases
            if self.utilities.utc_datetime(lease.expires_at) < datetime.now(timezone.utc)
        ]
        workers = self.worker_registry.list_workers()
        worker_count_by_state = dict(sorted(Counter(worker.state for worker in workers).items()))
        workers_by_role = dict(sorted(Counter(worker.role_profile or "general" for worker in workers).items()))
        draining_workers = [worker.worker_id for worker in workers if worker.drain_state == "draining"]
        leases_by_status = dict(sorted(Counter(lease.status for lease in all_leases).items()))
        missions_running = len(self.database.list_missions(status="running"))
        stuck_runs = self._stuck_runs()
        offline_workers = [worker.worker_id for worker in workers if worker.state == "offline"]
        unhealthy_workers = [worker.worker_id for worker in workers if worker.state == "unhealthy"]
        active_workers = [worker.worker_id for worker in workers if worker.state in {"leased", "executing"}]
        sandbox = runtime.sandbox_status() if runtime is not None else None
        sandbox_failures = len([event for event in self.database.list_events(limit=1_000) if event.event_type == "sandbox.failed"])
        return {
            "storage_backend": self.database.backend_name,
            "postgres_ready": postgres_ready,
            "redis_ready": redis_ready,
            "ready_queue_depth": self.dispatch_queue.ready_queue_depth() if redis_ready else 0,
            "queue_depth_by_shard": self.dispatch_queue.queue_depth_by_shard() if redis_ready else {},
            "active_leases": len(active_leases),
            "stale_leases": len(stale_leases),
            "reclaimed_leases": self.reclaimed_lease_count,
            "lease_reclaim_rate": round(self.reclaimed_lease_count / max(1, len(all_leases)), 3),
            "late_callback_count": self.late_callback_count,
            "worker_count_by_state": worker_count_by_state,
            "workers_by_role": workers_by_role,
            "draining_workers": draining_workers,
            "missions_running": missions_running,
            "leases_by_status": leases_by_status,
            "last_sweep_at": self.last_lease_sweep_at,
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
        """Return fleet-level summary."""
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
        """Return queue shard inspection status."""
        return [QueueShardStatus(**payload) for payload in self.dispatch_queue.inspect_queues()]

    def worker_health_summary(self, worker_id: str) -> WorkerHealthSummary:
        """Return derived worker health summary."""
        worker = self.worker_registry.get_worker(worker_id)
        recent_leases = self.database.list_leases(worker_id=worker_id)[-5:]
        matching_events = []
        for event in reversed(self.database.list_events(limit=500)):
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

    def run_status_summary(self, run: ResearchRun, session: ResearchSession) -> Dict[str, Any]:
        """Summarize current run status for operator surfaces."""
        runtime = self._runtime()
        if run.status in {"completed", "failed", "cancelled"}:
            return {
                "kind": "terminal",
                "reason": run.result.get("summary", f"Run is {run.status}."),
                "status": run.status,
            }
        if run.status == "awaiting_approval":
            approvals = self.database.list_approvals(run_id=run.run_id)
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
        snapshot = runtime.run_coordinator.coordination_snapshot(run, session) if runtime is not None else None
        ready_count = snapshot.counts_by_status.get("ready", 0) if snapshot else 0
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
        blocked_ids = snapshot.node_ids_by_status.get("blocked", []) if snapshot else []
        if blocked_ids:
            return {
                "kind": "blocked",
                "reason": f"{len(blocked_ids)} task node(s) are blocked.",
                "blocked_node_ids": blocked_ids,
            }
        stuck = next((item for item in self._stuck_runs() if item.run_id == run.run_id), None)
        if stuck:
            return {
                "kind": "stuck_candidate",
                "reason": stuck.reason,
                "age_seconds": stuck.age_seconds,
            }
        return {"kind": "in_progress", "reason": "Run is progressing through the task graph."}

    def submit_worker_events(self, lease_id: str, batch: WorkerEventBatch) -> WorkerLease:
        """Apply a worker event batch without completing the lease."""
        runtime = self._runtime()
        lease = self.database.get_lease(lease_id)
        if runtime is None:
            return lease
        run = runtime.get_run(lease.run_id)
        session = runtime.get_session(run.session_id)
        runtime._apply_worker_batch(run, batch, session=session)
        runtime._persist_run(run)
        runtime._persist_session(session)
        return self.database.get_lease(lease_id)

    def next_dispatch_for_worker(self, worker: WorkerSnapshot) -> Optional[DispatchEnvelope]:
        """Public wrapper for dispatch selection."""
        return self._next_dispatch_for_worker(worker)

    # =========================================================================
    # Dispatch operations
    # =========================================================================

    def _next_dispatch_for_worker(self, worker: WorkerSnapshot) -> Optional[DispatchEnvelope]:
        """Find next dispatch for worker."""
        inspected = 0
        max_checks = max(1, self.dispatch_queue.ready_queue_depth())
        
        while inspected < max_checks:
            queue_snapshot = self.dispatch_queue.inspect_queues(limit=1)
            eligible_shards = [
                shard["shard"]
                for shard in queue_snapshot
                if self._worker_can_poll_shard(worker, str(shard["shard"]))
            ]
            
            candidate = self.dispatch_queue.pop_ready_task(shards=eligible_shards or None)
            if candidate is None:
                return None
            
            run_id, task_node_id, shard = candidate
            inspected += 1
            
            try:
                run = self._get_run(run_id)
                session = self._get_session(run.session_id)
                node = self._get_node(session.task_graph, task_node_id)
            except (ValueError, AttributeError):
                continue
            
            if run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                continue
            
            self.coordination.mark_ready_nodes(session, run.run_id)
            
            if node.status != "ready":
                continue
            
            if not self.constraints.worker_matches_node(worker, session, node):
                constraints = self.constraints.constraint_for_node(node, session)
                self.dispatch_queue.requeue_ready_task(
                    run.run_id, node.node_id, shard=constraints.queue_shard or shard
                )
                continue
            
            try:
                return self._create_dispatch(run, session, node, worker.worker_id)
            except ValueError:
                continue
        
        return None

    def _create_dispatch(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
        worker_id: str,
    ) -> DispatchEnvelope:
        """Create dispatch envelope."""
        now = utc_now()
        
        with self.database.transaction() as conn:
            # Lock run and session
            locked_run = self.database.fetchone(
                "SELECT payload_json FROM runs WHERE run_id = ? FOR UPDATE",
                (run.run_id,), conn=conn
            )
            locked_session = self.database.fetchone(
                "SELECT payload_json FROM sessions WHERE session_id = ? FOR UPDATE",
                (run.session_id,), conn=conn
            )
            
            if not locked_run or not locked_session:
                raise ValueError("Run or session not found during lease claim")
            
            run = ResearchRun(**json.loads(locked_run["payload_json"]))
            session = ResearchSession(**json.loads(locked_session["payload_json"]))
            node = self._get_node(session.task_graph, node.node_id)
            
            if node.status != "ready":
                raise ValueError(f"Task node is no longer ready: {node.node_id}")
            
            # Create attempt
            retry_index = len([
                item for item in self.database.list_attempts(run_id=run.run_id, conn=conn)
                if item.task_node_id == node.node_id
            ])
            
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
            
            # Create lease
            lease = WorkerLease(
                lease_id=new_id("lease"),
                worker_id=worker_id,
                run_id=run.run_id,
                task_node_id=node.node_id,
                attempt_id=attempt.attempt_id,
                status="leased",
                approval_token=self.context.get_approval_token(run.run_id),
                expires_at=self.utilities.lease_expiry(),
                heartbeat_at=now,
                created_at=now,
                updated_at=now,
            )
            
            attempt.lease_id = lease.lease_id
            self.database.upsert_attempt(attempt, conn=conn)
            self.database.upsert_lease(lease, conn=conn)
            
            # Update node status
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "leased",
                {"worker_id": worker_id, "lease_id": lease.lease_id, "attempt_id": attempt.attempt_id, "leased_at": now},
            )
            
            # Update run
            run.assigned_worker_id = worker_id
            run.current_attempt_id = attempt.attempt_id
            run.active_lease_id = lease.lease_id
            run.status = "queued"
            run.updated_at = now
            self._persist_run(run, conn=conn)
            self._persist_session(session, conn=conn)
            
            # Update mission
            mission = self.database.get_mission_by_run(run.run_id, conn=conn)
            if mission:
                mission.status = "running"
                mission.updated_at = now
                self.database.upsert_mission(mission, conn=conn)
            
            # Update worker via registry
            worker = self.worker_registry.get_worker(worker_id)
            worker.state = "leased"
            worker.current_run_id = run.run_id
            worker.current_task_node_id = node.node_id
            worker.current_lease_id = lease.lease_id
            worker.lease_count += 1
            worker.heartbeat_at = now
            worker.updated_at = now
            self.worker_registry._persist(worker, conn=conn)
            
            self.database.append_event(
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
        
        # Track lease expiry
        self.dispatch_queue.track_lease_expiry(
            lease.lease_id,
            self.utilities.utc_datetime(lease.expires_at).timestamp()
        )
        
        # Build and return dispatch envelope
        return self.context.build_dispatch(run, session, node, worker, lease.lease_id, attempt.attempt_id)

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _stuck_runs(self) -> List[StuckRunSummary]:
        """Find runs that appear stuck without ready work or active leases."""
        runtime = self._runtime()
        if runtime is None:
            return []
        threshold = max(45, self.lease_timeout_seconds * 2)
        now = datetime.now(timezone.utc)
        stuck: List[StuckRunSummary] = []
        for run in runtime.list_runs(limit=500):
            if run.status not in {"queued", "running"}:
                continue
            try:
                session = runtime.get_session(run.session_id)
            except ValueError:
                continue
            snapshot = runtime.run_coordinator.coordination_snapshot(run, session)
            ready_count = snapshot.counts_by_status.get("ready", 0)
            leased_count = snapshot.counts_by_status.get("leased", 0) + snapshot.counts_by_status.get("running", 0)
            updated_at = self.utilities.utc_datetime(run.updated_at)
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

    def _get_run(self, run_id: str) -> ResearchRun:
        """Get run by ID."""
        row = self.database.fetchone(
            "SELECT payload_json FROM runs WHERE run_id = ?", (run_id,)
        )
        if not row:
            raise ValueError(f"Run not found: {run_id}")
        return ResearchRun(**json.loads(row["payload_json"]))

    def _get_session(self, session_id: str) -> ResearchSession:
        """Get session by ID."""
        row = self.database.fetchone(
            "SELECT payload_json FROM sessions WHERE session_id = ?", (session_id,)
        )
        if not row:
            raise ValueError(f"Session not found: {session_id}")
        return ResearchSession(**json.loads(row["payload_json"]))

    def _get_node(self, task_graph, node_id: str) -> TaskNode:
        """Get node from task graph."""
        if not task_graph:
            raise ValueError("Task graph is missing")
        for node in task_graph.nodes:
            if node.node_id == node_id:
                return node
        raise ValueError(f"Node not found: {node_id}")

    def _persist_run(self, run: ResearchRun, conn=None) -> None:
        """Persist run to database."""
        self.database.upsert_row(
            "runs",
            {
                "run_id": run.run_id,
                "session_id": run.session_id,
                "status": run.status,
                "prompt_frame_id": run.prompt_frame.prompt_frame_id if run.prompt_frame else None,
                "mission_id": run.mission_id,
                "current_attempt_id": run.current_attempt_id,
                "active_lease_id": run.active_lease_id,
                "payload_json": json.dumps(run.model_dump(), ensure_ascii=False),
                "created_at": run.created_at,
                "updated_at": run.updated_at,
            },
            "run_id",
            conn=conn,
        )

    def _persist_session(self, session: ResearchSession, conn=None) -> None:
        """Persist session to database."""
        self.database.upsert_row(
            "sessions",
            {
                "session_id": session.session_id,
                "goal": session.goal,
                "status": session.status,
                "active_policy_id": session.active_policy_id,
                "workflow_template_id": session.workflow_template_id,
                "constraint_set_id": session.constraint_set_id,
                "context_profile_id": session.context_profile_id,
                "prompt_template_id": session.prompt_template_id,
                "model_profile_id": session.model_profile_id,
                "execution_mode": session.execution_mode,
                "payload_json": json.dumps(session.model_dump(), ensure_ascii=False),
                "created_at": session.created_at,
                "updated_at": session.updated_at,
            },
            "session_id",
            conn=conn,
        )

    def _apply_worker_batch(
        self,
        run: ResearchRun,
        batch: WorkerEventBatch,
        session: Optional[ResearchSession] = None,
    ) -> None:
        """Apply worker event batch to run."""
        if session is None:
            session = self._get_session(run.session_id)
        
        for event in batch.events:
            self.database.append_event(
                event.event_type,
                event.payload,
                session_id=session.session_id,
                run_id=run.run_id,
            )
        
        if run.execution_trace:
            run.execution_trace.model_calls.extend(batch.model_calls)
            run.execution_trace.tool_calls.extend(batch.tool_calls)
            run.execution_trace.recovery_events.extend(batch.recovery_events)
            run.execution_trace.artifacts.extend(batch.artifacts)
            run.execution_trace.updated_at = utc_now()
        
        for artifact in batch.artifacts:
            self.database.record_artifact_ref(artifact)

    def _reclaim_lease(self, lease: WorkerLease) -> bool:
        """Reclaim a single expired lease."""
        current_time = utc_now()
        lease.status = "expired"
        lease.heartbeat_at = current_time
        lease.updated_at = current_time
        self.database.upsert_lease(lease)
        self.dispatch_queue.clear_lease(lease.lease_id)
        
        # Update attempt
        attempt = self.database.get_attempt(lease.attempt_id)
        attempt.status = "expired"
        attempt.error = "lease expired before completion"
        attempt.finished_at = current_time
        attempt.updated_at = current_time
        self.database.upsert_attempt(attempt)
        
        try:
            run = self._get_run(lease.run_id)
            session = self._get_session(run.session_id)
            node = self._get_node(session.task_graph, lease.task_node_id)
            
            if node.status in {"leased", "running"}:
                node.status = "ready"
                node.metadata["reclaimed_from_lease_id"] = lease.lease_id
                node.metadata["retry_index"] = attempt.retry_index + 1
                constraints = self.constraints.constraint_for_node(node, session)
                self.dispatch_queue.enqueue_ready_task(run.run_id, node.node_id, shard=constraints.queue_shard)
            
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
                self._persist_run(run)
                self._persist_session(session)
            
            mission = self.database.get_mission_by_run(run.run_id)
            if mission and mission.status not in {"completed", "failed"}:
                mission.status = "running"
                mission.updated_at = current_time
                self.database.upsert_mission(mission)
            
            self.database.append_event(
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
            return True
        except ValueError:
            return False

    def _record_ignored_completion(
        self,
        lease: WorkerLease,
        event_type: str,
        error: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        """Record ignored lease completion event."""
        try:
            run = self._get_run(lease.run_id)
            session = self._get_session(run.session_id)
            payload = {
                "worker_id": lease.worker_id,
                "lease_id": lease.lease_id,
                "attempt_id": lease.attempt_id,
                "task_node_id": lease.task_node_id,
                "status": lease.status,
            }
            if error:
                payload["error"] = error
            if reason:
                payload["reason"] = reason
            
            self.database.append_event(
                event_type,
                payload,
                session_id=session.session_id,
                run_id=run.run_id,
            )
        except ValueError:
            pass

    def _append_lease_event(
        self,
        event_type: str,
        lease: WorkerLease,
        run: ResearchRun,
        session: ResearchSession,
        error: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        """Append lease-related event."""
        payload = {
            "worker_id": lease.worker_id,
            "lease_id": lease.lease_id,
            "attempt_id": lease.attempt_id,
            "task_node_id": lease.task_node_id,
        }
        if error:
            payload["error"] = error
        if reason:
            payload["reason"] = reason
        
        self.database.append_event(
            event_type,
            payload,
            session_id=session.session_id,
            run_id=run.run_id,
        )

    def _worker_can_poll_shard(self, worker: WorkerSnapshot, shard: str) -> bool:
        """Check if worker can poll tasks from shard."""
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
