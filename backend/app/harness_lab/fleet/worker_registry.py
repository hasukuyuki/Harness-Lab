"""Worker Registry - evolved from workers/service.py.

Current Status:
    WorkerService in workers/service.py contains both:
    1. Worker lifecycle management (register, heartbeat, state)
    2. Worker acquisition/release (acquire_worker, release_worker)

Migration Plan:
    - Part 1: Worker lifecycle -> fleet/worker_registry.py (this file)
    - Part 2: Worker acquisition -> fleet/coordinator.py
    
Rationale:
    Worker registry should be a pure data layer, while acquisition
    strategy belongs to fleet coordination.

WebSocket Hooks (added 2026-04-18):
    - register_worker: broadcast worker.registered
    - heartbeat: broadcast worker.heartbeat, worker.state_changed
    - drain_worker: broadcast worker.drain
    - resume_worker: broadcast worker.resume
    - set_state: broadcast worker.state_changed
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import List, Optional, TYPE_CHECKING

from ..storage import PlatformStore
from ..types import (
    WorkerHeartbeatRequest,
    WorkerRegisterRequest,
    WorkerSandboxStats,
    WorkerSnapshot,
    WorkerState,
)
from ..utils import new_id, utc_now

if TYPE_CHECKING:
    from ..control_plane.websocket_publisher import WebSocketEventPublisher


class WorkerRegistry:
    """Pure data layer for worker lifecycle management.
    
    Evolved from workers/service.py WorkerService.
    Does NOT handle worker acquisition strategy (moved to fleet coordinator).
    
    WebSocket Integration:
        Optionally publishes worker lifecycle events via WebSocketEventPublisher.
        Events: registered, heartbeat, state_changed, drain, resume, offline, unhealthy
    """

    def __init__(
        self,
        database: PlatformStore,
        ws_publisher: Optional[WebSocketEventPublisher] = None,
    ) -> None:
        """Initialize worker registry.
        
        Args:
            database: PlatformStore for persistence
            ws_publisher: WebSocketEventPublisher for event broadcasting (optional)
        """
        self.database = database
        self.ws_publisher = ws_publisher
        self.offline_after_seconds = 90
    
    def set_ws_publisher(self, publisher: WebSocketEventPublisher) -> None:
        """Set WebSocket publisher after initialization."""
        self.ws_publisher = publisher

    def list_workers(self) -> List[WorkerSnapshot]:
        """List all workers with derived state."""
        rows = self.database.fetchall("SELECT payload_json FROM workers ORDER BY updated_at DESC")
        return [self._derive_state(WorkerSnapshot(**json.loads(row["payload_json"]))) for row in rows]

    def get_worker(self, worker_id: str) -> WorkerSnapshot:
        """Get worker by ID."""
        row = self.database.fetchone("SELECT payload_json FROM workers WHERE worker_id = ?", (worker_id,))
        if not row:
            raise ValueError(f"Worker not found: {worker_id}")
        return self._derive_state(WorkerSnapshot(**json.loads(row["payload_json"])))

    def register_worker(self, request: WorkerRegisterRequest) -> WorkerSnapshot:
        """Register a new worker.
        
        WebSocket Hook: broadcasts worker.registered event.
        """
        now = utc_now()
        role_suffix = request.role_profile or "general"
        worker_class = "sandboxed" if request.sandbox_ready else "general"
        
        # Initialize sandbox stats
        sandbox_stats = WorkerSandboxStats()
        
        snapshot = WorkerSnapshot(
            worker_id=request.worker_id or new_id("worker"),
            label=request.label,
            state="idle",
            drain_state="active",
            capabilities=request.capabilities,
            role_profile=request.role_profile,
            hostname=request.hostname,
            pid=request.pid,
            labels=request.labels,
            eligible_labels=list(request.labels),
            worker_class=f"{role_suffix}-{worker_class}",
            execution_mode=request.execution_mode,
            heartbeat_at=now,
            lease_count=0,
            version=request.version,
            current_run_id=None,
            current_task_node_id=None,
            current_lease_id=None,
            sandbox_backend=request.sandbox_backend,
            sandbox_ready=request.sandbox_ready,
            sandbox_hardened_ready=request.sandbox_hardened_ready,
            sandbox_stats=sandbox_stats,
            last_error=None,
            created_at=now,
            updated_at=now,
        )
        self._persist(snapshot)
        
        # WebSocket hook: broadcast worker registered
        if self.ws_publisher:
            self.ws_publisher.broadcast_worker_registered(
                worker_id=snapshot.worker_id,
                label=snapshot.label,
                role=snapshot.role_profile or "general",
                capabilities=snapshot.capabilities,
                hostname=snapshot.hostname,
                pid=snapshot.pid,
            )
        
        return snapshot

    def heartbeat(self, worker_id: str, request: WorkerHeartbeatRequest) -> WorkerSnapshot:
        """Update worker heartbeat.
        
        WebSocket Hook: broadcasts worker.heartbeat and worker.state_changed if state changed.
        """
        worker = self.get_worker(worker_id)
        old_state = worker.state
        
        worker.state = request.state
        worker.lease_count = request.lease_count
        worker.current_run_id = request.current_run_id
        worker.current_task_node_id = request.current_task_node_id
        worker.last_error = request.last_error
        if request.role_profile is not None:
            worker.role_profile = request.role_profile
        if request.sandbox_backend is not None:
            worker.sandbox_backend = request.sandbox_backend
        if request.sandbox_ready is not None:
            worker.sandbox_ready = request.sandbox_ready
        if request.current_lease_id is not None:
            worker.current_lease_id = request.current_lease_id
        elif request.current_task_node_id is None:
            worker.current_lease_id = None
        worker.heartbeat_at = utc_now()
        worker.updated_at = worker.heartbeat_at
        if worker.drain_state == "draining" and worker.current_lease_id:
            worker.state = "draining"
        self._persist(worker)
        
        # WebSocket hooks
        if self.ws_publisher:
            # Broadcast heartbeat
            self.ws_publisher.broadcast_worker_heartbeat(
                worker_id=worker.worker_id,
                state=worker.state,
                lease_count=worker.lease_count,
                current_run_id=worker.current_run_id,
                current_lease_id=worker.current_lease_id,
            )
            
            # Broadcast state change if different
            if old_state != worker.state:
                self.ws_publisher.broadcast_worker_state_changed(
                    worker_id=worker.worker_id,
                    old_state=old_state,
                    new_state=worker.state,
                    current_run_id=worker.current_run_id,
                    current_lease_id=worker.current_lease_id,
                )
        
        return worker

    def record_sandbox_execution(
        self,
        worker_id: str,
        success: bool,
        timed_out: bool = False,
        policy_denied: bool = False,
        approval_blocked: bool = False,
        error: Optional[str] = None,
    ) -> WorkerSnapshot:
        """Record sandbox execution statistics for a worker."""
        worker = self.get_worker(worker_id)
        
        # Initialize stats if not present
        if worker.sandbox_stats is None:
            worker.sandbox_stats = WorkerSandboxStats()
        
        # Update stats
        worker.sandbox_stats.total_executions += 1
        
        if success:
            worker.sandbox_stats.success_count += 1
        else:
            worker.sandbox_stats.failure_count += 1
            worker.sandbox_stats.last_failure_at = utc_now()
            worker.sandbox_stats.last_failure_reason = error
        
        if timed_out:
            worker.sandbox_stats.timeout_count += 1
        
        if policy_denied:
            worker.sandbox_stats.policy_denied_count += 1
        
        if approval_blocked:
            worker.sandbox_stats.approval_blocked_count += 1
        
        worker.updated_at = utc_now()
        self._persist(worker)
        return worker

    def set_drain_state(self, worker_id: str, drain_state: str, reason: Optional[str] = None) -> WorkerSnapshot:
        """Set worker drain state (active/draining).
        
        WebSocket Hook: broadcasts worker.drain or worker.resume.
        """
        worker = self.get_worker(worker_id)
        old_state = worker.state
        
        worker.drain_state = drain_state  # type: ignore
        if drain_state == "draining" and not worker.current_lease_id:
            worker.state = "draining"
        elif drain_state == "active" and not worker.current_lease_id:
            worker.state = "idle"
        if reason:
            worker.last_error = reason
        worker.updated_at = utc_now()
        self._persist(worker)
        
        # WebSocket hook
        if self.ws_publisher:
            if drain_state == "draining":
                self.ws_publisher.broadcast_worker_drain(
                    worker_id=worker.worker_id,
                    reason=reason,
                )
            elif drain_state == "active":
                self.ws_publisher.broadcast_worker_resume(
                    worker_id=worker.worker_id,
                )
            
            # Also broadcast state change if different
            if old_state != worker.state:
                self.ws_publisher.broadcast_worker_state_changed(
                    worker_id=worker.worker_id,
                    old_state=old_state,
                    new_state=worker.state,
                )
        
        return worker
    
    def drain_worker(self, worker_id: str, reason: Optional[str] = None) -> WorkerSnapshot:
        """Put a worker into draining mode.
        
        WebSocket Hook: broadcasts worker.drain.
        """
        return self.set_drain_state(worker_id, "draining", reason=reason)

    def resume_worker(self, worker_id: str) -> WorkerSnapshot:
        """Resume a draining worker so it can accept new leases."""
        return self.set_drain_state(worker_id, "active")

    def ensure_default_worker(self) -> WorkerSnapshot:
        """Ensure default control-plane worker exists and is active.
        
        If the worker exists but is offline, update heartbeat to make it active.
        This ensures that the default worker can actually execute tasks.
        """
        row = self.database.fetchone(
            "SELECT payload_json FROM workers WHERE worker_id = ?",
            ("worker_control_plane_local",)
        )
        if row:
            worker = WorkerSnapshot(**json.loads(row["payload_json"]))
            # Apply state derivation to check if offline
            worker = self._derive_state(worker)
            # If offline, re-activate by updating heartbeat
            if worker.state == "offline":
                now = utc_now()
                worker.state = "idle"
                worker.heartbeat_at = now
                worker.updated_at = now
                self._persist(worker)
            return worker
        return self.register_worker(
            WorkerRegisterRequest(
                worker_id="worker_control_plane_local",
                label="control-plane-local",
                capabilities=["filesystem", "git", "http_fetch", "knowledge_search", "model_reflection", "shell"],
                role_profile=None,
                labels=["control-plane", "local"],
                execution_mode="embedded",
                sandbox_backend=None,
                sandbox_ready=False,
                version="v1",
            )
        )

    def acquire_worker(self, run_id: str, task_node_id: Optional[str] = None) -> WorkerSnapshot:
        """Acquire a worker for executing a task.
        
        Transitions worker to 'executing' state and assigns run/task.
        Only acquires workers that are active (idle/registering state, not draining).
        Falls back to ensure_default_worker() if no active workers are available.
        """
        workers = self.list_workers()
        # Find an active worker (idle or registering, not draining)
        worker = next(
            (item for item in workers if item.drain_state == "active" and item.state in {"idle", "registering"}),
            None,
        )
        # If no active worker available, use default worker (which will be re-activated if offline)
        if worker is None:
            worker = self.ensure_default_worker()
        worker.state = "executing"
        worker.current_run_id = run_id
        worker.current_task_node_id = task_node_id
        worker.current_lease_id = None
        worker.lease_count += 1
        worker.heartbeat_at = utc_now()
        worker.updated_at = worker.heartbeat_at
        self._persist(worker)
        return worker

    def release_worker(self, worker_id: str, error: Optional[str] = None) -> WorkerSnapshot:
        """Release a worker after task completion.
        
        Transitions worker back to appropriate state (idle/draining/unhealthy).
        """
        worker = self.get_worker(worker_id)
        if worker.drain_state == "draining" and not error:
            worker.state = "draining"
        elif error:
            worker.state = "unhealthy"
        else:
            worker.state = "idle"
        worker.current_run_id = None
        worker.current_task_node_id = None
        worker.current_lease_id = None
        worker.last_error = error
        worker.heartbeat_at = utc_now()
        worker.updated_at = worker.heartbeat_at
        self._persist(worker)
        return worker

    def _persist(self, worker: WorkerSnapshot, conn: object | None = None) -> None:
        """Persist worker to database."""
        self.database.upsert_row(
            "workers",
            {
                "worker_id": worker.worker_id,
                "label": worker.label,
                "state": worker.state,
                "heartbeat_at": worker.heartbeat_at,
                "payload_json": json.dumps(worker.model_dump(), ensure_ascii=False),
                "created_at": worker.created_at,
                "updated_at": worker.updated_at,
            },
            "worker_id",
            conn=conn,
        )

    def _derive_state(self, worker: WorkerSnapshot) -> WorkerSnapshot:
        """Derive worker state from heartbeat and lease.
        
        WebSocket Hook: broadcasts worker.offline or worker.unhealthy if state changes.
        """
        heartbeat = datetime.fromisoformat(worker.heartbeat_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        old_state = worker.state
        
        if worker.current_lease_id:
            try:
                lease = self.database.get_lease(worker.current_lease_id)
                if worker.drain_state == "draining":
                    worker.state = "draining"
                else:
                    worker.state = "executing" if lease.status == "running" else "leased"
            except ValueError:
                # Lease no longer exists
                worker.current_lease_id = None
                worker.current_run_id = None
                worker.current_task_node_id = None
                worker.state = "draining" if worker.drain_state == "draining" else "idle"
        elif now - heartbeat > timedelta(seconds=self.offline_after_seconds):
            worker.state = "offline"
        elif worker.drain_state == "draining":
            worker.state = "draining"
        elif worker.last_error:
            worker.state = "unhealthy"
        else:
            worker.state = "idle"
        
        # WebSocket hook for derived state changes
        if self.ws_publisher and old_state != worker.state:
            if worker.state == "offline":
                self.ws_publisher.broadcast_worker_offline(
                    worker_id=worker.worker_id,
                    last_heartbeat_at=worker.heartbeat_at,
                )
            elif worker.state == "unhealthy":
                self.ws_publisher.broadcast_worker_unhealthy(
                    worker_id=worker.worker_id,
                    error=worker.last_error,
                )
            else:
                self.ws_publisher.broadcast_worker_state_changed(
                    worker_id=worker.worker_id,
                    old_state=old_state,
                    new_state=worker.state,
                )
        
        return worker
