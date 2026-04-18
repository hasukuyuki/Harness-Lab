"""Dispatcher - task dispatch logic evolved from dispatch_queue.py.

This module consolidates:
    1. dispatch_queue.py: Redis-backed ready queue and lease expiry index
    2. LeaseManager dispatch logic: worker-to-task matching

Design:
    - Dispatcher owns the queue and matching algorithm
    - WorkerRegistry provides worker state
    - LeaseManager owns lease lifecycle

WebSocket Hooks (added 2026-04-18):
    - _create_dispatch: broadcasts lease.acquired and queue.dispatched
    - _reclaim_lease: broadcasts lease.expired (via LeaseManager)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from ..storage import PlatformStore
    from ..types import (
        DispatchEnvelope,
        ResearchRun,
        ResearchSession,
        TaskNode,
        WorkerSnapshot,
    )
    from .protocols import (
        RunCoordinationProtocol,
        DispatchConstraintProtocol,
        DispatchContextProtocol,
    )
    from ..control_plane.websocket_publisher import WebSocketEventPublisher


class Dispatcher:
    """Dispatches ready tasks to available workers.
    
    Combines queue management from dispatch_queue.py with
    matching logic from LeaseManager.
    
    WebSocket Integration:
        Optionally publishes dispatch events via WebSocketEventPublisher.
        Events: lease.acquired, queue.dispatched, queue.enqueued
    """
    
    def __init__(
        self,
        queue,
        worker_registry,
        database: PlatformStore,
        lease_timeout_seconds: int = 30,
        ws_publisher: Optional[WebSocketEventPublisher] = None,
    ) -> None:
        """Initialize dispatcher.
        
        Args:
            queue: DispatchQueue or InMemoryDispatchQueue instance
            worker_registry: WorkerRegistry for worker state
            database: PlatformStore for persistence
            lease_timeout_seconds: Default lease timeout
            ws_publisher: WebSocketEventPublisher (optional)
        """
        self.queue = queue
        self.worker_registry = worker_registry
        self.database = database
        self.lease_timeout_seconds = lease_timeout_seconds
        self.ws_publisher = ws_publisher
        self.reclaimed_lease_count = 0
    
    def set_ws_publisher(self, publisher: WebSocketEventPublisher) -> None:
        """Set WebSocket publisher after initialization."""
        self.ws_publisher = publisher
    
    def get_queue_depth(self, shard: Optional[str] = None) -> int:
        """Get depth of ready queue."""
        return self.queue.ready_queue_depth(shard)
    
    def get_queue_depth_by_shard(self) -> Dict[str, int]:
        """Get queue depth per shard."""
        return self.queue.queue_depth_by_shard()
    
    def inspect_queues(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Inspect queue contents."""
        return self.queue.inspect_queues(limit)
    
    def next_dispatch_for_worker(
        self,
        worker: WorkerSnapshot,
        coordination: RunCoordinationProtocol,
        constraints: DispatchConstraintProtocol,
        context: DispatchContextProtocol,
        reclaim_first: bool = True,
    ) -> Optional[DispatchEnvelope]:
        """Find next dispatch for worker.
        
        This is the primary dispatch entry point. It:
        1. Optionally reclaims stale leases first
        2. Checks worker drain state and worker state (offline/unhealthy workers skipped)
        3. Polls ready queue for candidate tasks
        4. Validates task constraints match worker capabilities
        5. Creates dispatch envelope on match
        
        Args:
            worker: WorkerSnapshot to dispatch to
            coordination: Protocol for run coordination
            constraints: Protocol for constraint checking
            context: Protocol for building dispatch context
            reclaim_first: Whether to reclaim stale leases before dispatch
            
        Returns:
            DispatchEnvelope if task assigned, None otherwise
            Returns None if worker is draining, offline, or unhealthy
        """
        if reclaim_first:
            self._reclaim_stale_leases(coordination, constraints)
        
        # Draining workers don't receive new dispatches
        if worker.drain_state == "draining":
            return None
        
        # Offline or unhealthy workers cannot execute tasks
        if worker.state in {"offline", "unhealthy"}:
            return None
        
        inspected = 0
        max_checks = max(1, self.queue.ready_queue_depth())
        
        while inspected < max_checks:
            # Determine which shards this worker can poll from
            queue_snapshot = self.queue.inspect_queues(limit=1)
            eligible_shards = [
                str(shard["shard"])
                for shard in queue_snapshot
                if self._worker_can_poll_shard(worker, str(shard["shard"]))
            ]
            
            # Try to get a candidate task
            candidate = self.queue.pop_ready_task(shards=eligible_shards or None)
            if candidate is None:
                return None
            
            run_id, task_node_id, shard = candidate
            inspected += 1
            
            # Validate run/session/node exist and are valid
            try:
                run = self._get_run(run_id)
                session = self._get_session(run.session_id)
                node = self._get_node(session.task_graph, task_node_id)
            except (ValueError, AttributeError):
                continue
            
            # Skip if run is in terminal state
            if run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                continue
            
            # Ensure ready nodes are marked
            coordination.mark_ready_nodes(session, run.run_id)
            
            # Skip if node is no longer ready
            if node.status != "ready":
                continue
            
            # Check worker constraints match
            if not constraints.worker_matches_node(worker, session, node):
                # Requeue for another worker
                constraint_info = constraints.constraint_for_node(node, session)
                self.queue.requeue_ready_task(
                    run.run_id,
                    node.node_id,
                    shard=constraint_info.queue_shard or shard,
                    delay_seconds=0.5,  # Brief delay to prevent immediate retry
                )
                continue
            
            # Create dispatch envelope
            try:
                return self._create_dispatch(
                    run, session, node, worker.worker_id,
                    coordination, constraints, context,
                )
            except ValueError:
                # Dispatch creation failed, continue to next candidate
                continue
        
        return None
    
    def _create_dispatch(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
        worker_id: str,
        coordination: RunCoordinationProtocol,
        constraints: DispatchConstraintProtocol,
        context: DispatchContextProtocol,
    ) -> DispatchEnvelope:
        """Create dispatch envelope within a transaction.
        
        This method performs the actual lease creation and state updates
        atomically within a database transaction.
        
        WebSocket Hook: broadcasts lease.acquired and queue.dispatched.
        """
        from ..types import TaskAttempt, WorkerLease
        from ..utils import new_id, utc_now
        
        now = utc_now()
        
        with self.database.transaction() as conn:
            # Lock run and session for update
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
            
            # Refresh objects from locked rows
            from ..types import ResearchRun, ResearchSession
            run = ResearchRun(**json.loads(locked_run["payload_json"]))
            session = ResearchSession(**json.loads(locked_session["payload_json"]))
            node = self._get_node(session.task_graph, node.node_id)
            
            # Double-check node is still ready
            if node.status != "ready":
                raise ValueError(f"Task node is no longer ready: {node.node_id}")
            
            # Calculate retry index
            retry_index = len([
                item for item in self.database.list_attempts(run_id=run.run_id, conn=conn)
                if item.task_node_id == node.node_id
            ])
            
            # Create attempt
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
            from datetime import timedelta
            lease = WorkerLease(
                lease_id=new_id("lease"),
                worker_id=worker_id,
                run_id=run.run_id,
                task_node_id=node.node_id,
                attempt_id=attempt.attempt_id,
                status="leased",
                approval_token=context.get_approval_token(run.run_id),
                expires_at=(datetime.now(timezone.utc) + timedelta(seconds=self.lease_timeout_seconds)).isoformat(),
                heartbeat_at=now,
                created_at=now,
                updated_at=now,
            )
            
            attempt.lease_id = lease.lease_id
            self.database.upsert_attempt(attempt, conn=conn)
            self.database.upsert_lease(lease, conn=conn)
            
            # Update node status
            from ..orchestrator.service import OrchestratorService
            orchestrator = OrchestratorService()
            orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "leased",
                {
                    "worker_id": worker_id,
                    "lease_id": lease.lease_id,
                    "attempt_id": attempt.attempt_id,
                    "leased_at": now,
                },
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
            from ..types import Mission
            mission = self.database.get_mission_by_run(run.run_id, conn=conn)
            if mission:
                mission.status = "running"
                mission.updated_at = now
                self.database.upsert_mission(mission, conn=conn)
            
            # Update worker
            worker = self.worker_registry.get_worker(worker_id)
            worker.state = "leased"
            worker.current_run_id = run.run_id
            worker.current_task_node_id = node.node_id
            worker.current_lease_id = lease.lease_id
            worker.lease_count += 1
            worker.heartbeat_at = now
            worker.updated_at = now
            self.worker_registry._persist(worker, conn=conn)
            
            # Record event
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
        
        # Track lease expiry (outside transaction for speed)
        self.queue.track_lease_expiry(
            lease.lease_id,
            datetime.fromisoformat(lease.expires_at.replace("Z", "+00:00")).timestamp()
        )
        
        # Get shard from constraints
        constraint_info = constraints.constraint_for_node(node, session)
        shard = constraint_info.queue_shard or "general/normal/unlabeled"
        
        # WebSocket hooks: broadcast lease acquired and queue dispatched
        if self.ws_publisher:
            self.ws_publisher.broadcast_lease_acquired(
                lease_id=lease.lease_id,
                worker_id=worker_id,
                task_node_id=node.node_id,
                run_id=run.run_id,
                attempt_id=attempt.attempt_id,
            )
            self.ws_publisher.broadcast_queue_dispatched(
                shard=shard,
                task_node_id=node.node_id,
                worker_id=worker_id,
                lease_id=lease.lease_id,
                run_id=run.run_id,
            )
        
        # Build and return dispatch envelope
        return context.build_dispatch(run, session, node, worker, lease.lease_id, attempt.attempt_id)
    
    def _reclaim_stale_leases(
        self,
        coordination: RunCoordinationProtocol,
        constraints: DispatchConstraintProtocol,
    ) -> int:
        """Reclaim expired leases and return count reclaimed."""
        from datetime import timedelta
        from ..utils import utc_now
        
        now = datetime.now(timezone.utc)
        expired_lease_ids = set(self.queue.pop_expired_leases(now.timestamp()))
        
        # Also check database for expired leases not in index
        for lease in self.database.list_leases():
            if lease.status in {"leased", "running"}:
                expires_at = datetime.fromisoformat(lease.expires_at.replace("Z", "+00:00"))
                if expires_at < now:
                    expired_lease_ids.add(lease.lease_id)
        
        reclaimed = 0
        for lease_id in expired_lease_ids:
            try:
                lease = self.database.get_lease(lease_id)
            except ValueError:
                continue
            if lease.status not in {"leased", "running"}:
                continue
            
            if self._reclaim_lease(lease, coordination, constraints):
                reclaimed += 1
        
        self.reclaimed_lease_count += reclaimed
        return reclaimed
    
    def _reclaim_lease(
        self,
        lease,
        coordination: RunCoordinationProtocol,
        constraints: DispatchConstraintProtocol,
    ) -> bool:
        """Reclaim a single expired lease."""
        from ..utils import utc_now
        from ..types import Mission
        
        current_time = utc_now()
        
        # Update lease
        lease.status = "expired"
        lease.heartbeat_at = current_time
        lease.updated_at = current_time
        self.database.upsert_lease(lease)
        self.queue.clear_lease(lease.lease_id)
        
        # Update attempt
        attempt = self.database.get_attempt(lease.attempt_id)
        attempt.status = "expired"
        attempt.error = "lease expired before completion"
        attempt.finished_at = current_time
        attempt.updated_at = current_time
        self.database.upsert_attempt(attempt)
        
        try:
            # Get run/session/node
            run = self._get_run(lease.run_id)
            session = self._get_session(run.session_id)
            node = self._get_node(session.task_graph, lease.task_node_id)
            
            # Reset node to ready
            if node.status in {"leased", "running"}:
                node.status = "ready"
                node.metadata["reclaimed_from_lease_id"] = lease.lease_id
                node.metadata["retry_index"] = attempt.retry_index + 1
                constraint_info = constraints.constraint_for_node(node, session)
                self.queue.enqueue_ready_task(run.run_id, node.node_id, shard=constraint_info.queue_shard)
            
            # Update run if not terminal
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
            
            # Update mission
            mission = self.database.get_mission_by_run(run.run_id)
            if mission and mission.status not in {"completed", "failed"}:
                mission.status = "running"
                mission.updated_at = current_time
                self.database.upsert_mission(mission)
            
            # Record event
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
            
            # Update worker
            try:
                worker = self.worker_registry.get_worker(lease.worker_id)
                if worker.current_lease_id == lease.lease_id:
                    worker.current_lease_id = None
                    worker.current_run_id = None
                    worker.current_task_node_id = None
                    if worker.drain_state == "draining":
                        worker.state = "draining"
                    elif worker.last_error:
                        worker.state = "unhealthy"
                    else:
                        worker.state = "idle"
                    worker.updated_at = current_time
                    self.worker_registry._persist(worker)
            except ValueError:
                pass
            
            return True
        except ValueError:
            return False
    
    def _worker_can_poll_shard(self, worker: WorkerSnapshot, shard: str) -> bool:
        """Check if worker can poll tasks from shard.
        
        Shard format: "{role}/{risk_level}/{label1}/{label2}/..."
        
        Worker must be:
        - Not in draining state
        - Not offline or unhealthy
        """
        # Draining workers don't receive new dispatches
        if worker.drain_state == "draining":
            return False
        
        # Offline or unhealthy workers cannot execute tasks
        if worker.state in {"offline", "unhealthy"}:
            return False
        
        parts = shard.split("/")
        role = parts[0] if parts else None
        labels = parts[2:] if len(parts) > 2 else []
        
        # Check role match
        if worker.role_profile and role and worker.role_profile != role:
            return False
        
        # Check label requirements
        worker_labels = set(worker.labels or [])
        if labels and "unlabeled" not in labels:
            if not all(label in worker_labels for label in labels):
                return False
        
        return True
    
    def _get_run(self, run_id: str):
        """Get run by ID."""
        row = self.database.fetchone(
            "SELECT payload_json FROM runs WHERE run_id = ?", (run_id,)
        )
        if not row:
            raise ValueError(f"Run not found: {run_id}")
        from ..types import ResearchRun
        return ResearchRun(**json.loads(row["payload_json"]))
    
    def _get_session(self, session_id: str):
        """Get session by ID."""
        row = self.database.fetchone(
            "SELECT payload_json FROM sessions WHERE session_id = ?", (session_id,)
        )
        if not row:
            raise ValueError(f"Session not found: {session_id}")
        from ..types import ResearchSession
        return ResearchSession(**json.loads(row["payload_json"]))
    
    def _get_node(self, task_graph, node_id: str):
        """Get node from task graph."""
        if not task_graph:
            raise ValueError("Task graph is missing")
        for node in task_graph.nodes:
            if node.node_id == node_id:
                return node
        raise ValueError(f"Node not found: {node_id}")
    
    def _persist_run(self, run, conn=None) -> None:
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
    
    def _persist_session(self, session, conn=None) -> None:
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
    
    # === Multi-Agent Dispatch Extensions ===
    # Design source: Claude Plugin Module 05 - Coordinator Mode
    
    def next_dispatch_for_parallel_workers(
        self,
        workers: List[WorkerSnapshot],
        coordination: RunCoordinationProtocol,
        constraints: DispatchConstraintProtocol,
        context: DispatchContextProtocol,
        max_parallel: int = 4,
    ) -> List[Tuple[WorkerSnapshot, DispatchEnvelope]]:
        """Dispatch tasks to multiple workers in parallel wave.
        
        For multi-agent execution mode. Assigns ready tasks
        to available workers based on role matching.
        
        Args:
            workers: List of available WorkerSnapshot
            coordination: Protocol for run coordination
            constraints: Protocol for constraint checking
            context: Protocol for building dispatch context
            max_parallel: Maximum parallel dispatches
            
        Returns:
            List of (worker, dispatch_envelope) tuples
        """
        if not workers:
            return []
        
        # Filter available workers
        available = [w for w in workers if w.state in {"idle", "registering"} and w.drain_state != "draining"]
        
        if not available:
            return []
        
        # Group workers by role
        role_workers: Dict[str, List[WorkerSnapshot]] = {}
        for worker in available:
            role = worker.role_profile or "executor"
            if role not in role_workers:
                role_workers[role] = []
            role_workers[role].append(worker)
        
        # Get ready queue snapshot
        queue_snapshot = self.queue.inspect_queues(limit=max_parallel * 2)
        
        # Match tasks to workers by role
        dispatches: List[Tuple[WorkerSnapshot, DispatchEnvelope]] = []
        
        for shard_info in queue_snapshot:
            shard = str(shard_info.get("shard", ""))
            parts = shard.split("/")
            shard_role = parts[0] if parts else "executor"
            
            # Find worker with matching role
            matching_workers = role_workers.get(shard_role, [])
            if not matching_workers:
                # Fall back to any available worker
                matching_workers = available
            
            for worker in matching_workers[:max_parallel]:
                if len(dispatches) >= max_parallel:
                    break
                
                # Check if this worker already has a dispatch
                if any(d[0].worker_id == worker.worker_id for d in dispatches):
                    continue
                
                # Try to dispatch to this worker
                dispatch = self.next_dispatch_for_worker(
                    worker,
                    coordination,
                    constraints,
                    context,
                    reclaim_first=False,  # Already reclaimed above
                )
                
                if dispatch:
                    dispatches.append((worker, dispatch))
        
        return dispatches
    
    def _find_worker_for_role(
        self,
        role: str,
        workers: List[WorkerSnapshot],
    ) -> Optional[WorkerSnapshot]:
        """Find best worker for a specific role.
        
        Priority:
        1. Worker with matching role_profile
        2. Worker with role in labels
        3. Any available worker
        
        Args:
            role: AgentRole to match
            workers: List of available workers
            
        Returns:
            Best matching WorkerSnapshot or None
        """
        # First: exact role match
        for worker in workers:
            if worker.role_profile == role:
                return worker
        
        # Second: role in labels
        for worker in workers:
            if role in (worker.labels or []):
                return worker
        
        # Third: any available
        for worker in workers:
            if worker.state in {"idle", "registering"}:
                return worker
        
        return None
    
    def _worker_can_poll_shard_extended(
        self,
        worker: WorkerSnapshot,
        shard: str,
        require_role_match: bool = False,
    ) -> bool:
        """Extended shard polling check with optional role requirement.
        
        Args:
            worker: WorkerSnapshot
            shard: Shard string
            require_role_match: If True, require exact role match
            
        Returns:
            True if worker can poll from shard
            
        Worker must be:
        - Not in draining state
        - Not offline or unhealthy
        """
        # Draining workers don't receive new dispatches
        if worker.drain_state == "draining":
            return False
        
        # Offline or unhealthy workers cannot execute tasks
        if worker.state in {"offline", "unhealthy"}:
            return False
        
        parts = shard.split("/")
        shard_role = parts[0] if parts else None
        
        # Role matching
        if require_role_match and shard_role:
            if worker.role_profile and worker.role_profile != shard_role:
                return False
            # Also check labels
            if shard_role not in (worker.labels or []) and worker.role_profile != shard_role:
                return False
        
        # Original label check
        labels = parts[2:] if len(parts) > 2 else []
        worker_labels = set(worker.labels or [])
        if labels and "unlabeled" not in labels:
            if not all(label in worker_labels for label in labels):
                return False
        
        return True
    
    def get_role_queue_depth(self, role: str) -> int:
        """Get queue depth for a specific role shard.
        
        Args:
            role: AgentRole to check
            
        Returns:
            Number of tasks in role shard
        """
        depths = self.queue.queue_depth_by_shard()
        role_depth = 0
        for shard, depth in depths.items():
            parts = str(shard).split("/")
            shard_role = parts[0] if parts else "executor"
            if shard_role == role:
                role_depth += depth
        return role_depth
    
    def list_available_workers_by_role(self, role: str) -> List[WorkerSnapshot]:
        """List available workers for a specific role.
        
        Args:
            role: AgentRole to filter
            
        Returns:
            List of available WorkerSnapshot with matching role
        """
        workers = self.worker_registry.list_workers()
        available = [w for w in workers if w.state in {"idle", "registering"} and w.drain_state != "draining"]
        
        # Filter by role
        matching = []
        for worker in available:
            if worker.role_profile == role:
                matching.append(worker)
            elif role in (worker.labels or []):
                matching.append(worker)
        
        # Fall back to all available if no role match
        return matching if matching else available


class InMemoryDispatcher(Dispatcher):
    """In-memory dispatcher for testing without Redis."""
    
    def __init__(
        self,
        worker_registry,
        database: PlatformStore,
        lease_timeout_seconds: int = 30,
        existing_queue=None,
        ws_publisher=None,
    ) -> None:
        from ..dispatch_queue import InMemoryDispatchQueue
        super().__init__(
            queue=existing_queue if existing_queue is not None else InMemoryDispatchQueue(),
            worker_registry=worker_registry,
            database=database,
            lease_timeout_seconds=lease_timeout_seconds,
            ws_publisher=ws_publisher,
        )
