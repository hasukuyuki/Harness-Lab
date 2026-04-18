"""WebSocket Event Publisher - unified interface for Runtime components.

Provides dependency-injectable WebSocket event publishing for:
- WorkerRegistry: worker lifecycle events
- LeaseManager: lease lifecycle events
- Dispatcher: task dispatch events
- Runtime: health broadcast

Design source: Claude Plugin Module 05 - Coordinator Mode
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .websocket import ConnectionManager


class WebSocketEventPublisher:
    """Unified WebSocket event publisher for Runtime components.
    
    Features:
    - Dependency-injectable interface
    - Async-safe broadcasting
    - Event type namespaces (worker.lease.queue.health)
    - Optional publishing (graceful degradation if not configured)
    
    Usage:
        publisher = WebSocketEventPublisher(manager)
        
        # In WorkerRegistry
        publisher.broadcast_worker_event("registered", worker_id, {...})
        
        # In LeaseManager
        publisher.broadcast_lease_event("acquired", lease_id, {...})
        
        # In Dispatcher
        publisher.broadcast_queue_event("dispatched", shard, {...})
    """
    
    def __init__(
        self,
        manager: Optional[ConnectionManager] = None,
        enabled: bool = True,
    ) -> None:
        """Initialize publisher.
        
        Args:
            manager: ConnectionManager instance (optional for graceful degradation)
            enabled: Whether publishing is enabled
        """
        self._manager = manager
        self._enabled = enabled
        self._loop: Optional[asyncio.AbstractEventLoop] = None
    
    def set_manager(self, manager: ConnectionManager) -> None:
        """Set connection manager after initialization.
        
        Allows late binding for dependency injection.
        """
        self._manager = manager
    
    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable publishing."""
        self._enabled = enabled
    
    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Set async loop for background publishing."""
        self._loop = loop
    
    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create async loop."""
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
        return self._loop
    
    def _publish(self, message: Dict[str, Any]) -> None:
        """Publish message to all connections.
        
        Non-blocking: schedules broadcast on event loop.
        """
        if not self._enabled or self._manager is None:
            return
        
        loop = self._get_loop()
        
        # Schedule broadcast (non-blocking)
        if loop.is_running():
            asyncio.create_task(self._manager.broadcast(message))
        else:
            # If loop not running, run briefly to send
            loop.run_until_complete(self._manager.broadcast(message))
    
    # === Worker Events ===
    
    def broadcast_worker_event(
        self,
        event_type: str,
        worker_id: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Broadcast worker-related event.
        
        Event types:
        - registered: Worker joined fleet
        - heartbeat: Worker heartbeat received
        - state_changed: Worker state transition
        - drain: Worker set to draining
        - resume: Worker resumed to active
        - offline: Worker went offline
        - unhealthy: Worker marked unhealthy
        
        Args:
            event_type: Worker event type
            worker_id: Worker identifier
            data: Additional event data
        """
        message = {
            "event_type": f"worker.{event_type}",
            "timestamp": datetime.now().timestamp(),
            "data": {
                "worker_id": worker_id,
                **(data or {}),
            }
        }
        self._publish(message)
    
    def broadcast_worker_registered(
        self,
        worker_id: str,
        label: str,
        role: str,
        capabilities: list,
        hostname: Optional[str] = None,
        pid: Optional[int] = None,
    ) -> None:
        """Broadcast worker registration event."""
        self.broadcast_worker_event(
            "registered",
            worker_id,
            {
                "label": label,
                "role": role,
                "capabilities": capabilities,
                "hostname": hostname,
                "pid": pid,
                "state": "idle",
            }
        )
    
    def broadcast_worker_heartbeat(
        self,
        worker_id: str,
        state: str,
        lease_count: int,
        current_run_id: Optional[str] = None,
        current_lease_id: Optional[str] = None,
    ) -> None:
        """Broadcast worker heartbeat event."""
        self.broadcast_worker_event(
            "heartbeat",
            worker_id,
            {
                "state": state,
                "lease_count": lease_count,
                "current_run_id": current_run_id,
                "current_lease_id": current_lease_id,
            }
        )
    
    def broadcast_worker_state_changed(
        self,
        worker_id: str,
        old_state: str,
        new_state: str,
        current_run_id: Optional[str] = None,
        current_lease_id: Optional[str] = None,
    ) -> None:
        """Broadcast worker state transition event."""
        self.broadcast_worker_event(
            "state_changed",
            worker_id,
            {
                "old_state": old_state,
                "new_state": new_state,
                "current_run_id": current_run_id,
                "current_lease_id": current_lease_id,
            }
        )
    
    def broadcast_worker_drain(
        self,
        worker_id: str,
        reason: Optional[str] = None,
        initiator: Optional[str] = None,
    ) -> None:
        """Broadcast worker drain event."""
        self.broadcast_worker_event(
            "drain",
            worker_id,
            {
                "reason": reason,
                "initiator": initiator,
                "drain_state": "draining",
            }
        )
    
    def broadcast_worker_resume(
        self,
        worker_id: str,
    ) -> None:
        """Broadcast worker resume event."""
        self.broadcast_worker_event(
            "resume",
            worker_id,
            {
                "drain_state": "active",
            }
        )
    
    def broadcast_worker_offline(
        self,
        worker_id: str,
        last_heartbeat_at: str,
    ) -> None:
        """Broadcast worker offline event."""
        self.broadcast_worker_event(
            "offline",
            worker_id,
            {
                "last_heartbeat_at": last_heartbeat_at,
            }
        )
    
    def broadcast_worker_unhealthy(
        self,
        worker_id: str,
        error: Optional[str] = None,
    ) -> None:
        """Broadcast worker unhealthy event."""
        self.broadcast_worker_event(
            "unhealthy",
            worker_id,
            {
                "error": error,
            }
        )
    
    # === Lease Events ===
    
    def broadcast_lease_event(
        self,
        event_type: str,
        lease_id: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Broadcast lease-related event.
        
        Event types:
        - acquired: Lease acquired by worker
        - heartbeat: Lease heartbeat received
        - released: Lease released
        - completed: Lease completed successfully
        - failed: Lease failed
        - expired: Lease expired before completion
        
        Args:
            event_type: Lease event type
            lease_id: Lease identifier
            data: Additional event data
        """
        message = {
            "event_type": f"lease.{event_type}",
            "timestamp": datetime.now().timestamp(),
            "data": {
                "lease_id": lease_id,
                **(data or {}),
            }
        }
        self._publish(message)
    
    def broadcast_lease_acquired(
        self,
        lease_id: str,
        worker_id: str,
        task_node_id: str,
        run_id: str,
        attempt_id: str,
    ) -> None:
        """Broadcast lease acquisition event."""
        self.broadcast_lease_event(
            "acquired",
            lease_id,
            {
                "worker_id": worker_id,
                "task_node_id": task_node_id,
                "run_id": run_id,
                "attempt_id": attempt_id,
            }
        )
    
    def broadcast_lease_heartbeat(
        self,
        lease_id: str,
        worker_id: str,
        status: str,
    ) -> None:
        """Broadcast lease heartbeat event."""
        self.broadcast_lease_event(
            "heartbeat",
            lease_id,
            {
                "worker_id": worker_id,
                "status": status,
            }
        )
    
    def broadcast_lease_released(
        self,
        lease_id: str,
        worker_id: str,
        reason: Optional[str] = None,
    ) -> None:
        """Broadcast lease release event."""
        self.broadcast_lease_event(
            "released",
            lease_id,
            {
                "worker_id": worker_id,
                "reason": reason,
            }
        )
    
    def broadcast_lease_completed(
        self,
        lease_id: str,
        worker_id: str,
        task_node_id: str,
        summary: Optional[str] = None,
    ) -> None:
        """Broadcast lease completion event."""
        self.broadcast_lease_event(
            "completed",
            lease_id,
            {
                "worker_id": worker_id,
                "task_node_id": task_node_id,
                "summary": summary,
                "success": True,
            }
        )
    
    def broadcast_lease_failed(
        self,
        lease_id: str,
        worker_id: str,
        task_node_id: str,
        error: Optional[str] = None,
    ) -> None:
        """Broadcast lease failure event."""
        self.broadcast_lease_event(
            "failed",
            lease_id,
            {
                "worker_id": worker_id,
                "task_node_id": task_node_id,
                "error": error,
                "success": False,
            }
        )
    
    def broadcast_lease_expired(
        self,
        lease_id: str,
        worker_id: str,
        task_node_id: str,
    ) -> None:
        """Broadcast lease expiration event."""
        self.broadcast_lease_event(
            "expired",
            lease_id,
            {
                "worker_id": worker_id,
                "task_node_id": task_node_id,
            }
        )
    
    # === Queue Events ===
    
    def broadcast_queue_event(
        self,
        event_type: str,
        shard: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Broadcast queue-related event.
        
        Event types:
        - dispatched: Task dispatched from queue
        - enqueued: Task added to queue
        - dequeued: Task removed from queue
        - depth_changed: Queue depth changed
        - shard_status: Shard status update
        
        Args:
            event_type: Queue event type
            shard: Queue shard identifier
            data: Additional event data
        """
        message = {
            "event_type": f"queue.{event_type}",
            "timestamp": datetime.now().timestamp(),
            "data": {
                "shard": shard,
                **(data or {}),
            }
        }
        self._publish(message)
    
    def broadcast_queue_dispatched(
        self,
        shard: str,
        task_node_id: str,
        worker_id: str,
        lease_id: str,
        run_id: str,
    ) -> None:
        """Broadcast task dispatch from queue."""
        self.broadcast_queue_event(
            "dispatched",
            shard,
            {
                "task_node_id": task_node_id,
                "worker_id": worker_id,
                "lease_id": lease_id,
                "run_id": run_id,
            }
        )
    
    def broadcast_queue_enqueued(
        self,
        shard: str,
        task_node_id: str,
        run_id: str,
    ) -> None:
        """Broadcast task added to queue."""
        self.broadcast_queue_event(
            "enqueued",
            shard,
            {
                "task_node_id": task_node_id,
                "run_id": run_id,
            }
        )
    
    def broadcast_queue_depth_changed(
        self,
        shard: str,
        old_depth: int,
        new_depth: int,
    ) -> None:
        """Broadcast queue depth change."""
        self.broadcast_queue_event(
            "depth_changed",
            shard,
            {
                "old_depth": old_depth,
                "new_depth": new_depth,
            }
        )
    
    def broadcast_queue_snapshot(
        self,
        queues_data: list,
    ) -> None:
        """Broadcast queue status snapshot."""
        message = {
            "event_type": "queue.snapshot",
            "timestamp": datetime.now().timestamp(),
            "data": queues_data,
        }
        self._publish(message)
    
    # === Health Events ===
    
    def broadcast_health_event(
        self,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Broadcast health-related event.
        
        Event types:
        - status: Periodic health status
        - postgres_ready: Postgres connection status
        - redis_ready: Redis connection status
        - fleet_summary: Fleet-level health summary
        
        Args:
            event_type: Health event type
            data: Health data dict
        """
        message = {
            "event_type": f"health.{event_type}",
            "timestamp": datetime.now().timestamp(),
            "data": data or {},
        }
        self._publish(message)
    
    def broadcast_health_status(
        self,
        postgres_ready: bool,
        redis_ready: bool,
        worker_count: int,
        active_lease_count: int,
        queue_depth: int,
        draining_workers: list,
        offline_workers: list,
        unhealthy_workers: list,
        stuck_runs: list,
        **kwargs,
    ) -> None:
        """Broadcast periodic health status.
        
        Called every 60 seconds by periodic health broadcast.
        """
        self.broadcast_health_event(
            "status",
            {
                "postgres_ready": postgres_ready,
                "redis_ready": redis_ready,
                "worker_count": worker_count,
                "active_lease_count": active_lease_count,
                "queue_depth": queue_depth,
                "draining_workers": draining_workers,
                "offline_workers": offline_workers,
                "unhealthy_workers": unhealthy_workers,
                "stuck_runs": stuck_runs,
                **kwargs,
            }
        )
    
    def broadcast_fleet_summary(
        self,
        worker_count_by_state: dict,
        workers_by_role: dict,
        queue_depth_by_shard: dict,
        lease_reclaim_rate: float,
        stuck_run_count: int,
    ) -> None:
        """Broadcast fleet-level health summary."""
        self.broadcast_health_event(
            "fleet_summary",
            {
                "worker_count_by_state": worker_count_by_state,
                "workers_by_role": workers_by_role,
                "queue_depth_by_shard": queue_depth_by_shard,
                "lease_reclaim_rate": lease_reclaim_rate,
                "stuck_run_count": stuck_run_count,
            }
        )
    
    # === System Events ===
    
    def broadcast_system_event(
        self,
        event_name: str,
        message: str,
        level: str = "info",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Broadcast generic system event.
        
        Args:
            event_name: Event name
            message: Human-readable message
            level: Event level (info, warning, error)
            details: Additional details
        """
        msg = {
            "event_type": "system_event",
            "timestamp": datetime.now().timestamp(),
            "data": {
                "event_name": event_name,
                "message": message,
                "level": level,
                "details": details or {},
            }
        }
        self._publish(msg)


# === Global Publisher Instance ===
# Lazy-initialized to avoid circular imports

_publisher: Optional[WebSocketEventPublisher] = None


def get_publisher() -> WebSocketEventPublisher:
    """Get global WebSocket event publisher.
    
    Returns:
        WebSocketEventPublisher instance
    """
    global _publisher  # Fix: declare as global to avoid UnboundLocalError
    if _publisher is None:
        from .websocket import manager
        _publisher = WebSocketEventPublisher(manager)
    return _publisher


def set_publisher(publisher: WebSocketEventPublisher) -> None:
    """Set global publisher (for testing or custom config)."""
    global _publisher
    _publisher = publisher


def init_publisher(manager: ConnectionManager) -> WebSocketEventPublisher:
    """Initialize publisher with connection manager.
    
    Args:
        manager: ConnectionManager instance
        
    Returns:
        Initialized WebSocketEventPublisher
    """
    global _publisher
    _publisher = WebSocketEventPublisher(manager)
    return _publisher