"""Dispatcher - task dispatch logic evolved from dispatch_queue.py.

This module consolidates:
    1. dispatch_queue.py: Redis-backed ready queue and lease expiry index
    2. LeaseManager dispatch logic: worker-to-task matching

Design:
    - Dispatcher owns the queue and matching algorithm
    - WorkerRegistry provides worker state
    - LeaseManager (future) will own lease lifecycle
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from .worker_registry import WorkerRegistry


class Dispatcher:
    """Dispatches ready tasks to available workers.
    
    Combines queue management from dispatch_queue.py with
    matching logic from LeaseManager (execution_plane.py).
    """
    
    def __init__(self, queue, worker_registry: WorkerRegistry) -> None:
        """Initialize dispatcher.
        
        Args:
            queue: DispatchQueue or InMemoryDispatchQueue instance
            worker_registry: WorkerRegistry for worker state
        """
        self.queue = queue
        self.worker_registry = worker_registry
    
    def get_queue_depth(self, shard: Optional[str] = None) -> int:
        """Get depth of ready queue."""
        return self.queue.ready_queue_depth(shard)
    
    def get_queue_depth_by_shard(self) -> Dict[str, int]:
        """Get queue depth per shard."""
        return self.queue.queue_depth_by_shard()
    
    def inspect_queues(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Inspect queue contents."""
        return self.queue.inspect_queues(limit)
    
    # TODO: Migrate matching logic from LeaseManager.next_dispatch_for_worker()
    # This requires interface definition for:
    # - Getting run/session/node for task
    # - Checking worker constraints match
    # - Creating dispatch envelope


class InMemoryDispatcher:
    """In-memory dispatcher for testing without Redis."""
    
    def __init__(self, worker_registry: WorkerRegistry) -> None:
        from ..dispatch_queue import InMemoryDispatchQueue
        self.queue = InMemoryDispatchQueue()
        self.worker_registry = worker_registry
    
    def get_queue_depth(self, shard: Optional[str] = None) -> int:
        return self.queue.ready_queue_depth(shard)
