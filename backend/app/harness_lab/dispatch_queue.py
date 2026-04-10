from __future__ import annotations

import json
import threading
import time
from typing import Dict, List, Optional, Tuple

import redis


def _task_ref(run_id: str, task_node_id: str) -> str:
    return json.dumps({"run_id": run_id, "task_node_id": task_node_id}, ensure_ascii=False, sort_keys=True)


def _decode_task_ref(payload: str) -> Tuple[str, str]:
    data = json.loads(payload)
    return str(data["run_id"]), str(data["task_node_id"])


class DispatchQueue:
    """Redis-backed ready queue plus lease expiry index."""

    def __init__(self, redis_url: str, namespace: str = "harness_lab") -> None:
        self.redis_url = redis_url
        self.namespace = namespace
        self.client = redis.from_url(redis_url, decode_responses=True)
        self.ready_key = f"{namespace}:dispatch:ready"
        self.shards_key = f"{namespace}:dispatch:ready:shards"
        self.lease_key = f"{namespace}:dispatch:lease_expiry"

    def _ready_shard_key(self, shard: str) -> str:
        return f"{self.ready_key}:{shard}"

    def ping(self) -> None:
        self.client.ping()

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:  # noqa: BLE001
            return None

    def reset(self) -> None:
        shard_keys = [self._ready_shard_key(shard) for shard in self.list_ready_shards()]
        keys = [self.ready_key, self.shards_key, self.lease_key, *shard_keys]
        self.client.delete(*keys)

    def enqueue_ready_task(
        self,
        run_id: str,
        task_node_id: str,
        score: Optional[float] = None,
        shard: str = "default",
    ) -> None:
        ready_key = self._ready_shard_key(shard)
        self.client.sadd(self.shards_key, shard)
        self.client.zadd(self.ready_key, {_task_ref(run_id, task_node_id): score or time.time()})
        self.client.zadd(ready_key, {_task_ref(run_id, task_node_id): score or time.time()})

    def requeue_ready_task(
        self,
        run_id: str,
        task_node_id: str,
        delay_seconds: float = 0.25,
        shard: str = "default",
    ) -> None:
        self.enqueue_ready_task(run_id, task_node_id, score=time.time() + delay_seconds, shard=shard)

    def pop_ready_task(self, shards: Optional[List[str]] = None) -> Optional[Tuple[str, str, str]]:
        candidate_shards = shards or self.list_ready_shards()
        if not candidate_shards:
            return None
        best: Optional[Tuple[str, str, float]] = None
        for shard in candidate_shards:
            items = self.client.zrange(self._ready_shard_key(shard), 0, 0, withscores=True)
            if not items:
                continue
            member, score = items[0]
            if best is None or score < best[2]:
                best = (shard, member, float(score))
        if best is None:
            return None
        shard, member, _score = best
        removed = self.client.zrem(self._ready_shard_key(shard), member)
        self.client.zrem(self.ready_key, member)
        if not removed:
            return self.pop_ready_task(shards=shards)
        run_id, task_node_id = _decode_task_ref(member)
        return run_id, task_node_id, shard

    def ready_queue_depth(self, shard: Optional[str] = None) -> int:
        if shard:
            return int(self.client.zcard(self._ready_shard_key(shard)))
        return int(self.client.zcard(self.ready_key))

    def list_ready_shards(self) -> List[str]:
        return sorted(str(item) for item in self.client.smembers(self.shards_key))

    def queue_depth_by_shard(self) -> Dict[str, int]:
        return {shard: self.ready_queue_depth(shard) for shard in self.list_ready_shards()}

    def inspect_queues(self, limit: int = 5) -> List[Dict[str, object]]:
        result: List[Dict[str, object]] = []
        for shard in self.list_ready_shards():
            members = self.client.zrange(self._ready_shard_key(shard), 0, max(0, limit - 1))
            sample_tasks = [
                {"run_id": run_id, "task_node_id": task_node_id}
                for run_id, task_node_id in (_decode_task_ref(member) for member in members)
            ]
            result.append({"shard": shard, "depth": self.ready_queue_depth(shard), "sample_tasks": sample_tasks})
        return result

    def track_lease_expiry(self, lease_id: str, expires_at_epoch: float) -> None:
        self.client.zadd(self.lease_key, {lease_id: expires_at_epoch})

    def clear_lease(self, lease_id: str) -> None:
        self.client.zrem(self.lease_key, lease_id)

    def pop_expired_leases(self, now_epoch: Optional[float] = None) -> List[str]:
        deadline = now_epoch or time.time()
        expired = self.client.zrangebyscore(self.lease_key, min="-inf", max=deadline)
        if expired:
            self.client.zremrangebyscore(self.lease_key, min="-inf", max=deadline)
        return [str(item) for item in expired]


class InMemoryDispatchQueue:
    """Minimal queue for local tests where Redis is intentionally absent."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready: Dict[str, float] = {}
        self._ready_shards: Dict[str, Dict[str, float]] = {}
        self._leases: Dict[str, float] = {}

    def ping(self) -> None:
        return None

    def close(self) -> None:
        return None

    def reset(self) -> None:
        with self._lock:
            self._ready.clear()
            self._ready_shards.clear()
            self._leases.clear()

    def enqueue_ready_task(
        self,
        run_id: str,
        task_node_id: str,
        score: Optional[float] = None,
        shard: str = "default",
    ) -> None:
        with self._lock:
            member = _task_ref(run_id, task_node_id)
            self._ready[member] = score or time.time()
            self._ready_shards.setdefault(shard, {})[member] = score or time.time()

    def requeue_ready_task(
        self,
        run_id: str,
        task_node_id: str,
        delay_seconds: float = 0.25,
        shard: str = "default",
    ) -> None:
        self.enqueue_ready_task(run_id, task_node_id, score=time.time() + delay_seconds, shard=shard)

    def pop_ready_task(self, shards: Optional[List[str]] = None) -> Optional[Tuple[str, str, str]]:
        with self._lock:
            candidate_shards = shards or sorted(self._ready_shards.keys())
            selected_shard: Optional[str] = None
            selected_member: Optional[str] = None
            selected_score: Optional[float] = None
            for shard in candidate_shards:
                ready = self._ready_shards.get(shard) or {}
                if not ready:
                    continue
                member = min(ready, key=ready.get)
                score = ready[member]
                if selected_score is None or score < selected_score:
                    selected_shard = shard
                    selected_member = member
                    selected_score = score
            if selected_member is None or selected_shard is None:
                return None
            self._ready.pop(selected_member, None)
            shard_ready = self._ready_shards.get(selected_shard, {})
            shard_ready.pop(selected_member, None)
        run_id, task_node_id = _decode_task_ref(selected_member)
        return run_id, task_node_id, selected_shard

    def ready_queue_depth(self, shard: Optional[str] = None) -> int:
        with self._lock:
            if shard:
                return len(self._ready_shards.get(shard, {}))
            return len(self._ready)

    def list_ready_shards(self) -> List[str]:
        with self._lock:
            return sorted(self._ready_shards.keys())

    def queue_depth_by_shard(self) -> Dict[str, int]:
        with self._lock:
            return {shard: len(items) for shard, items in sorted(self._ready_shards.items())}

    def inspect_queues(self, limit: int = 5) -> List[Dict[str, object]]:
        with self._lock:
            result: List[Dict[str, object]] = []
            for shard, ready in sorted(self._ready_shards.items()):
                members = sorted(ready.items(), key=lambda item: item[1])[:limit]
                sample_tasks = [
                    {"run_id": run_id, "task_node_id": task_node_id}
                    for run_id, task_node_id in (_decode_task_ref(member) for member, _ in members)
                ]
                result.append({"shard": shard, "depth": len(ready), "sample_tasks": sample_tasks})
            return result

    def track_lease_expiry(self, lease_id: str, expires_at_epoch: float) -> None:
        with self._lock:
            self._leases[lease_id] = expires_at_epoch

    def clear_lease(self, lease_id: str) -> None:
        with self._lock:
            self._leases.pop(lease_id, None)

    def pop_expired_leases(self, now_epoch: Optional[float] = None) -> List[str]:
        deadline = now_epoch or time.time()
        with self._lock:
            expired = [lease_id for lease_id, expires_at in self._leases.items() if expires_at <= deadline]
            for lease_id in expired:
                self._leases.pop(lease_id, None)
        return expired
