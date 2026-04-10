from __future__ import annotations

import asyncio
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from ..artifact_store import LocalFilesystemArtifactStore
from ..boundary.gateway import ToolGateway
from ..boundary.sandbox import SandboxManager
from ..runtime.models import ModelRegistry
from ..settings import HarnessLabSettings
from ..types import (
    ActionPlan,
    ArtifactRef,
    DispatchEnvelope,
    KnowledgeReindexRequest,
    KnowledgeSearchRequest,
    LeaseCompletionRequest,
    LeaseFailureRequest,
    LeaseReleaseRequest,
    ModelProfile,
    RecoveryEvent,
    ToolCallRecord,
    ToolExecutionResult,
    WorkerEventBatch,
    WorkerEventRecord,
    WorkerHeartbeatRequest,
    WorkerLease,
    WorkerPollRequest,
    WorkerPollResponse,
    WorkerRegisterRequest,
    WorkerSnapshot,
)
from ..utils import compact_text, new_id, utc_now


JsonTransport = Callable[[str, str, Optional[Dict[str, Any]]], Dict[str, Any]]


class _NoopConstraints:
    def verify(self, subject: str, payload: Dict[str, Any], constraint_set_id: str):  # noqa: ARG002
        return []


class LocalArtifactStore:
    def __init__(self, repo_root: Path, artifact_root: Optional[Path] = None) -> None:
        self.repo_root = repo_root
        resolved_root = artifact_root or (repo_root / "backend" / "data" / "harness_lab" / "artifacts")
        self.artifact_root = resolved_root
        self.backend = LocalFilesystemArtifactStore(resolved_root)
        self._created: list[ArtifactRef] = []

    def write_artifact_text(
        self,
        run_id: str,
        artifact_type: str,
        filename: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ArtifactRef:
        artifact = self.backend.write_text(run_id, artifact_type, filename, content, metadata)
        self._created.append(artifact)
        return artifact

    def drain_artifacts(self) -> list[ArtifactRef]:
        artifacts = list(self._created)
        self._created.clear()
        return artifacts


class WorkerRuntimeClient:
    def __init__(
        self,
        control_plane_url: str,
        request_fn: Optional[JsonTransport] = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.control_plane_url = control_plane_url.rstrip("/")
        self.request_fn = request_fn
        self.timeout_seconds = timeout_seconds

    def register_worker(self, request: WorkerRegisterRequest) -> WorkerSnapshot:
        payload = self._request("POST", "/api/workers/register", request.model_dump(exclude_none=True))
        return WorkerSnapshot(**payload["data"])

    def get_worker(self, worker_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/workers/{worker_id}")

    def drain_worker(self, worker_id: str, reason: Optional[str] = None) -> WorkerSnapshot:
        payload = self._request("POST", f"/api/workers/{worker_id}/drain", {"reason": reason} if reason else {})
        return WorkerSnapshot(**payload["data"])

    def resume_worker(self, worker_id: str) -> WorkerSnapshot:
        payload = self._request("POST", f"/api/workers/{worker_id}/resume", {})
        return WorkerSnapshot(**payload["data"])

    def heartbeat_worker(self, worker_id: str, request: WorkerHeartbeatRequest) -> WorkerSnapshot:
        payload = self._request("POST", f"/api/workers/{worker_id}/heartbeat", request.model_dump(exclude_none=True))
        return WorkerSnapshot(**payload["data"])

    def poll_worker(self, worker_id: str, request: WorkerPollRequest) -> WorkerPollResponse:
        payload = self._request("POST", f"/api/workers/{worker_id}/poll", request.model_dump(exclude_none=True))
        return WorkerPollResponse(**payload["data"])

    def heartbeat_lease(self, lease_id: str, request: WorkerHeartbeatRequest) -> WorkerLease:
        payload = self._request("POST", f"/api/leases/{lease_id}/heartbeat", request.model_dump(exclude_none=True))
        return WorkerLease(**payload["data"])

    def submit_worker_events(self, lease_id: str, batch: WorkerEventBatch) -> WorkerLease:
        payload = self._request("POST", f"/api/leases/{lease_id}/events", batch.model_dump(exclude_none=True))
        return WorkerLease(**payload["data"])

    def complete_lease(self, lease_id: str, request: LeaseCompletionRequest) -> Dict[str, Any]:
        return self._request("POST", f"/api/leases/{lease_id}/complete", request.model_dump(exclude_none=True))

    def fail_lease(self, lease_id: str, request: LeaseFailureRequest) -> Dict[str, Any]:
        return self._request("POST", f"/api/leases/{lease_id}/fail", request.model_dump(exclude_none=True))

    def release_lease(self, lease_id: str, request: LeaseReleaseRequest) -> Dict[str, Any]:
        return self._request("POST", f"/api/leases/{lease_id}/release", request.model_dump(exclude_none=True))

    def get_run(self, run_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/runs/{run_id}")

    def search_knowledge(self, request: KnowledgeSearchRequest) -> Dict[str, Any]:
        return self._request("POST", "/api/knowledge/search", request.model_dump(exclude_none=True))

    def fleet_status(self) -> Dict[str, Any]:
        return self._request("GET", "/api/fleet/status")

    def queue_status(self) -> Dict[str, Any]:
        return self._request("GET", "/api/queues")

    def reindex_knowledge(self, request: KnowledgeReindexRequest) -> Dict[str, Any]:
        return self._request("POST", "/api/knowledge/reindex", request.model_dump(exclude_none=True))

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.request_fn:
            return self.request_fn(method, path, payload)
        body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url=f"{self.control_plane_url}{path}",
            method=method,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path} failed: {detail or exc.reason}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc


class WorkerExecutionLoop:
    def __init__(
        self,
        client: WorkerRuntimeClient,
        poll_interval_seconds: float = 1.0,
        repo_root: Optional[Path] = None,
        artifact_root: Optional[Path] = None,
        model_registry: Optional[ModelRegistry] = None,
    ) -> None:
        self.client = client
        self.poll_interval_seconds = poll_interval_seconds
        self.repo_root = repo_root or Path(__file__).resolve().parents[4]
        resolved_artifact_root = artifact_root
        if resolved_artifact_root is None:
            env_artifact_root = os.getenv("HARNESS_ARTIFACT_ROOT")
            if env_artifact_root:
                resolved_artifact_root = Path(env_artifact_root)
        self.artifact_store = LocalArtifactStore(self.repo_root, resolved_artifact_root)
        self.settings = HarnessLabSettings.from_env()
        self.sandbox_manager = SandboxManager(self.settings, self.artifact_store)
        self.tool_gateway = ToolGateway(self.artifact_store, _NoopConstraints(), sandbox_manager=self.sandbox_manager)
        self.model_registry = model_registry or ModelRegistry()

    def register(
        self,
        worker_id: Optional[str] = None,
        label: str = "cli-worker",
        capabilities: Optional[list[str]] = None,
        role_profile: Optional[str] = None,
        labels: Optional[list[str]] = None,
        version: str = "v1",
        execution_mode: str = "remote_http",
    ) -> WorkerSnapshot:
        sandbox_status = self.sandbox_manager.status()
        resolved_capabilities = capabilities or []
        return self.client.register_worker(
            WorkerRegisterRequest(
                worker_id=worker_id,
                label=label,
                capabilities=resolved_capabilities,
                role_profile=role_profile,
                hostname=socket.gethostname(),
                pid=os.getpid(),
                labels=labels or ["cli", "remote-worker"],
                execution_mode=execution_mode,
                sandbox_backend=sandbox_status.sandbox_backend,
                sandbox_ready=sandbox_status.docker_ready and sandbox_status.sandbox_image_ready,
                version=version,
            )
        )

    def serve(
        self,
        worker_id: Optional[str] = None,
        label: str = "cli-worker",
        capabilities: Optional[list[str]] = None,
        role_profile: Optional[str] = None,
        labels: Optional[list[str]] = None,
        interval_seconds: Optional[float] = None,
        once: bool = False,
        max_tasks: int = 1,
        max_dispatches: Optional[int] = None,
        max_idle_cycles: int = 3,
    ) -> Dict[str, Any]:
        interval = interval_seconds or self.poll_interval_seconds
        try:
            worker = self.client.get_worker(worker_id)["data"] if worker_id else None
        except Exception:  # noqa: BLE001
            worker = None
        if worker is None:
            worker_snapshot = self.register(
                worker_id=worker_id,
                label=label,
                capabilities=capabilities,
                role_profile=role_profile,
                labels=labels,
                execution_mode="remote_http",
            )
            worker_id = worker_snapshot.worker_id
            worker = worker_snapshot.model_dump()
        else:
            worker_id = worker["worker_id"]
        effective_role_profile = role_profile or worker.get("role_profile")

        handled = 0
        idle_cycles = 0
        while True:
            sandbox_status = self.sandbox_manager.status()
            self.client.heartbeat_worker(
                worker_id,
                WorkerHeartbeatRequest(
                    state="idle",
                    lease_count=0,
                    current_lease_id=None,
                    role_profile=effective_role_profile,
                    sandbox_backend=sandbox_status.sandbox_backend,
                    sandbox_ready=sandbox_status.docker_ready and sandbox_status.sandbox_image_ready,
                ),
            )
            response = self.client.poll_worker(worker_id, WorkerPollRequest(max_tasks=max(1, max_tasks)))
            if not response.dispatches:
                idle_cycles += 1
                if once or (max_dispatches is not None and handled >= max_dispatches) or idle_cycles >= max_idle_cycles:
                    break
                time.sleep(max(0.1, interval))
                continue
            idle_cycles = 0
            for dispatch in response.dispatches:
                self._execute_dispatch(worker_id, dispatch, effective_role_profile)
                handled += 1
                if max_dispatches is not None and handled >= max_dispatches:
                    return {"worker_id": worker_id, "handled_dispatches": handled}
            if once:
                return {"worker_id": worker_id, "handled_dispatches": handled}
        return {"worker_id": worker_id, "handled_dispatches": handled}

    def _execute_dispatch(self, worker_id: str, dispatch: DispatchEnvelope, role_profile: Optional[str]) -> None:
        self.client.heartbeat_worker(
            worker_id,
            WorkerHeartbeatRequest(
                state="leased",
                lease_count=1,
                current_run_id=dispatch.run_id,
                current_task_node_id=dispatch.task_node_id,
                current_lease_id=dispatch.lease_id,
                role_profile=role_profile,
                sandbox_backend=self.settings.sandbox_backend,
                sandbox_ready=not dispatch.requires_sandbox or (
                    self.sandbox_manager.status().docker_ready and self.sandbox_manager.status().sandbox_image_ready
                ),
            ),
        )
        stop_heartbeat = threading.Event()
        final_state = "idle"
        final_error: Optional[str] = None
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(dispatch, stop_heartbeat, role_profile),
            daemon=True,
        )
        heartbeat_thread.start()
        try:
            if dispatch.task_node.kind != "execution":
                batch = WorkerEventBatch(
                    lease_id=dispatch.lease_id,
                    events=[
                        WorkerEventRecord(
                            event_type="worker.dispatch.completed",
                            payload={
                                "worker_id": worker_id,
                                "lease_id": dispatch.lease_id,
                                "task_node_id": dispatch.task_node_id,
                                "kind": dispatch.task_node.kind,
                            },
                        )
                    ],
                    emitted_at=utc_now(),
                )
                self.client.submit_worker_events(dispatch.lease_id, batch)
                self.client.complete_lease(
                    dispatch.lease_id,
                    LeaseCompletionRequest(summary=f"{dispatch.task_node.label} completed by remote worker."),
                )
                return

            batch, ok, error = self._execute_execution_node(dispatch, worker_id)
            if batch.events or batch.model_calls or batch.tool_calls or batch.artifacts or batch.recovery_events:
                self.client.submit_worker_events(dispatch.lease_id, batch)
            if ok:
                self.client.complete_lease(
                    dispatch.lease_id,
                    LeaseCompletionRequest(summary=f"{dispatch.task_node.label} finished on remote worker."),
                )
            else:
                self.client.fail_lease(
                    dispatch.lease_id,
                    LeaseFailureRequest(worker_event_batch=None, error=error or "Remote worker execution failed."),
                )
        except Exception as exc:  # noqa: BLE001
            message = compact_text(str(exc), 240)
            final_state = "unhealthy"
            final_error = message
            batch = WorkerEventBatch(
                lease_id=dispatch.lease_id,
                events=[
                    WorkerEventRecord(
                        event_type="worker.execution.error",
                        payload={
                            "worker_id": worker_id,
                            "lease_id": dispatch.lease_id,
                            "task_node_id": dispatch.task_node_id,
                            "error": message,
                        },
                    )
                ],
                artifacts=self.artifact_store.drain_artifacts(),
                recovery_events=[
                    RecoveryEvent(
                        recovery_id=new_id("recovery"),
                        kind="worker_exception",
                        summary=message,
                        created_at=utc_now(),
                    )
                ],
                emitted_at=utc_now(),
            )
            try:
                self.client.submit_worker_events(dispatch.lease_id, batch)
                self.client.fail_lease(
                    dispatch.lease_id,
                    LeaseFailureRequest(worker_event_batch=None, error=message),
                )
            except Exception:  # noqa: BLE001
                self.client.release_lease(dispatch.lease_id, LeaseReleaseRequest(reason=message))
        finally:
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=0.5)
            self.client.heartbeat_worker(
                worker_id,
                WorkerHeartbeatRequest(
                    state=final_state,
                    lease_count=0,
                    current_lease_id=None,
                    role_profile=None,
                    sandbox_backend=self.settings.sandbox_backend,
                    sandbox_ready=self.sandbox_manager.status().docker_ready and self.sandbox_manager.status().sandbox_image_ready,
                    last_error=final_error,
                ),
            )

    def _execute_execution_node(
        self,
        dispatch: DispatchEnvelope,
        worker_id: str,
    ) -> tuple[WorkerEventBatch, bool, Optional[str]]:
        action = dispatch.intent.suggested_action
        model_calls = []
        if action.tool_name == "model_reflection":
            profile = self._worker_model_profile(dispatch)
            reflection, model_call = self.model_registry.reflect_with_trace(
                prompt=str(action.payload.get("prompt", dispatch.intent.intent)),
                profile=profile,
                extra={"run_id": dispatch.run_id, "lease_id": dispatch.lease_id, "worker_id": worker_id},
            )
            result = self.tool_gateway.model_reflection_result(reflection)
            model_calls.append(model_call)
            events = [
                WorkerEventRecord(
                    event_type="model.reflection_called",
                    payload={**model_call.model_dump(), "worker_id": worker_id, "lease_id": dispatch.lease_id},
                )
            ]
        else:
            if action.tool_name == "knowledge_search":
                result = self._execute_remote_knowledge_search(dispatch)
            else:
                contextual_action = self._contextual_action(dispatch, action)
                result = asyncio.run(self.tool_gateway.execute(dispatch.run_id, contextual_action))
            events = []

        tool_call = ToolCallRecord(
            tool_name=action.tool_name,
            payload=action.payload,
            ok=result.ok,
            output=result.output,
            error=result.error,
            created_at=utc_now(),
        )
        events.append(
            WorkerEventRecord(
                event_type="tool.executed",
                payload={
                    "worker_id": worker_id,
                    "lease_id": dispatch.lease_id,
                    "tool_name": tool_call.tool_name,
                    "ok": tool_call.ok,
                    "error": tool_call.error,
                    "sandboxed": isinstance(tool_call.output.get("sandbox_trace"), dict),
                },
            )
        )
        sandbox_trace = tool_call.output.get("sandbox_trace")
        if isinstance(sandbox_trace, dict):
            events.append(
                WorkerEventRecord(
                    event_type="sandbox.executed" if tool_call.ok else "sandbox.failed",
                    payload={
                        "worker_id": worker_id,
                        "lease_id": dispatch.lease_id,
                        "task_node_id": dispatch.task_node_id,
                        "tool_name": tool_call.tool_name,
                        "changed_paths": tool_call.output.get("changed_paths", []),
                        "sandbox_trace": sandbox_trace,
                    },
                )
            )
        artifacts = self.artifact_store.drain_artifacts()
        batch = WorkerEventBatch(
            lease_id=dispatch.lease_id,
            events=events,
            model_calls=model_calls,
            tool_calls=[tool_call],
            artifacts=artifacts,
            recovery_events=(
                [
                    RecoveryEvent(
                        recovery_id=new_id("recovery"),
                        kind="tool_failure",
                        summary=result.error or "Remote worker execution failed.",
                        created_at=utc_now(),
                    )
                ]
                if not result.ok
                else []
            ),
            emitted_at=utc_now(),
        )
        return batch, result.ok, result.error

    def _execute_remote_knowledge_search(self, dispatch: DispatchEnvelope) -> ToolExecutionResult:
        payload = dispatch.intent.suggested_action.payload
        response = self.client.search_knowledge(
            KnowledgeSearchRequest(
                query=str(payload.get("query", dispatch.intent.intent)),
                top_k=max(1, int(payload.get("top_k", 8) or 8)),
                path_hint=str(payload.get("path_hint", "") or "") or None,
                source_types=payload.get("source_types") or [],
            )
        )
        result = response["data"]
        artifact = self.artifact_store.write_artifact_text(
            run_id=dispatch.run_id,
            artifact_type="knowledge_search_results",
            filename="execution_search_results.json",
            content=json.dumps(result, ensure_ascii=False, indent=2),
            metadata={
                "query": result.get("query"),
                "used_fallback": result.get("used_fallback"),
                "source_coverage": result.get("source_coverage", {}),
            },
        )
        result["artifact_id"] = artifact.artifact_id
        result["results"] = result.get("hits", [])
        return ToolExecutionResult(ok=True, output=result)

    def _heartbeat_loop(
        self,
        dispatch: DispatchEnvelope,
        stop_signal: threading.Event,
        role_profile: Optional[str],
    ) -> None:
        interval_seconds = max(1, dispatch.heartbeat_interval_seconds)
        while not stop_signal.wait(interval_seconds):
            try:
                sandbox_status = self.sandbox_manager.status()
                self.client.heartbeat_lease(
                    dispatch.lease_id,
                    WorkerHeartbeatRequest(
                        state="executing",
                        lease_count=1,
                        current_run_id=dispatch.run_id,
                        current_task_node_id=dispatch.task_node_id,
                        current_lease_id=dispatch.lease_id,
                        role_profile=role_profile,
                        sandbox_backend=sandbox_status.sandbox_backend,
                        sandbox_ready=sandbox_status.docker_ready and sandbox_status.sandbox_image_ready,
                    ),
                )
            except Exception:  # noqa: BLE001
                return

    @staticmethod
    def _contextual_action(dispatch: DispatchEnvelope, action: ActionPlan) -> ActionPlan:
        payload = dict(action.payload)
        if dispatch.approval_token:
            payload.setdefault("_approval_token", dispatch.approval_token)
        if dispatch.sandbox_spec is not None:
            payload.setdefault("_sandbox_spec", dispatch.sandbox_spec.model_dump())
        return action.model_copy(update={"payload": payload})

    @staticmethod
    def _worker_model_profile(dispatch: DispatchEnvelope) -> ModelProfile:
        provider = (os.getenv("HARNESS_LAB_MODEL_PROVIDER", "deepseek") or "deepseek").strip()
        model_name = (os.getenv("HARNESS_LAB_MODEL_NAME", "deepseek-chat") or "deepseek-chat").strip()
        now = utc_now()
        return ModelProfile(
            model_profile_id=dispatch.intent.model_profile_id,
            name="Remote Worker Reflection Profile",
            provider=provider,
            profile="balanced",
            status="published",
            config={"mode": "chat", "model_name": model_name},
            created_at=now,
            updated_at=now,
        )
