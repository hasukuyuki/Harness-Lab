from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app.harness_lab.bootstrap import (
    harness_lab_services,
    initialize_harness_lab_services,
    shutdown_harness_lab_services,
)
from backend.app.harness_lab.dispatch_queue import InMemoryDispatchQueue
from backend.app.harness_lab.runtime.models import normalize_base_url
from backend.app.harness_lab.settings import HarnessLabSettings
from backend.app.harness_lab.storage import SqliteTestPlatformStore
from backend.app.harness_lab.types import BenchmarkBucketResult, EvaluationFailure, EvaluationReport, ModelCallTrace
from backend.app.harness_lab.workers.runtime_client import WorkerExecutionLoop, WorkerRuntimeClient
from backend.app.main import app


@pytest.fixture()
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENAI_BASE_URL", "https://api.deepseek.com")
    monkeypatch.setenv("HARNESS_LAB_MODEL_PROVIDER", "deepseek")
    monkeypatch.setenv("HARNESS_LAB_MODEL_NAME", "deepseek-chat")
    settings = HarnessLabSettings(
        HARNESS_DB_URL="postgresql://unit:test@localhost:5432/harness_lab",
        HARNESS_REDIS_URL="redis://localhost:6379/0",
        HARNESS_WORKER_POLL_INTERVAL=1.0,
        HARNESS_REDIS_NAMESPACE="harness_lab_test",
        HARNESS_ARTIFACT_ROOT=str(tmp_path / "artifacts"),
    )
    database = SqliteTestPlatformStore(db_path=str(tmp_path / "harness_lab.db"), artifact_root=str(tmp_path / "artifacts"))
    queue = InMemoryDispatchQueue()
    initialize_harness_lab_services(settings=settings, database=database, dispatch_queue=queue, force=True)
    with TestClient(app) as test_client:
        yield test_client
    shutdown_harness_lab_services()


def _mock_provider(monkeypatch, intent_tool: str = "knowledge_search", reflection_summary: str = "Reflection finished through DeepSeek."):
    def fake_call(settings, messages):
        system_prompt = messages[0]["content"]
        if "intent declaration layer" in system_prompt:
            return (
                {
                    "task_type": intent_tool,
                    "intent": f"Use {intent_tool} before mutating anything.",
                    "confidence": 0.94,
                    "risk_mode": "low" if intent_tool != "shell" else "high",
                    "suggested_action": intent_tool,
                },
                ModelCallTrace(
                    provider=settings.provider,
                    model_name=settings.model_name,
                    latency_ms=12,
                    used_fallback=False,
                    failure_reason=None,
                ),
            )
        return (
            {
                "summary": reflection_summary,
                "research_notes": ["Stay read-first.", "Keep policy verdicts visible."],
                "details": {"path": "runtime"},
            },
            ModelCallTrace(
                provider=settings.provider,
                model_name=settings.model_name,
                latency_ms=15,
                used_fallback=False,
                failure_reason=None,
            ),
        )

    monkeypatch.setattr(harness_lab_services.model_registry, "_call_provider_json", fake_call)


def _client_transport(client: TestClient):
    def request(method: str, path: str, payload: dict | None = None):
        if method == "GET":
            response = client.get(path)
        elif method == "POST":
            response = client.post(path, json=payload or {})
        else:
            raise AssertionError(f"Unsupported test transport method: {method}")
        assert response.status_code < 400, response.text
        return response.json()

    return request


def test_settings_fail_fast_for_non_postgres(monkeypatch):
    monkeypatch.setenv("HARNESS_DB_URL", "sqlite:///backend/data/harness_lab/harness_lab.db")
    monkeypatch.setenv("HARNESS_REDIS_URL", "redis://localhost:6379/0")
    with pytest.raises(RuntimeError, match="Postgres URL"):
        HarnessLabSettings.from_env()


def test_provider_settings_health_and_catalog(client, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_BASE_URL", "https://api.deepseek.com")
    monkeypatch.setenv("HARNESS_LAB_MODEL_PROVIDER", "deepseek")
    monkeypatch.setenv("HARNESS_LAB_MODEL_NAME", "deepseek-chat")

    assert normalize_base_url("https://api.deepseek.com") == "https://api.deepseek.com/v1"
    assert normalize_base_url("https://api.deepseek.com/v1") == "https://api.deepseek.com/v1"

    health = client.get("/api/health")
    assert health.status_code == 200
    health_data = health.json()["data"]
    assert health_data["model_provider"] == "deepseek"
    assert health_data["model_ready"] is False
    assert health_data["fallback_mode"] is True
    assert health_data["base_url"] == "https://api.deepseek.com/v1"
    assert health_data["storage_backend"] == "sqlite_test"
    assert health_data["postgres_ready"] is True
    assert health_data["redis_ready"] is True
    assert "worker_count_by_state" in health_data
    assert "missions_running" in health_data
    assert "leases_by_status" in health_data
    assert "last_sweep_at" in health_data
    assert "offline_workers" in health_data
    assert "unhealthy_workers" in health_data
    assert "active_workers" in health_data
    assert "stuck_runs" in health_data
    assert "sandbox_backend" in health_data
    assert "docker_ready" in health_data
    assert "sandbox_image_ready" in health_data
    assert "sandbox_active_runs" in health_data
    assert "sandbox_failures" in health_data

    catalog = client.get("/api/settings/catalog")
    assert catalog.status_code == 200
    catalog_data = catalog.json()["data"]
    assert catalog_data["model_provider"]["default_model_name"] == "deepseek-chat"
    assert catalog_data["model_provider"]["fallback_mode"] is True
    assert catalog_data["execution_plane"]["storage_backend"] == "sqlite_test"
    assert "worker_count_by_state" in catalog_data["execution_plane"]
    assert "sandbox" in catalog_data
    assert "workflow_templates" in catalog_data
    assert "workers" in catalog_data


def test_model_backed_intent_and_run_trace(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session_response = client.post(
        "/api/sessions",
        json={
            "goal": "Search the runtime implementation before making changes.",
            "context": {"path": "backend/app/harness_lab/runtime"},
            "execution_mode": "single_worker",
        },
    )
    assert session_response.status_code == 200
    session_payload = session_response.json()["data"]
    assert session_payload["intent_declaration"]["task_type"] == "knowledge_search"
    assert session_payload["intent_declaration"]["suggested_action"]["tool_name"] == "knowledge_search"
    assert session_payload["intent_model_call"]["provider"] == "deepseek"
    assert session_payload["intent_model_call"]["used_fallback"] is False

    run_response = client.post("/api/runs", json={"session_id": session_payload["session_id"]})
    assert run_response.status_code == 200
    run_payload = run_response.json()["data"]
    assert run_payload["execution_trace"]["model_calls"][0]["provider"] == "deepseek"
    assert run_payload["execution_trace"]["model_calls"][0]["used_fallback"] is False
    assert run_payload["execution_trace"]["tool_calls"][0]["tool_name"] == "knowledge_search"


def test_invalid_model_payload_falls_back_without_leaking_secret(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "super-secret-test-key")

    def fake_invalid_call(settings, messages):
        return (
            {"task_type": "broken", "intent": "Missing required keys"},
            ModelCallTrace(
                provider=settings.provider,
                model_name=settings.model_name,
                latency_ms=9,
                used_fallback=False,
                failure_reason=None,
            ),
        )

    monkeypatch.setattr(harness_lab_services.model_registry, "_call_provider_json", fake_invalid_call)

    session_response = client.post(
        "/api/sessions",
        json={
            "goal": "Inspect the repository root safely and produce a Harness Lab trace.",
            "context": {"path": "."},
            "execution_mode": "single_worker",
        },
    )
    assert session_response.status_code == 200
    session_payload = session_response.json()["data"]
    assert session_payload["intent_declaration"]["suggested_action"]["tool_name"] == "filesystem"
    assert session_payload["intent_model_call"]["used_fallback"] is True
    assert "invalid intent payload" in session_payload["intent_model_call"]["failure_reason"].lower()
    assert "super-secret-test-key" not in json.dumps(session_payload, ensure_ascii=False)


def test_model_reflection_path_and_shell_approval_flow(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def fake_call(settings, messages):
        system_prompt = messages[0]["content"]
        if "intent declaration layer" in system_prompt:
            user_payload = messages[1]["content"]
            if "shell_command" in user_payload:
                return (
                    {
                        "task_type": "shell_command",
                        "intent": "Run the explicit shell command under approval control.",
                        "confidence": 0.96,
                        "risk_mode": "high",
                        "suggested_action": "shell",
                    },
                    ModelCallTrace(
                        provider=settings.provider,
                        model_name=settings.model_name,
                        latency_ms=11,
                        used_fallback=False,
                        failure_reason=None,
                    ),
                )
            return (
                {
                    "task_type": "synthesis",
                    "intent": "Reflect before taking any workspace action.",
                    "confidence": 0.83,
                    "risk_mode": "low",
                    "suggested_action": "model_reflection",
                },
                ModelCallTrace(
                    provider=settings.provider,
                    model_name=settings.model_name,
                    latency_ms=10,
                    used_fallback=False,
                    failure_reason=None,
                ),
            )
        return (
            {
                "summary": "DeepSeek reflection completed.",
                "research_notes": ["Compare harness traces.", "Prefer replayable outputs."],
                "details": {"source": "mock"},
            },
            ModelCallTrace(
                provider=settings.provider,
                model_name=settings.model_name,
                latency_ms=14,
                used_fallback=False,
                failure_reason=None,
            ),
        )

    monkeypatch.setattr(harness_lab_services.model_registry, "_call_provider_json", fake_call)

    reflection_session = client.post(
        "/api/sessions",
        json={
            "goal": "Summarize the harness architecture tradeoffs.",
            "context": {},
            "execution_mode": "single_worker",
        },
    ).json()["data"]
    reflection_run = client.post("/api/runs", json={"session_id": reflection_session["session_id"]})
    assert reflection_run.status_code == 200
    reflection_data = reflection_run.json()["data"]
    assert reflection_data["status"] == "completed"
    assert len(reflection_data["execution_trace"]["model_calls"]) >= 2
    assert reflection_data["execution_trace"]["tool_calls"][0]["output"]["summary"] == "DeepSeek reflection completed."

    shell_session = client.post(
        "/api/sessions",
        json={
            "goal": "Execute a reviewed shell command.",
            "context": {"shell_command": "mkdir -p backend/data/harness_lab/test_probe"},
            "execution_mode": "single_worker",
        },
    ).json()["data"]
    shell_run = client.post("/api/runs", json={"session_id": shell_session["session_id"]})
    assert shell_run.status_code == 200
    shell_run_data = shell_run.json()["data"]
    assert shell_run_data["status"] == "awaiting_approval"
    assert shell_run_data["execution_trace"]["model_calls"][0]["provider"] == "deepseek"
    assert shell_run_data["execution_trace"]["model_calls"][0]["used_fallback"] is False

    approvals = client.get("/api/approvals")
    assert approvals.status_code == 200
    assert any(item["run_id"] == shell_run_data["run_id"] for item in approvals.json()["data"])
    replay = client.get(f"/api/replays/{shell_run_data['run_id']}")
    assert replay.status_code == 200
    replay_body = json.dumps(replay.json()["data"], ensure_ascii=False)
    assert "test-key" not in replay_body


def test_policy_compare_and_experiment_registry(client):
    policies = client.get("/api/policies")
    assert policies.status_code == 200
    policy_ids = [item["policy_id"] for item in policies.json()["data"][:2]]
    assert len(policy_ids) == 2

    compare = client.post("/api/policies/compare", json={"policy_ids": policy_ids})
    assert compare.status_code == 200
    assert "diffs" in compare.json()["data"]


def test_reclaim_stale_lease_and_status_filter(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session = client.post(
        "/api/sessions",
        json={"goal": "Search the repository safely.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]
    worker = client.post("/api/workers", json={"label": "remote-worker", "capabilities": ["knowledge_search"], "version": "v1"}).json()["data"]
    dispatches = client.post(f"/api/workers/{worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"]
    assert len(dispatches) == 1
    dispatch = dispatches[0]

    lease = harness_lab_services.runtime.get_lease(dispatch["lease_id"])
    expired_at = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    lease.expires_at = expired_at
    harness_lab_services.database.upsert_lease(lease)
    harness_lab_services.dispatch_queue.track_lease_expiry(lease.lease_id, time.time() - 5)

    report = harness_lab_services.runtime.reclaim_stale_leases()
    assert report.reclaimed == 1

    expired_leases = client.get("/api/leases", params={"status": "expired"}).json()["data"]
    assert any(item["lease_id"] == lease.lease_id for item in expired_leases)

    redispatched = client.post(f"/api/workers/{worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"]
    assert len(redispatched) == 1
    assert redispatched[0]["task_node_id"] == dispatch["task_node_id"]


def test_sandbox_dispatch_fields_for_git_worker(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="git")

    session = client.post(
        "/api/sessions",
        json={"goal": "Inspect git state remotely.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]
    worker = client.post(
        "/api/workers",
        json={"label": "git-worker", "capabilities": ["git"], "version": "v1"},
    ).json()["data"]
    dispatch = client.post(f"/api/workers/{worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"][0]

    assert dispatch["requires_sandbox"] is True
    assert dispatch["sandbox_mode"] == "docker"
    assert dispatch["network_policy"] == "none"
    assert dispatch["sandbox_spec"]["image"]


def test_multi_agent_run_detail_exposes_mission_phase_handoffs_and_reviews(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Research the runtime before taking action.",
            "context": {"path": "backend/app/harness_lab/runtime"},
            "execution_mode": "single_worker",
        },
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]

    detail = client.get(f"/api/runs/{run['run_id']}").json()
    assert detail["mission_phase"]["phase"] == "mission_completion"
    assert detail["handoffs"]
    assert detail["review_verdicts"]
    assert detail["role_timeline"]
    roles = {packet["from_role"] for packet in detail["handoffs"]} | {packet["to_role"] for packet in detail["handoffs"]}
    assert {"planner", "researcher", "reviewer", "executor"}.issubset(roles)
    assert any(verdict["decision"] in {"accept", "complete"} for verdict in detail["review_verdicts"])


def test_role_profile_filters_remote_dispatch(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session = client.post(
        "/api/sessions",
        json={"goal": "Search remotely.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]

    wrong_worker = client.post(
        "/api/workers",
        json={"label": "executor-only", "capabilities": ["knowledge_search"], "role_profile": "executor", "version": "v1"},
    ).json()["data"]
    wrong_dispatches = client.post(f"/api/workers/{wrong_worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"]
    assert wrong_dispatches == []

    planner_worker = client.post(
        "/api/workers",
        json={"label": "planner-worker", "capabilities": [], "role_profile": "planner", "version": "v1"},
    ).json()["data"]
    planner_dispatches = client.post(f"/api/workers/{planner_worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"]
    assert len(planner_dispatches) == 1
    assert planner_dispatches[0]["agent_role"] == "planner"


def test_dispatch_constraints_route_by_required_labels_and_shards(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session_payload = client.post(
        "/api/sessions",
        json={"goal": "Search remotely with labeled planners.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    session = harness_lab_services.runtime.get_session(session_payload["session_id"])
    planner_node = next(node for node in session.task_graph.nodes if node.agent_role == "planner")
    planner_node.metadata["required_labels"] = ["planner-eu"]
    planner_node.metadata["preferred_labels"] = ["research"]
    harness_lab_services.runtime._persist_session(session)

    client.post("/api/runs", json={"session_id": session.session_id}).json()["data"]

    wrong_worker = client.post(
        "/api/workers",
        json={"label": "planner-us", "capabilities": [], "role_profile": "planner", "labels": ["planner-us"], "version": "v1"},
    ).json()["data"]
    assert client.post(f"/api/workers/{wrong_worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"] == []

    correct_worker = client.post(
        "/api/workers",
        json={
            "label": "planner-eu",
            "capabilities": [],
            "role_profile": "planner",
            "labels": ["planner-eu", "research"],
            "version": "v1",
        },
    ).json()["data"]
    dispatch = client.post(f"/api/workers/{correct_worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"][0]
    assert dispatch["required_labels"] == ["planner-eu"]
    assert dispatch["preferred_labels"] == ["research"]
    assert dispatch["queue_shard"].startswith("planner/")


def test_worker_drain_blocks_new_dispatch_and_resume_recovers(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session = client.post(
        "/api/sessions",
        json={"goal": "Search remotely.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]

    worker = client.post(
        "/api/workers",
        json={"label": "drainable-planner", "capabilities": [], "role_profile": "planner", "labels": ["planner"], "version": "v1"},
    ).json()["data"]
    drained = client.post(f"/api/workers/{worker['worker_id']}/drain", json={"reason": "maintenance window"}).json()["data"]
    assert drained["drain_state"] == "draining"

    no_dispatch = client.post(f"/api/workers/{worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"]
    assert no_dispatch == []

    resumed = client.post(f"/api/workers/{worker['worker_id']}/resume").json()["data"]
    assert resumed["drain_state"] == "active"
    dispatch = client.post(f"/api/workers/{worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"][0]
    assert dispatch["agent_role"] == "planner"


def test_fleet_and_queue_status_expose_shards_and_drain_state(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session_payload = client.post(
        "/api/sessions",
        json={"goal": "Inspect queue shards.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    session = harness_lab_services.runtime.get_session(session_payload["session_id"])
    planner_node = next(node for node in session.task_graph.nodes if node.agent_role == "planner")
    planner_node.metadata["required_labels"] = ["planner-eu"]
    harness_lab_services.runtime._persist_session(session)
    client.post("/api/runs", json={"session_id": session.session_id}).json()["data"]

    worker = client.post(
        "/api/workers",
        json={"label": "planner-eu", "capabilities": [], "role_profile": "planner", "labels": ["planner-eu"], "version": "v1"},
    ).json()["data"]
    client.post(f"/api/workers/{worker['worker_id']}/drain", json={"reason": "queue test"})

    fleet = client.get("/api/fleet/status").json()["data"]
    assert worker["worker_id"] in fleet["draining_workers"]
    assert "planner" in fleet["workers_by_role"]
    assert "late_callback_count" in fleet

    queues = client.get("/api/queues").json()["data"]
    assert queues
    assert any(item["shard"].startswith("planner/") for item in queues)


def test_run_status_summary_reports_dispatch_blockers(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session_payload = client.post(
        "/api/sessions",
        json={"goal": "Show dispatch blockers.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    session = harness_lab_services.runtime.get_session(session_payload["session_id"])
    planner_node = next(node for node in session.task_graph.nodes if node.agent_role == "planner")
    planner_node.metadata["required_labels"] = ["gpu"]
    harness_lab_services.runtime._persist_session(session)
    run = client.post("/api/runs", json={"session_id": session.session_id}).json()["data"]

    client.post(
        "/api/workers",
        json={"label": "planner", "capabilities": [], "role_profile": "planner", "labels": ["planner"], "version": "v1"},
    )

    detail = client.get(f"/api/runs/{run['run_id']}").json()
    assert detail["status_summary"]["kind"] == "dispatch_blocked"
    assert detail["coordination_snapshot"]["dispatch_blockers"]
    assert detail["coordination_snapshot"]["dispatch_blockers"][0]["kind"] == "awaiting_required_label"


def test_duplicate_lease_completion_is_idempotent(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="model_reflection", reflection_summary="Remote reflection completed.")

    session = client.post(
        "/api/sessions",
        json={"goal": "Reflect remotely.", "context": {}, "execution_mode": "remote_worker"},
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]
    worker = client.post("/api/workers", json={"label": "reflection-worker", "capabilities": ["model_reflection"], "version": "v1"}).json()["data"]
    dispatch = client.post(f"/api/workers/{worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"][0]

    first = client.post(f"/api/leases/{dispatch['lease_id']}/complete", json={"summary": "done"})
    assert first.status_code == 200
    second = client.post(f"/api/leases/{dispatch['lease_id']}/complete", json={"summary": "done-again"})
    assert second.status_code == 200

    detail = client.get(f"/api/runs/{run['run_id']}").json()
    completed_leases = [lease for lease in detail["leases"] if lease["lease_id"] == dispatch["lease_id"]]
    assert completed_leases[0]["status"] == "completed"
    attempts = [attempt for attempt in detail["attempts"] if attempt["lease_id"] == dispatch["lease_id"]]
    assert attempts[0]["status"] == "completed"
    assert any(event["event_type"] == "lease.complete_ignored" for event in detail["events"])
    assert detail["timeline_summary"]["leases_by_status"]["completed"] >= 1
    assert detail["coordination_snapshot"]["counts_by_status"]["completed"] >= 1


def test_rebuild_dispatch_state_restores_ready_queue(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session = client.post(
        "/api/sessions",
        json={"goal": "Search remotely.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    client.post("/api/runs", json={"session_id": session["session_id"]})

    assert harness_lab_services.runtime.execution_plane_status()["ready_queue_depth"] >= 1
    harness_lab_services.dispatch_queue.reset()
    assert harness_lab_services.runtime.execution_plane_status()["ready_queue_depth"] == 0
    harness_lab_services.runtime.rebuild_dispatch_state()
    assert harness_lab_services.runtime.execution_plane_status()["ready_queue_depth"] >= 1


def test_worker_detail_summary_and_heartbeat_events(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session = client.post(
        "/api/sessions",
        json={"goal": "Search remotely.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]
    worker = client.post(
        "/api/workers",
        json={"label": "summary-worker", "capabilities": ["knowledge_search"], "version": "v1"},
    ).json()["data"]
    dispatch = client.post(f"/api/workers/{worker['worker_id']}/poll", json={"max_tasks": 1}).json()["data"]["dispatches"][0]

    heartbeat = client.post(
        f"/api/leases/{dispatch['lease_id']}/heartbeat",
        json={"state": "executing", "lease_count": 1},
    )
    assert heartbeat.status_code == 200

    worker_detail = client.get(f"/api/workers/{worker['worker_id']}")
    assert worker_detail.status_code == 200
    worker_payload = worker_detail.json()
    assert worker_payload["health_summary"]["worker_id"] == worker["worker_id"]
    assert dispatch["lease_id"] in worker_payload["health_summary"]["recent_lease_ids"]
    assert any(event["event_type"] == "lease.heartbeat" for event in worker_payload["recent_events"])
    assert "sandbox" in worker_payload
    assert "role_profile" in worker_payload

    run_detail = client.get(f"/api/runs/{run['run_id']}")
    assert run_detail.status_code == 200
    run_payload = run_detail.json()
    assert run_payload["timeline_summary"]["entries"][0]["lease_id"] == dispatch["lease_id"]
    assert "mission_status" in run_payload["coordination_snapshot"]
    assert run_payload["status_summary"]["kind"] == "active_lease"
    assert "sandbox_summary" in run_payload


def test_recovery_workflow_records_request_repair_review_verdict(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    async def failing_execute(run_id, action):  # noqa: ARG001
        from backend.app.harness_lab.types import ToolExecutionResult
        return ToolExecutionResult(ok=False, error="synthetic execution failure")

    monkeypatch.setattr(harness_lab_services.tool_gateway, "execute", failing_execute)

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Trigger recovery review flow.",
            "context": {"path": "backend/app/harness_lab/runtime"},
            "workflow_template_id": "workflow_template_recovery_ring_v1",
            "execution_mode": "single_worker",
        },
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]
    detail = client.get(f"/api/runs/{run['run_id']}").json()
    assert any(verdict["decision"] == "request_repair" for verdict in detail["review_verdicts"])


def test_remote_worker_http_loop_completes_run(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    session = client.post(
        "/api/sessions",
        json={"goal": "Search the repository safely.", "context": {"path": "."}, "execution_mode": "remote_worker"},
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]

    runtime_client = WorkerRuntimeClient("http://testserver", request_fn=_client_transport(client))
    loop = WorkerExecutionLoop(
        runtime_client,
        poll_interval_seconds=0.01,
        repo_root=Path(harness_lab_services.database.repo_root),
        artifact_root=Path(harness_lab_services.settings.resolved_artifact_root()),
    )
    result = loop.serve(
        label="http-worker",
        capabilities=["knowledge_search"],
        interval_seconds=0.01,
        max_tasks=1,
        max_idle_cycles=2,
    )
    assert result["handled_dispatches"] >= 6

    run_detail = client.get(f"/api/runs/{run['run_id']}").json()
    assert run_detail["data"]["status"] == "completed"
    assert run_detail["status_summary"]["kind"] == "terminal"
    assert run_detail["timeline_summary"]["leases_by_status"]["completed"] >= 1
    assert run_detail["worker"]["execution_mode"] == "remote_http"
    assert run_detail["worker"]["hostname"]
    assert run_detail["worker"]["pid"]

    health = client.get("/api/health").json()["data"]
    assert all(item["run_id"] != run["run_id"] for item in health["stuck_runs"])


def test_remote_worker_policy_gate_is_not_marked_stuck(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def fake_call(settings, messages):
        if "intent declaration layer" in messages[0]["content"]:
            return (
                {
                    "task_type": "shell_command",
                    "intent": "Run the explicit shell command under approval control.",
                    "confidence": 0.96,
                    "risk_mode": "high",
                    "suggested_action": "shell",
                },
                ModelCallTrace(
                    provider=settings.provider,
                    model_name=settings.model_name,
                    latency_ms=11,
                    used_fallback=False,
                    failure_reason=None,
                ),
            )
        return (
            {
                "summary": "DeepSeek reflection completed.",
                "research_notes": ["Approval remains required.", "Do not bypass policy."],
                "details": {"source": "mock"},
            },
            ModelCallTrace(
                provider=settings.provider,
                model_name=settings.model_name,
                latency_ms=14,
                used_fallback=False,
                failure_reason=None,
            ),
        )

    monkeypatch.setattr(harness_lab_services.model_registry, "_call_provider_json", fake_call)

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Execute a reviewed shell command.",
            "context": {"shell_command": "mkdir -p backend/data/harness_lab/test_probe_http"},
            "execution_mode": "remote_worker",
        },
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]

    runtime_client = WorkerRuntimeClient("http://testserver", request_fn=_client_transport(client))
    loop = WorkerExecutionLoop(
        runtime_client,
        poll_interval_seconds=0.01,
        repo_root=Path(harness_lab_services.database.repo_root),
        artifact_root=Path(harness_lab_services.settings.resolved_artifact_root()),
    )
    result = loop.serve(
        label="approval-http-worker",
        capabilities=["shell"],
        interval_seconds=0.01,
        max_tasks=1,
        max_idle_cycles=2,
    )
    assert result["handled_dispatches"] >= 3

    run_detail = client.get(f"/api/runs/{run['run_id']}").json()
    assert run_detail["data"]["status"] == "awaiting_approval"
    assert run_detail["status_summary"]["kind"] == "awaiting_approval"

    health = client.get("/api/health").json()["data"]
    assert all(item["run_id"] != run["run_id"] for item in health["stuck_runs"])


def test_improvement_diagnosis_and_failure_clusters_capture_multi_agent_signals(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    async def failing_execute(run_id, action):  # noqa: ARG001
        from backend.app.harness_lab.types import ToolExecutionResult

        return ToolExecutionResult(ok=False, error="synthetic execution failure")

    monkeypatch.setattr(harness_lab_services.tool_gateway, "execute", failing_execute)

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Trigger multi-agent diagnosis.",
            "context": {"path": "backend/app/harness_lab/runtime"},
            "workflow_template_id": "workflow_template_recovery_ring_v1",
            "execution_mode": "single_worker",
        },
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]

    diagnosis = client.post("/api/improvement/diagnose", json={"trace_refs": [run["run_id"]]}).json()["data"]
    assert diagnosis["cluster_count"] >= 1
    assert any(cluster["signature_type"] in {"review_reject_loop", "repair_path_failure"} for cluster in diagnosis["clusters"])
    assert any(cluster["roles"] for cluster in diagnosis["clusters"])
    assert any(cluster["handoff_pairs"] for cluster in diagnosis["clusters"])

    clusters = client.get("/api/failure-clusters").json()["data"]
    assert any(cluster["signature_type"] in {"review_reject_loop", "repair_path_failure"} for cluster in clusters)


def test_policy_and_workflow_candidates_auto_evaluate_from_multi_agent_traces(client, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_provider(monkeypatch, intent_tool="knowledge_search")

    async def failing_execute(run_id, action):  # noqa: ARG001
        from backend.app.harness_lab.types import ToolExecutionResult

        return ToolExecutionResult(ok=False, error="synthetic execution failure")

    monkeypatch.setattr(harness_lab_services.tool_gateway, "execute", failing_execute)

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Generate trace-aware candidates.",
            "context": {"path": "backend/app/harness_lab/runtime"},
            "workflow_template_id": "workflow_template_recovery_ring_v1",
            "execution_mode": "single_worker",
        },
    ).json()["data"]
    run = client.post("/api/runs", json={"session_id": session["session_id"]}).json()["data"]

    policy_payload = client.post(
        "/api/improvement/candidates/policy",
        json={"trace_refs": [run["run_id"]]},
    ).json()["data"]
    assert policy_payload["candidate"]["evaluation_ids"]
    assert len(policy_payload["evaluations"]) == 2
    assert policy_payload["candidate"]["metrics"]["diagnosis"]["cluster_count"] >= 1
    assert policy_payload["gate"]["candidate_id"] == policy_payload["candidate"]["candidate_id"]

    workflow_payload = client.post(
        "/api/improvement/candidates/workflow",
        json={"trace_refs": [run["run_id"]]},
    ).json()["data"]
    assert workflow_payload["candidate"]["requires_human_approval"] is True
    assert workflow_payload["candidate"]["evaluation_ids"]
    assert len(workflow_payload["evaluations"]) == 2
    assert workflow_payload["candidate"]["metrics"]["proposal_summary"]
    assert workflow_payload["gate"]["approval_required"] is True


def test_candidate_gate_blocks_multi_agent_handoff_and_dispatch_regressions():
    candidate = {
        "candidate_id": "candidate_test",
        "kind": "policy",
        "approved": True,
    }
    replay = EvaluationReport(
        evaluation_id="eval_replay_ok",
        candidate_id="candidate_test",
        suite="replay",
        status="passed",
        success_rate=1.0,
        safety_score=1.0,
        recovery_score=1.0,
        regression_count=0,
        metrics={},
        trace_refs=["run_1"],
        created_at=datetime.now(timezone.utc).isoformat(),
        updated_at=datetime.now(timezone.utc).isoformat(),
    )
    benchmark = EvaluationReport(
        evaluation_id="eval_benchmark_fail",
        candidate_id="candidate_test",
        suite="benchmark",
        status="failed",
        success_rate=0.5,
        safety_score=0.6,
        recovery_score=0.4,
        regression_count=2,
        bucket_results=[
            BenchmarkBucketResult(bucket="handoff_chain", total=1, passed=0, failed=1, coverage=1.0, regressions=["handoff stalled"]),
            BenchmarkBucketResult(bucket="review_gate", total=1, passed=1, failed=0, coverage=1.0, regressions=[]),
            BenchmarkBucketResult(bucket="role_dispatch", total=1, passed=0, failed=1, coverage=1.0, regressions=["role dispatch regressed"]),
            BenchmarkBucketResult(bucket="approval_sandbox", total=1, passed=1, failed=0, coverage=1.0, regressions=[]),
        ],
        hard_failures=[
            EvaluationFailure(
                kind="safety_regression",
                severity="hard",
                bucket="role_dispatch",
                trace_ref="run_1",
                summary="role dispatch regressed",
            )
        ],
        metrics={},
        trace_refs=["run_1"],
        created_at=datetime.now(timezone.utc).isoformat(),
        updated_at=datetime.now(timezone.utc).isoformat(),
    )

    gate = harness_lab_services.improvement.evaluation_harness.candidate_gate(candidate, [replay, benchmark])
    assert gate.publish_ready is False
    assert any("handoff_chain" in blocker for blocker in gate.blockers)
    assert any("role_dispatch" in blocker for blocker in gate.blockers)
