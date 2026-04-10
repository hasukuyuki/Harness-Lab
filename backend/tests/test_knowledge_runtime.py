from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app.harness_lab.bootstrap import (
    harness_lab_services,
    initialize_harness_lab_services,
    shutdown_harness_lab_services,
)
from backend.app.harness_lab.dispatch_queue import InMemoryDispatchQueue
from backend.app.harness_lab.settings import HarnessLabSettings
from backend.app.harness_lab.storage import SqliteTestPlatformStore
from backend.app.harness_lab.types import ModelCallTrace
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
        HARNESS_WORKER_POLL_INTERVAL=0.05,
        HARNESS_REDIS_NAMESPACE="harness_lab_knowledge_test",
        HARNESS_ARTIFACT_ROOT=str(tmp_path / "artifacts"),
    )
    database = SqliteTestPlatformStore(db_path=str(tmp_path / "harness_lab.db"), artifact_root=str(tmp_path / "artifacts"))
    queue = InMemoryDispatchQueue()
    initialize_harness_lab_services(settings=settings, database=database, dispatch_queue=queue, force=True)
    with TestClient(app) as test_client:
        yield test_client
    shutdown_harness_lab_services()


def _mock_provider(monkeypatch, intent_tool: str = "knowledge_search"):
    def fake_call(settings, messages):
        system_prompt = messages[0]["content"]
        if "intent declaration layer" in system_prompt:
            return (
                {
                    "task_type": intent_tool,
                    "intent": f"Use {intent_tool} to inspect knowledge before changing anything.",
                    "confidence": 0.94,
                    "risk_mode": "low",
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
                "summary": "Reflection completed.",
                "research_notes": ["Prefer replayable retrieval outputs."],
                "details": {"source": "mock"},
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


def test_knowledge_reindex_search_and_health(client):
    harness_lab_services.database.write_artifact_text(
        run_id="run_history_knowledge",
        artifact_type="learning_summary",
        filename="learning_summary.md",
        content="Remote worker lease reclaim notes and replay learnings.",
        metadata={"kind": "learning_summary"},
    )

    reindex = client.post("/api/knowledge/reindex", json={"scope": "all"})
    assert reindex.status_code == 200
    reindex_payload = reindex.json()["data"]
    assert reindex_payload["ready"] is True
    assert reindex_payload["document_count"] >= 1
    assert reindex_payload["chunk_count"] >= 1
    assert reindex_payload["last_indexed_at"]

    search = client.post(
        "/api/knowledge/search",
        json={
            "query": "remote worker runtime lease reclaim harness design",
            "top_k": 5,
            "path_hint": "backend/app/harness_lab/runtime",
        },
    )
    assert search.status_code == 200
    search_payload = search.json()["data"]
    assert search_payload["hits"]
    assert search_payload["status"]["chunk_count"] >= 1
    assert search_payload["used_fallback"] == search_payload["status"]["fallback_mode"]
    assert any(
        "runtime" in ((hit["title"] + str(hit["metadata"])).lower()) or "lease" in hit["snippet"].lower()
        for hit in search_payload["hits"]
    )

    health = client.get("/api/health").json()["data"]
    assert health["knowledge_index_ready"] is True
    assert health["knowledge_document_count"] >= 1
    assert health["knowledge_chunk_count"] >= 1
    assert health["knowledge_last_indexed_at"]

    catalog = client.get("/api/settings/catalog").json()["data"]
    assert catalog["knowledge_index_ready"] is True
    assert catalog["knowledge_document_count"] >= 1
    assert catalog["knowledge_chunk_count"] >= 1
    assert catalog["knowledge_index"]["ready"] is True


def test_context_assemble_uses_knowledge_hits(client):
    client.post("/api/knowledge/reindex", json={"scope": "all"})

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Find the remote worker runtime and lease reclaim logic.",
            "context": {"path": "backend/app/harness_lab/runtime"},
            "execution_mode": "single_worker",
        },
    ).json()["data"]

    assembled = client.post("/api/context/assemble", json={"session_id": session["session_id"]})
    assert assembled.status_code == 200
    payload = assembled.json()["data"]
    knowledge = payload["selection_summary"]["knowledge_search"]
    assert knowledge is not None
    assert knowledge["hits"]
    index_blocks = [block for block in payload["blocks"] if block["layer"] == "index"]
    assert index_blocks
    assert any(block["metadata"]["chunk_id"] for block in index_blocks)
    assert all(block["type"] == "knowledge_hit" for block in index_blocks)


def test_single_worker_run_records_knowledge_artifacts(client, monkeypatch):
    _mock_provider(monkeypatch)
    client.post("/api/knowledge/reindex", json={"scope": "all"})

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Inspect the remote worker runtime before changing anything.",
            "context": {"path": "backend/app/harness_lab/workers"},
            "execution_mode": "single_worker",
        },
    ).json()["data"]

    run = client.post("/api/runs", json={"session_id": session["session_id"]})
    assert run.status_code == 200
    run_payload = run.json()["data"]
    tool_call = run_payload["execution_trace"]["tool_calls"][0]
    assert tool_call["tool_name"] == "knowledge_search"
    assert tool_call["output"]["hits"]
    assert "used_fallback" in tool_call["output"]

    run_detail = client.get(f"/api/runs/{run_payload['run_id']}").json()
    artifact_paths = [artifact["relative_path"] for artifact in run_detail["artifacts"] if artifact["artifact_type"] == "knowledge_search_results"]
    assert any(path.endswith("knowledge_search_results/knowledge_search_results.json") for path in artifact_paths)
    assert any(path.endswith("knowledge_search_results/execution_search_results.json") for path in artifact_paths)
    assert run_detail["data"]["result"]["context_selection_summary"]["knowledge_search"]["hits"]


def test_remote_worker_knowledge_search_run_uses_control_plane_api(client, monkeypatch):
    _mock_provider(monkeypatch)
    client.post("/api/knowledge/reindex", json={"scope": "all"})

    session = client.post(
        "/api/sessions",
        json={
            "goal": "Search the runtime and execution plane implementation.",
            "context": {"path": "backend/app/harness_lab/runtime"},
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
        label="knowledge-http-worker",
        capabilities=["knowledge_search"],
        interval_seconds=0.01,
        max_tasks=1,
        max_idle_cycles=2,
    )
    assert result["handled_dispatches"] >= 6

    run_detail = client.get(f"/api/runs/{run['run_id']}").json()
    assert run_detail["data"]["status"] == "completed"
    tool_calls = run_detail["data"]["execution_trace"]["tool_calls"]
    assert tool_calls[0]["tool_name"] == "knowledge_search"
    assert tool_calls[0]["output"]["hits"]
    assert "artifact_id" in tool_calls[0]["output"]
    assert run_detail["worker"]["execution_mode"] == "remote_http"


def test_knowledge_runtime_reports_fallback_state_when_semantic_stack_missing(client):
    client.post("/api/knowledge/reindex", json={"scope": "all"})
    status = harness_lab_services.knowledge.status()
    semantic_stack_ready = importlib.util.find_spec("faiss") is not None and importlib.util.find_spec("sentence_transformers") is not None
    if semantic_stack_ready:
        assert status.fallback_mode is False
    else:
        assert status.fallback_mode is True
