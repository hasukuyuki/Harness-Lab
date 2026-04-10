from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import numpy as np

from backend.app.harness_lab.boundary.gateway import ToolGateway
from backend.app.harness_lab.knowledge.service import KnowledgeIndexService
from backend.app.harness_lab.types import ActionPlan, ArtifactRef, ToolExecutionResult
from backend.app.harness_lab.utils import utc_now
from backend.app.harness_lab.workers.runtime_client import LocalArtifactStore


class _NoopConstraints:
    def verify(self, subject: str, payload: dict[str, Any], constraint_set_id: str | None = None):  # noqa: ARG002
        return []


class FakeKnowledgeDatabase:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self.data_dir = repo_root / "data"
        self.artifact_root = repo_root / "artifacts"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self._artifacts: list[ArtifactRef] = []

    def list_artifacts(self, run_id: str | None = None):
        if run_id is None:
            return list(self._artifacts)
        return [artifact for artifact in self._artifacts if artifact.run_id == run_id]

    def add_artifact(self, artifact_type: str, relative_path: str, content: str) -> None:
        absolute_path = self.artifact_root / relative_path
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        absolute_path.write_text(content, encoding="utf-8")
        self._artifacts.append(
            ArtifactRef(
                artifact_id=f"artifact_{len(self._artifacts) + 1}",
                run_id="run_unit",
                artifact_type=artifact_type,
                relative_path=relative_path,
                metadata={},
                created_at=utc_now(),
            )
        )


class FakeEncoder:
    def encode(self, texts, normalize_embeddings: bool = True):
        vectors = []
        for text in texts:
            lowered = text.lower()
            vectors.append(
                [
                    2.0 if "runtime" in lowered else 0.2,
                    2.0 if "lease" in lowered else 0.1,
                    2.0 if "design" in lowered else 0.1,
                ]
            )
        array = np.asarray(vectors, dtype="float32")
        if normalize_embeddings:
            norms = np.linalg.norm(array, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            array = array / norms
        return array


class FakeIndexFlatIP:
    def __init__(self, dimensions: int) -> None:
        self.dimensions = dimensions
        self.vectors = np.zeros((0, dimensions), dtype="float32")

    def add(self, embeddings) -> None:
        self.vectors = np.asarray(embeddings, dtype="float32")

    def search(self, query_vectors, top_k: int):
        if len(self.vectors) == 0:
            return np.zeros((1, top_k), dtype="float32"), np.full((1, top_k), -1, dtype="int64")
        scores = np.dot(self.vectors, np.asarray(query_vectors[0], dtype="float32"))
        order = np.argsort(-scores)[:top_k]
        top_scores = np.full((1, top_k), -1, dtype="float32")
        top_indices = np.full((1, top_k), -1, dtype="int64")
        top_scores[0, : len(order)] = scores[order]
        top_indices[0, : len(order)] = order
        return top_scores, top_indices


class FakeFaissModule:
    def __init__(self) -> None:
        self._registry: dict[str, FakeIndexFlatIP] = {}

    def IndexFlatIP(self, dimensions: int) -> FakeIndexFlatIP:  # noqa: N802
        return FakeIndexFlatIP(dimensions)

    def write_index(self, index: FakeIndexFlatIP, path: str) -> None:
        self._registry[path] = index
        Path(path).write_text("fake-index", encoding="utf-8")

    def read_index(self, path: str) -> FakeIndexFlatIP:
        return self._registry[path]


def test_knowledge_index_service_semantic_reindex_and_filtered_search(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    runtime_file = repo_root / "backend" / "app" / "harness_lab" / "runtime" / "service.py"
    design_file = repo_root / "design" / "harness-architecture-design.md"
    runtime_file.parent.mkdir(parents=True, exist_ok=True)
    design_file.parent.mkdir(parents=True, exist_ok=True)
    runtime_file.write_text("Remote worker runtime lease reclaim logic lives here.", encoding="utf-8")
    design_file.write_text("Harness design notes for execution boundaries.", encoding="utf-8")

    database = FakeKnowledgeDatabase(repo_root)
    database.add_artifact("recovery_packet", "run_unit/recovery_packet/recovery.json", "lease recovery packet details")

    service = KnowledgeIndexService(database)
    fake_faiss = FakeFaissModule()
    monkeypatch.setattr(service, "_get_encoder", lambda: FakeEncoder())
    monkeypatch.setattr(service, "_get_faiss", lambda: fake_faiss)

    status = service.reindex("all")
    assert status.ready is True
    assert status.fallback_mode is False
    assert status.chunk_count >= 3

    workspace_result = service.search(
        query="runtime lease reclaim",
        top_k=3,
        path_hint="backend/app/harness_lab/runtime",
        source_types=["workspace"],
    )
    assert workspace_result.used_fallback is False
    assert workspace_result.hits
    assert workspace_result.hits[0].metadata["path"].endswith("runtime/service.py")

    docs_result = service.search(query="execution boundary design", top_k=2, source_types=["docs"])
    assert docs_result.hits
    assert all(hit.source_type == "docs" for hit in docs_result.hits)

    artifact_result = service.search(query="recovery packet", top_k=2, source_types=["artifacts"])
    assert artifact_result.hits
    assert artifact_result.hits[0].metadata["artifact_type"] == "recovery_packet"


def test_tool_gateway_executes_fallback_search_and_workspace_tools(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    work_file = repo_root / "docs" / "runtime-notes.md"
    work_file.parent.mkdir(parents=True, exist_ok=True)
    work_file.write_text("Runtime notes about remote workers and lease reclaim.", encoding="utf-8")

    store = LocalArtifactStore(repo_root=repo_root, artifact_root=repo_root / "artifacts")
    gateway = ToolGateway(store, _NoopConstraints(), knowledge_index=None)

    snapshot = gateway.create_snapshot_manifest("run_gateway")
    assert snapshot.relative_path.endswith("workspace_manifest.json")

    write_result = asyncio.run(
        gateway.execute(
            "run_gateway",
            ActionPlan(
                tool_name="filesystem",
                subject="tool.filesystem.write_file",
                payload={"action": "write_file", "path": "scratch.txt", "content": "temporary note"},
            ),
        )
    )
    assert write_result.ok is True
    assert write_result.output["written_path"] == "scratch.txt"

    read_result = asyncio.run(
        gateway.execute(
            "run_gateway",
            ActionPlan(
                tool_name="filesystem",
                subject="tool.filesystem.read_file",
                payload={"action": "read_file", "path": "scratch.txt"},
            ),
        )
    )
    assert read_result.ok is True
    assert "temporary note" in read_result.output["content"]

    list_result = asyncio.run(
        gateway.execute(
            "run_gateway",
            ActionPlan(
                tool_name="filesystem",
                subject="tool.filesystem.list_dir",
                payload={"action": "list_dir", "path": "."},
            ),
        )
    )
    assert list_result.ok is True
    assert any(entry["name"] == "scratch.txt" for entry in list_result.output["entries"])

    shell_result = asyncio.run(
        gateway.execute(
            "run_gateway",
            ActionPlan(tool_name="shell", subject="tool.shell.execute", payload={"command": "pwd"}),
        )
    )
    assert shell_result.ok is True
    assert str(repo_root) in shell_result.output["stdout"]

    async def fake_shell(payload: dict[str, Any]) -> ToolExecutionResult:
        return ToolExecutionResult(ok=True, output={"command": payload["command"], "stdout": "", "stderr": "", "exit_code": 0})

    monkeypatch.setattr(gateway, "_run_shell", fake_shell)
    git_result = asyncio.run(
        gateway.execute(
            "run_gateway",
            ActionPlan(tool_name="git", subject="tool.git.status", payload={"action": "status"}),
        )
    )
    assert git_result.ok is True
    assert git_result.output["command"] == "git status --short"

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def read(self, _size: int | None = None):
            return b"remote documentation body"

    monkeypatch.setattr("urllib.request.urlopen", lambda url, timeout=10: FakeResponse())
    http_result = asyncio.run(
        gateway.execute(
            "run_gateway",
            ActionPlan(tool_name="http_fetch", subject="tool.http_fetch.get", payload={"url": "https://example.com"}),
        )
    )
    assert http_result.ok is True
    assert http_result.output["status"] == 200

    search_result = asyncio.run(
        gateway.execute(
            "run_gateway",
            ActionPlan(
                tool_name="knowledge_search",
                subject="tool.knowledge_search.query",
                payload={"query": "remote worker lease reclaim", "top_k": 3},
            ),
        )
    )
    assert search_result.ok is True
    assert search_result.output["hits"]
    assert search_result.output["used_fallback"] is True
    assert search_result.output["artifact_id"]
