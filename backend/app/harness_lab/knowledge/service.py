from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from ..storage import HarnessLabDatabase
from ..types import (
    ArtifactRef,
    KnowledgeIndexStatus,
    KnowledgeReindexScope,
    KnowledgeSearchHit,
    KnowledgeSearchResult,
    KnowledgeSourceType,
)
from ..utils import compact_text, new_id, read_json, safe_preview, score_overlap, utc_now, write_json

DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


class KnowledgeIndexService:
    """Local-first retrieval service with semantic indexing and lexical fallback."""

    def __init__(
        self,
        database: HarnessLabDatabase,
        model_name: str = DEFAULT_EMBEDDING_MODEL,
        max_chars: int = 1200,
        overlap_chars: int = 150,
    ) -> None:
        self.database = database
        self.repo_root = database.repo_root
        self.knowledge_root = self.database.data_dir / "knowledge"
        self.knowledge_root.mkdir(parents=True, exist_ok=True)
        self.model_name = model_name
        self.max_chars = max_chars
        self.overlap_chars = overlap_chars
        self.manifest_path = self.knowledge_root / "chunk_manifest.json"
        self.status_path = self.knowledge_root / "status.json"
        self.index_path = self.knowledge_root / "semantic.index"
        self.vector_path = self.knowledge_root / "semantic_vectors.npy"
        self.excluded_prefixes = [
            ".git",
            "frontend/node_modules",
            "frontend/dist",
            "backend/tests",
            "backend/data/harness_lab/artifacts",
            "backend/data/harness_lab/knowledge",
            "__pycache__",
            ".pytest_cache",
        ]
        self.doc_roots = ["design", ".kiro/specs"]
        self.doc_files = {"README.md", "PROJECT_SUMMARY.md", "SUGGESTIONS.md"}
        self._encoder: Any | None = None
        self._faiss: Any | None = None

    def status(self) -> KnowledgeIndexStatus:
        payload = read_json(self.status_path, {})
        return KnowledgeIndexStatus(
            ready=bool(payload.get("ready", False)),
            document_count=int(payload.get("document_count", 0)),
            chunk_count=int(payload.get("chunk_count", 0)),
            last_indexed_at=payload.get("last_indexed_at"),
            fallback_mode=bool(payload.get("fallback_mode", True)),
            model_name=payload.get("model_name") or self.model_name,
        )

    def reindex(self, scope: KnowledgeReindexScope = "all") -> KnowledgeIndexStatus:
        documents = self._collect_documents(scope)
        chunks = self._build_chunks(documents)
        embeddings = self._encode_chunks(chunks)
        if embeddings is not None and len(chunks) == len(embeddings):
            faiss_module = self._get_faiss()
            index = faiss_module.IndexFlatIP(int(embeddings.shape[1]))
            index.add(embeddings)
            faiss_module.write_index(index, str(self.index_path))
            np.save(self.vector_path, embeddings)
            fallback_mode = False
        else:
            self._safe_unlink(self.index_path)
            self._safe_unlink(self.vector_path)
            fallback_mode = True
        payload = {
            "ready": bool(chunks),
            "document_count": len(documents),
            "chunk_count": len(chunks),
            "last_indexed_at": utc_now(),
            "fallback_mode": fallback_mode,
            "model_name": self.model_name,
            "chunks": chunks,
        }
        write_json(self.manifest_path, payload)
        write_json(self.status_path, {key: value for key, value in payload.items() if key != "chunks"})
        return self.status()

    def search(
        self,
        query: str,
        top_k: int = 5,
        path_hint: Optional[str] = None,
        source_types: Optional[Sequence[KnowledgeSourceType]] = None,
    ) -> KnowledgeSearchResult:
        cleaned_query = (query or "").strip()
        normalized_types = list(source_types or [])
        if not cleaned_query:
            return KnowledgeSearchResult(
                query="",
                top_k=max(1, top_k),
                path_hint=path_hint,
                source_types=normalized_types,
                hits=[],
                used_fallback=True,
                source_coverage={},
                status=self.status(),
            )
        manifest = read_json(self.manifest_path, {})
        status = self.status()
        chunks = manifest.get("chunks", [])
        if not chunks:
            chunks = self._build_chunks(self._collect_documents("all"))
            status.ready = bool(chunks)
            status.document_count = len({chunk["source_ref"] for chunk in chunks})
            status.chunk_count = len(chunks)
        filtered_chunks = self._filter_chunks(chunks, normalized_types)
        if not filtered_chunks:
            filtered_chunks = chunks
        semantic_hits = self._semantic_search(cleaned_query, filtered_chunks, top_k=max(1, top_k), path_hint=path_hint)
        if semantic_hits:
            hits = semantic_hits
            used_fallback = False
        else:
            hits = self._fallback_hits(cleaned_query, filtered_chunks, top_k=max(1, top_k), path_hint=path_hint)
            used_fallback = True
        return KnowledgeSearchResult(
            query=cleaned_query,
            top_k=max(1, top_k),
            path_hint=path_hint,
            source_types=normalized_types,
            hits=hits,
            used_fallback=used_fallback,
            source_coverage=self._source_coverage(hits),
            status=KnowledgeIndexStatus(
                ready=status.ready,
                document_count=status.document_count or len({chunk["source_ref"] for chunk in chunks}),
                chunk_count=status.chunk_count or len(chunks),
                last_indexed_at=status.last_indexed_at,
                fallback_mode=status.fallback_mode or used_fallback,
                model_name=status.model_name or self.model_name,
            ),
        )

    def _collect_documents(self, scope: KnowledgeReindexScope) -> List[Dict[str, Any]]:
        documents: List[Dict[str, Any]] = []
        include_workspace = scope in {"workspace", "all"}
        include_docs = scope in {"docs", "all"}
        include_artifacts = scope in {"artifacts", "all"}

        for path in self._iter_workspace_files():
            relative = str(path.relative_to(self.repo_root))
            source_type = self._source_type_for_path(relative)
            if source_type == "docs" and not include_docs:
                continue
            if source_type == "workspace" and not include_workspace:
                continue
            documents.append(
                {
                    "source_type": source_type,
                    "source_ref": f"file://{relative}",
                    "title": relative,
                    "path": relative,
                    "content": path.read_text(encoding="utf-8"),
                    "updated_at": utc_now(),
                }
            )

        if include_artifacts:
            documents.extend(self._artifact_documents())
        return documents

    def _artifact_documents(self) -> List[Dict[str, Any]]:
        documents: List[Dict[str, Any]] = []
        for artifact in self.database.list_artifacts():
            if artifact.artifact_type not in {"learning_summary", "recovery_packet"}:
                continue
            absolute_path = self.database.artifact_root / artifact.relative_path
            if not absolute_path.exists():
                continue
            try:
                content = absolute_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            documents.append(
                {
                    "source_type": "artifacts",
                    "source_ref": f"artifact://{artifact.artifact_id}",
                    "title": artifact.relative_path,
                    "path": artifact.relative_path,
                    "artifact_type": artifact.artifact_type,
                    "content": content,
                    "updated_at": artifact.created_at,
                }
            )
        return documents

    def _build_chunks(self, documents: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        chunks: List[Dict[str, Any]] = []
        for document in documents:
            content = str(document.get("content", "") or "").strip()
            if not content:
                continue
            for chunk in self._chunk_document(document):
                chunks.append(chunk)
        return chunks

    def _chunk_document(self, document: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        content = str(document["content"])
        start = 0
        chunk_index = 0
        previous_start = -1
        while start < len(content):
            end = min(len(content), start + self.max_chars)
            if end < len(content):
                natural_break = content.rfind("\n", start, end)
                if natural_break > start + max(40, self.max_chars // 3):
                    end = natural_break + 1
            if end <= start:
                end = min(len(content), start + self.max_chars)
            chunk_text = content[start:end].strip()
            if chunk_text:
                line_start = content.count("\n", 0, start) + 1
                line_end = line_start + chunk_text.count("\n")
                yield {
                    "chunk_id": new_id("chunk"),
                    "source_type": document["source_type"],
                    "source_ref": document["source_ref"],
                    "title": document["title"],
                    "path": document.get("path"),
                    "artifact_type": document.get("artifact_type"),
                    "updated_at": document.get("updated_at"),
                    "line_start": line_start,
                    "line_end": line_end,
                    "content": chunk_text,
                    "chunk_index": chunk_index,
                }
                chunk_index += 1
            if end >= len(content):
                break
            previous_start = start
            start = max(end - self.overlap_chars, start + 1)
            if start <= previous_start:
                start = end

    def _encode_chunks(self, chunks: Sequence[Dict[str, Any]]) -> Optional[np.ndarray]:
        if not chunks:
            return None
        encoder = self._get_encoder()
        if encoder is None or self._get_faiss() is None:
            return None
        texts = [chunk["content"] for chunk in chunks]
        embeddings = np.asarray(encoder.encode(texts, normalize_embeddings=True), dtype="float32")
        if embeddings.ndim != 2 or embeddings.shape[0] != len(chunks):
            return None
        return embeddings

    def _semantic_search(
        self,
        query: str,
        chunks: Sequence[Dict[str, Any]],
        top_k: int,
        path_hint: Optional[str],
    ) -> List[KnowledgeSearchHit]:
        if not chunks or not self.index_path.exists():
            return []
        encoder = self._get_encoder()
        faiss_module = self._get_faiss()
        if encoder is None or faiss_module is None:
            return []
        manifest = read_json(self.manifest_path, {})
        all_chunks = manifest.get("chunks", [])
        if not all_chunks:
            return []
        allowed_ids = {chunk["chunk_id"] for chunk in chunks}
        try:
            index = faiss_module.read_index(str(self.index_path))
            query_vector = np.asarray(encoder.encode([query], normalize_embeddings=True), dtype="float32")
            search_k = min(max(top_k * 10, 20), len(all_chunks))
            scores, indices = index.search(query_vector, search_k)
        except Exception:  # noqa: BLE001
            return []
        ranked: List[Tuple[float, Dict[str, Any]]] = []
        for score, index_id in zip(scores[0], indices[0]):
            if index_id < 0 or index_id >= len(all_chunks):
                continue
            chunk = all_chunks[int(index_id)]
            if chunk["chunk_id"] not in allowed_ids:
                continue
            ranked.append((self._rank_score(float(score), chunk, query, path_hint), chunk))
        if not ranked:
            return []
        ranked.sort(key=lambda item: item[0], reverse=True)
        return [self._hit_from_chunk(chunk, score) for score, chunk in ranked[:top_k]]

    def _fallback_hits(
        self,
        query: str,
        chunks: Sequence[Dict[str, Any]],
        top_k: int,
        path_hint: Optional[str],
    ) -> List[KnowledgeSearchHit]:
        ranked: List[Tuple[float, Dict[str, Any]]] = []
        for chunk in chunks:
            overlap_score = score_overlap(query, f"{chunk.get('path', '')}\n{chunk['content']}")
            if overlap_score <= 0:
                continue
            ranked.append((self._rank_score(overlap_score, chunk, query, path_hint), chunk))
        ranked.sort(key=lambda item: item[0], reverse=True)
        return [self._hit_from_chunk(chunk, score) for score, chunk in ranked[:top_k]]

    @staticmethod
    def _filter_chunks(
        chunks: Sequence[Dict[str, Any]],
        source_types: Sequence[KnowledgeSourceType],
    ) -> List[Dict[str, Any]]:
        if not source_types:
            return list(chunks)
        allowed = set(source_types)
        return [chunk for chunk in chunks if chunk.get("source_type") in allowed]

    def _rank_score(self, base_score: float, chunk: Dict[str, Any], query: str, path_hint: Optional[str]) -> float:
        lexical = score_overlap(query, f"{chunk.get('title', '')}\n{chunk.get('content', '')}")
        bonus = 0.0
        relative_path = str(chunk.get("path", "") or "")
        if path_hint and relative_path and path_hint in relative_path:
            bonus += 0.35
        return round(base_score + (lexical * 0.25) + bonus, 4)

    @staticmethod
    def _hit_from_chunk(chunk: Dict[str, Any], score: float) -> KnowledgeSearchHit:
        metadata = {
            "path": chunk.get("path"),
            "line_start": chunk.get("line_start"),
            "line_end": chunk.get("line_end"),
            "artifact_type": chunk.get("artifact_type"),
            "updated_at": chunk.get("updated_at"),
        }
        return KnowledgeSearchHit(
            chunk_id=chunk["chunk_id"],
            source_type=chunk["source_type"],
            source_ref=chunk["source_ref"],
            title=chunk["title"],
            snippet=compact_text(chunk["content"], 500),
            score=round(score, 4),
            metadata={key: value for key, value in metadata.items() if value is not None},
        )

    @staticmethod
    def _source_coverage(hits: Sequence[KnowledgeSearchHit]) -> Dict[str, int]:
        coverage: Dict[str, int] = {}
        for hit in hits:
            coverage[hit.source_type] = coverage.get(hit.source_type, 0) + 1
        return dict(sorted(coverage.items()))

    def _get_encoder(self):
        if self._encoder is not None:
            return self._encoder
        try:
            from sentence_transformers import SentenceTransformer
        except Exception:  # noqa: BLE001
            return None
        try:
            self._encoder = SentenceTransformer(self.model_name)
        except Exception:  # noqa: BLE001
            self._encoder = None
        return self._encoder

    def _get_faiss(self):
        if self._faiss is not None:
            return self._faiss
        try:
            import faiss
        except Exception:  # noqa: BLE001
            return None
        self._faiss = faiss
        return self._faiss

    def _iter_workspace_files(self):
        for path in self.repo_root.rglob("*"):
            if not path.is_file():
                continue
            relative = str(path.relative_to(self.repo_root))
            if any(relative == prefix or relative.startswith(prefix + "/") for prefix in self.excluded_prefixes):
                continue
            if path.name.startswith(".env"):
                continue
            if path.stat().st_size > 250_000:
                continue
            try:
                path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            yield path

    def _source_type_for_path(self, relative: str) -> KnowledgeSourceType:
        if relative in self.doc_files or any(relative == root or relative.startswith(root + "/") for root in self.doc_roots):
            return "docs"
        if Path(relative).suffix.lower() in {".md", ".txt", ".rst"}:
            return "docs"
        return "workspace"

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        if path.exists():
            path.unlink()
