from __future__ import annotations

import json
from typing import Dict, List, Optional, Tuple

from ..knowledge.service import KnowledgeIndexService
from ..storage import HarnessLabDatabase
from ..types import ContextBlock, ContextProfile, IntentDeclaration, KnowledgeSearchResult, ResearchSession
from ..utils import compact_text, new_id, score_overlap, token_estimate, top_items


class ContextManager:
    """Layered context assembler for Harness Lab."""

    def __init__(self, database: HarnessLabDatabase, knowledge_index: Optional[KnowledgeIndexService] = None) -> None:
        self.database = database
        self.knowledge_index = knowledge_index
        self.repo_root = database.repo_root
        self.excluded_prefixes = [
            ".git",
            "frontend/node_modules",
            "frontend/dist",
            "backend/data",
            "__pycache__",
            ".pytest_cache",
        ]

    def assemble(
        self,
        session: ResearchSession,
        profile: ContextProfile,
        intent: Optional[IntentDeclaration] = None,
    ) -> Tuple[List[ContextBlock], Dict[str, Any]]:
        blocks: List[ContextBlock] = []
        selected_goal = session.goal
        now_intent = intent or session.intent_declaration
        structure_content = self._structure_summary()
        blocks.append(
            self._block(
                layer="structure",
                block_type="workspace_map",
                title="Workspace structure",
                source_ref="workspace://root",
                content=structure_content,
                score=1.0,
                selected=True,
                metadata={"kind": "always_on"},
            )
        )
        task_content = f"Goal: {selected_goal}\nContext: {json.dumps(session.context, ensure_ascii=False, indent=2)}"
        if now_intent:
            task_content += f"\nIntent: {now_intent.intent}\nTask type: {now_intent.task_type}\nRisk mode: {now_intent.risk_mode}"
        blocks.append(
            self._block(
                layer="task",
                block_type="goal_bundle",
                title="Active task",
                source_ref="session://goal",
                content=task_content,
                score=1.0,
                selected=True,
                metadata={"kind": "always_on"},
            )
        )
        for history in self._history_blocks(selected_goal, profile.config.get("history_limit", 2)):
            blocks.append(history)
        path_hint = str(session.context.get("path", "") or "")
        index_limit = int(profile.config.get("index_limit", 6))
        knowledge_result = self._knowledge_result(selected_goal, path_hint, now_intent, index_limit)
        for file_block in self._index_blocks(knowledge_result):
            blocks.append(file_block)

        max_tokens = int(profile.config.get("max_tokens", 1400))
        max_blocks = int(profile.config.get("max_blocks", 8))
        token_total = 0
        selected_count = 0
        truncated: List[str] = []
        for block in sorted(blocks, key=self._selection_sort_key):
            must_keep = block.metadata.get("kind") == "always_on"
            if must_keep:
                token_total += block.token_estimate
                selected_count += 1
                continue
            if selected_count >= max_blocks or token_total + block.token_estimate > max_tokens:
                block.selected = False
                truncated.append(block.context_block_id)
                continue
            block.selected = True
            token_total += block.token_estimate
            selected_count += 1
        summary = {
            "selected_count": len([block for block in blocks if block.selected]),
            "total_count": len(blocks),
            "max_tokens": max_tokens,
            "used_tokens": token_total,
            "truncated_blocks": truncated,
            "knowledge_search": knowledge_result.model_dump() if knowledge_result else None,
        }
        return blocks, summary

    def _block(
        self,
        layer: str,
        block_type: str,
        title: str,
        source_ref: str,
        content: str,
        score: float,
        selected: bool,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ContextBlock:
        return ContextBlock(
            context_block_id=new_id("ctx"),
            layer=layer,  # type: ignore[arg-type]
            type=block_type,
            title=title,
            source_ref=source_ref,
            content=content,
            score=round(score, 3),
            token_estimate=token_estimate(content),
            selected=selected,
            dependencies=[],
            metadata=metadata or {},
        )

    def _structure_summary(self) -> str:
        top_level = top_items(
            sorted(
                [
                    item.name
                    for item in self.repo_root.iterdir()
                    if not item.name.startswith(".") and item.name not in {"frontend", "backend"} or item.name in {"frontend", "backend", "design"}
                ]
            ),
            14,
        )
        key_paths = [
            "backend/app/main.py",
            "backend/app/harness_lab",
            "frontend/src/App.tsx",
            "frontend/src/lab",
            "design/harness-architecture-design.md",
            ".kiro/specs/graphical-frontend-interface/design.md",
        ]
        return (
            "Top-level workspace entries:\n- "
            + "\n- ".join(top_level)
            + "\nKey paths:\n- "
            + "\n- ".join(key_paths)
        )

    def _history_blocks(self, goal: str, limit: int) -> List[ContextBlock]:
        rows = self.database.fetchall(
            "SELECT payload_json FROM runs ORDER BY updated_at DESC LIMIT ?",
            (max(3, limit * 3),),
        )
        blocks: List[ContextBlock] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            score = score_overlap(goal, json.dumps(payload.get("result", {}), ensure_ascii=False))
            if score <= 0 and len(blocks) >= limit:
                continue
            result_summary = payload.get("result", {}).get("summary", "No result summary recorded")
            blocks.append(
                self._block(
                    layer="history",
                    block_type="recent_run",
                    title=f"Recent run {payload.get('run_id')}",
                    source_ref=f"run://{payload.get('run_id')}",
                    content=compact_text(result_summary, 500),
                    score=max(score, 0.15),
                    selected=False,
                    metadata={"status": payload.get("status")},
                )
            )
            if len(blocks) >= limit:
                break
        return blocks

    def _index_blocks(self, result: Optional[KnowledgeSearchResult]) -> List[ContextBlock]:
        blocks: List[ContextBlock] = []
        if not result:
            return blocks
        for hit in result.hits:
            relative = str(hit.metadata.get("path") or hit.title)
            blocks.append(
                self._block(
                    layer="index",
                    block_type="knowledge_hit",
                    title=relative,
                    source_ref=hit.source_ref,
                    content=hit.snippet,
                    score=hit.score,
                    selected=False,
                    metadata={
                        "path": hit.metadata.get("path"),
                        "chunk_id": hit.chunk_id,
                        "source_type": hit.source_type,
                        "used_fallback": result.used_fallback,
                        "line_start": hit.metadata.get("line_start"),
                        "line_end": hit.metadata.get("line_end"),
                    },
                )
            )
        return blocks

    def _knowledge_result(
        self,
        goal: str,
        path_hint: str,
        intent: Optional[IntentDeclaration],
        limit: int,
    ) -> Optional[KnowledgeSearchResult]:
        if self.knowledge_index is None:
            return None
        latest_signal = self._latest_run_signal()
        query_parts = [goal]
        if path_hint:
            query_parts.append(path_hint)
        if intent:
            query_parts.extend([intent.task_type, intent.intent])
        if latest_signal:
            query_parts.append(latest_signal)
        return self.knowledge_index.search(
            query=" ".join(part for part in query_parts if part).strip(),
            top_k=max(1, limit),
            path_hint=path_hint or None,
        )

    def _latest_run_signal(self) -> str:
        rows = self.database.fetchall("SELECT payload_json FROM runs ORDER BY updated_at DESC LIMIT 1")
        if not rows:
            return ""
        payload = json.loads(rows[0]["payload_json"])
        return str(payload.get("result", {}).get("summary", "") or "")

    @staticmethod
    def _selection_sort_key(block: ContextBlock):
        layer_order = {"structure": 0, "task": 1, "history": 2, "index": 3}
        return (layer_order.get(block.layer, 10), -block.score, block.token_estimate)
