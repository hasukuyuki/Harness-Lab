from __future__ import annotations

import asyncio
import difflib
import hashlib
import json
import shlex
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from .sandbox import SandboxManager
from ..constraints.engine import ConstraintEngine
from ..knowledge.service import KnowledgeIndexService
from ..storage import HarnessLabDatabase
from ..types import (
    ActionPlan,
    ArtifactRef,
    KnowledgeSearchResult,
    PolicyVerdict,
    SandboxSpec,
    SandboxStatus,
    ToolDescriptor,
    ToolExecutionResult,
)
from ..utils import compact_text, ensure_parent, safe_preview, score_overlap


class ToolGateway:
    """Single-user workspace boundary with preflight-aware tool execution."""

    def __init__(
        self,
        database: HarnessLabDatabase,
        constraints: ConstraintEngine,
        knowledge_index: Optional[KnowledgeIndexService] = None,
        sandbox_manager: Optional[SandboxManager] = None,
    ) -> None:
        self.database = database
        self.constraints = constraints
        self.knowledge_index = knowledge_index
        self.sandbox_manager = sandbox_manager
        self.repo_root = database.repo_root
        self._allowed_commands = frozenset([
            "ls", "cat", "git", "pwd", "echo", "find", "grep", "head", "tail",
            "wc", "sort", "uniq", "diff", "mkdir", "touch", "rm", "mv", "cp",
        ])
        self._descriptors = [
            ToolDescriptor(
                name="shell",
                description="Execute shell commands inside the local workspace",
                risk_level="high",
                timeout_ms=20_000,
                side_effect_class="process_and_filesystem",
                input_schema={"type": "object", "properties": {"command": {"type": "string"}}},
            ),
            ToolDescriptor(
                name="filesystem",
                description="Read, list, or write workspace files",
                risk_level="medium",
                timeout_ms=5_000,
                side_effect_class="filesystem",
                input_schema={"type": "object", "properties": {"action": {"type": "string"}, "path": {"type": "string"}}},
            ),
            ToolDescriptor(
                name="git",
                description="Inspect local git state",
                risk_level="low",
                timeout_ms=10_000,
                side_effect_class="vcs_read",
                input_schema={"type": "object", "properties": {"action": {"type": "string"}}},
            ),
            ToolDescriptor(
                name="http_fetch",
                description="Perform HTTP GET for research traces",
                risk_level="medium",
                timeout_ms=10_000,
                side_effect_class="network_read",
                input_schema={"type": "object", "properties": {"url": {"type": "string"}}},
            ),
            ToolDescriptor(
                name="knowledge_search",
                description="Search repository content for relevant context",
                risk_level="low",
                timeout_ms=6_000,
                side_effect_class="knowledge_read",
                input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
            ),
            ToolDescriptor(
                name="model_reflection",
                description="Produce local research reflections without changing the workspace",
                risk_level="low",
                timeout_ms=2_000,
                side_effect_class="local_reasoning",
                input_schema={"type": "object", "properties": {"prompt": {"type": "string"}}},
            ),
        ]
        self.excluded_prefixes = [
            ".git",
            "backend/tests",
            "frontend/node_modules",
            "frontend/dist",
            "backend/data",
            "__pycache__",
            ".pytest_cache",
        ]

    def list_tools(self) -> List[ToolDescriptor]:
        return self._descriptors

    def preflight(self, action: ActionPlan, constraint_set_id: str) -> Dict[str, Any]:
        """Run preflight checks and return verdicts with explanation.
        
        Returns a dict containing:
        - verdicts: List of PolicyVerdict
        - final_verdict: The consolidated verdict
        - explanation: Detailed ConstraintExplanation
        - used_fallback: Whether fallback logic was used
        """
        response = self.constraints.verify(
            subject=action.subject,
            payload=action.payload,
            constraint_set_id=constraint_set_id,
        )
        return {
            "verdicts": response.verdicts,
            "final_verdict": response.final_verdict,
            "explanation": response.explanation,
            "used_fallback": response.used_fallback,
            "compiled_rule_count": response.compiled_rule_count,
            "matched_rules": response.matched_rules,
        }

    def model_reflection_result(self, reflection: Dict[str, Any]) -> ToolExecutionResult:
        return ToolExecutionResult(ok=True, output=reflection)

    def requires_sandbox(self, action: ActionPlan) -> bool:
        if self.sandbox_manager is None:
            return action.tool_name in {"shell", "git", "http_fetch"} or (
                action.tool_name == "filesystem" and action.payload.get("action") == "write_file"
            )
        return self.sandbox_manager.requires_sandbox(action)

    def sandbox_status(self) -> Optional[SandboxStatus]:
        if self.sandbox_manager is None:
            return None
        return self.sandbox_manager.status()

    def sandbox_spec_for(self, action: ActionPlan, approval_token: Optional[str] = None) -> Optional[SandboxSpec]:
        if self.sandbox_manager is None:
            return None
        return self.sandbox_manager.sandbox_spec_for(action, approval_token=approval_token)

    def create_snapshot_manifest(self, run_id: str) -> ArtifactRef:
        manifest = {}
        for path in self._iter_workspace_files(limit=200):
            relative = str(path.relative_to(self.repo_root))
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest[relative] = digest
        return self.database.write_artifact_text(
            run_id=run_id,
            artifact_type="snapshot_manifest",
            filename="workspace_manifest.json",
            content=json.dumps(manifest, ensure_ascii=False, indent=2),
            metadata={"file_count": len(manifest)},
        )

    async def execute(self, run_id: str, action: ActionPlan) -> ToolExecutionResult:
        if action.tool_name == "shell":
            if self.sandbox_manager is not None:
                return await self._run_sandboxed(run_id, action)
            return await self._run_shell(action.payload)
        if action.tool_name == "filesystem":
            if action.payload.get("action") == "write_file" and self.sandbox_manager is not None:
                return await self._run_sandboxed(run_id, action)
            return await self._run_filesystem(run_id, action.payload)
        if action.tool_name == "git":
            if self.sandbox_manager is not None:
                return await self._run_sandboxed(run_id, action)
            return await self._run_git(action.payload)
        if action.tool_name == "http_fetch":
            if self.sandbox_manager is not None:
                return await self._run_sandboxed(run_id, action)
            return await self._run_http_fetch(action.payload)
        if action.tool_name == "knowledge_search":
            payload = dict(action.payload)
            payload.setdefault("_run_id", run_id)
            return self._knowledge_search(payload)
        if action.tool_name == "model_reflection":
            return self.model_reflection_result(
                {
                    "summary": compact_text(str(action.payload.get("prompt", "")), 240),
                    "research_notes": [
                        "Harness Lab prefers structured prompt frames over raw prompt stuffing.",
                        "Policy verdicts should be visible before execution.",
                        "Replay artifacts are first-class research outputs.",
                    ],
                    "details": {"fallback_mode": True},
                }
            )
        return ToolExecutionResult(ok=False, error=f"Unsupported tool: {action.tool_name}")

    async def _run_sandboxed(self, run_id: str, action: ActionPlan) -> ToolExecutionResult:
        if self.sandbox_manager is None:
            return ToolExecutionResult(ok=False, error="Sandbox manager is not configured")
        approval_token = str(action.payload.get("_approval_token", "") or "") or None
        raw_spec = action.payload.get("_sandbox_spec")
        sandbox_spec = SandboxSpec.model_validate(raw_spec) if isinstance(raw_spec, dict) else None
        before_content: Optional[str] = None
        if action.tool_name == "filesystem" and action.payload.get("action") == "write_file":
            raw_path = str(action.payload.get("path", ".") or ".")
            path = self.resolve_path(raw_path)
            before_content = path.read_text(encoding="utf-8") if path.exists() and path.is_file() else ""
        result = await self.sandbox_manager.execute_action(
            action,
            sandbox_spec=sandbox_spec,
            approval_token=approval_token,
        )
        if action.tool_name == "filesystem" and action.payload.get("action") == "write_file":
            return self._sandboxed_write_result(run_id, action.payload, result, before_content or "")
        return self._sandboxed_tool_result(run_id, action, result)

    async def _run_shell(self, payload: Dict[str, Any]) -> ToolExecutionResult:
        command = str(payload.get("command", "") or "")
        if not command:
            return ToolExecutionResult(ok=False, error="Shell command is required")
        
        # Parse command to extract the base command name
        try:
            args = shlex.split(command)
        except ValueError as e:
            return ToolExecutionResult(ok=False, error=f"Invalid shell command syntax: {e}")
        
        if not args:
            return ToolExecutionResult(ok=False, error="Shell command is empty after parsing")
        
        base_cmd = args[0]
        if base_cmd not in self._allowed_commands:
            return ToolExecutionResult(
                ok=False,
                error=f"Command '{base_cmd}' is not allowed. Allowed commands: {', '.join(sorted(self._allowed_commands))}"
            )
        
        # Use create_subprocess_exec for safer execution (no shell interpretation)
        process = await asyncio.create_subprocess_exec(
            *args,
            cwd=str(self.repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
        except asyncio.TimeoutError:
            process.kill()
            return ToolExecutionResult(ok=False, error="Shell command timed out")
        return ToolExecutionResult(
            ok=process.returncode == 0,
            error=None if process.returncode == 0 else f"Command exited with {process.returncode}",
            output={
                "command": command,
                "stdout": compact_text(stdout.decode("utf-8", errors="replace"), 4_000),
                "stderr": compact_text(stderr.decode("utf-8", errors="replace"), 4_000),
                "exit_code": process.returncode,
            },
        )

    async def _run_filesystem(self, run_id: str, payload: Dict[str, Any]) -> ToolExecutionResult:
        action = payload.get("action")
        raw_path = str(payload.get("path", ".") or ".")
        path = self.resolve_path(raw_path)
        if action == "list_dir":
            entries = [
                {
                    "name": child.name,
                    "path": str(child.relative_to(self.repo_root)),
                    "type": "directory" if child.is_dir() else "file",
                }
                for child in sorted(path.iterdir(), key=lambda item: item.name)
            ]
            return ToolExecutionResult(ok=True, output={"entries": entries, "path": str(path.relative_to(self.repo_root))})
        if action == "read_file":
            content = path.read_text(encoding="utf-8")
            return ToolExecutionResult(
                ok=True,
                output={"path": str(path.relative_to(self.repo_root)), "content": compact_text(content, 6_000)},
            )
        if action == "write_file":
            before = path.read_text(encoding="utf-8") if path.exists() and path.is_file() else ""
            after = str(payload.get("content", ""))
            patch = "\n".join(
                difflib.unified_diff(
                    before.splitlines(),
                    after.splitlines(),
                    fromfile=str(path),
                    tofile=str(path),
                    lineterm="",
                )
            )
            patch_artifact = self.database.write_artifact_text(
                run_id=run_id,
                artifact_type="patch_stage",
                filename=f"{path.name}.diff",
                content=patch or "[no diff]",
                metadata={"target_path": str(path.relative_to(self.repo_root))},
            )
            backup_artifact = self.database.write_artifact_text(
                run_id=run_id,
                artifact_type="file_backup",
                filename=f"{path.name}.bak",
                content=before,
                metadata={"target_path": str(path.relative_to(self.repo_root))},
            )
            ensure_parent(path)
            path.write_text(after, encoding="utf-8")
            return ToolExecutionResult(
                ok=True,
                output={
                    "written_path": str(path.relative_to(self.repo_root)),
                    "patch_artifact_id": patch_artifact.artifact_id,
                    "backup_artifact_id": backup_artifact.artifact_id,
                },
            )
        return ToolExecutionResult(ok=False, error=f"Unsupported filesystem action: {action}")

    async def _run_git(self, payload: Dict[str, Any]) -> ToolExecutionResult:
        action = str(payload.get("action", "status") or "status")
        command_map = {
            "status": "git status --short",
            "diff": "git diff --stat",
            "log": "git log --oneline -5",
        }
        command = command_map.get(action)
        if not command:
            return ToolExecutionResult(ok=False, error=f"Unsupported git action: {action}")
        return await self._run_shell({"command": command})

    async def _run_http_fetch(self, payload: Dict[str, Any]) -> ToolExecutionResult:
        url = str(payload.get("url", "") or "")
        if not url:
            return ToolExecutionResult(ok=False, error="URL is required")
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                body = response.read(8_000).decode("utf-8", errors="replace")
            return ToolExecutionResult(
                ok=True,
                output={
                    "url": url,
                    "status": getattr(response, "status", 200),
                    "body": compact_text(body, 4_000),
                },
            )
        except Exception as exc:  # noqa: BLE001
            return ToolExecutionResult(ok=False, error=str(exc))

    def _sandboxed_tool_result(self, run_id: str, action: ActionPlan, result) -> ToolExecutionResult:
        output: Dict[str, Any]
        if action.tool_name == "http_fetch":
            output = result.parsed_output or {"url": action.payload.get("url")}
            output.setdefault("url", str(action.payload.get("url", "") or ""))
        elif action.tool_name == "git":
            output = {
                "action": str(action.payload.get("action", "status") or "status"),
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
            }
        else:
            output = {
                "command": str(action.payload.get("command", "") or ""),
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
            }
        output["sandbox_trace"] = result.sandbox_trace.model_dump()
        output["changed_paths"] = result.changed_paths
        if result.patch:
            patch_artifact = self.database.write_artifact_text(
                run_id=run_id,
                artifact_type="patch_stage",
                filename=f"{action.tool_name}.diff",
                content=result.patch,
                metadata={
                    "tool_name": action.tool_name,
                    "changed_paths": result.changed_paths,
                    "sandbox_id": result.sandbox_trace.sandbox_id,
                },
            )
            output["patch_artifact_id"] = patch_artifact.artifact_id
        return ToolExecutionResult(ok=result.ok, output=output, error=result.error)

    def _sandboxed_write_result(self, run_id: str, payload: Dict[str, Any], result, before: str) -> ToolExecutionResult:
        raw_path = str(payload.get("path", ".") or ".")
        path = self.resolve_path(raw_path)
        target_path = str(path.relative_to(self.repo_root))
        patch_artifact = self.database.write_artifact_text(
            run_id=run_id,
            artifact_type="patch_stage",
            filename=f"{path.name}.diff",
            content=result.patch or "[no diff]",
            metadata={
                "target_path": target_path,
                "changed_paths": result.changed_paths,
                "sandbox_id": result.sandbox_trace.sandbox_id,
            },
        )
        backup_artifact = self.database.write_artifact_text(
            run_id=run_id,
            artifact_type="file_backup",
            filename=f"{path.name}.bak",
            content=before,
            metadata={"target_path": target_path},
        )
        return ToolExecutionResult(
            ok=result.ok,
            error=result.error,
            output={
                "written_path": target_path,
                "patch_artifact_id": patch_artifact.artifact_id,
                "backup_artifact_id": backup_artifact.artifact_id,
                "sandbox_trace": result.sandbox_trace.model_dump(),
                "changed_paths": result.changed_paths,
                "exit_code": result.exit_code,
            },
        )

    def _knowledge_search(self, payload: Dict[str, Any]) -> ToolExecutionResult:
        query = str(payload.get("query", "") or "")
        if not query:
            return ToolExecutionResult(ok=False, error="Query is required")
        top_k = max(1, int(payload.get("top_k", 8) or 8))
        raw_source_types = payload.get("source_types") or []
        source_types = [str(item) for item in raw_source_types if str(item).strip()]
        path_hint = str(payload.get("path_hint", "") or "")
        result = self.knowledge_search(query=query, top_k=top_k, path_hint=path_hint or None, source_types=source_types)
        artifact = self.database.write_artifact_text(
            run_id=str(payload.get("run_id") or payload.get("_run_id") or ""),
            artifact_type="knowledge_search_results",
            filename="execution_search_results.json",
            content=json.dumps(result.model_dump(), ensure_ascii=False, indent=2),
            metadata={"query": query, "path_hint": path_hint or None},
        ) if str(payload.get("run_id") or payload.get("_run_id") or "").strip() else None
        output = result.model_dump()
        output["results"] = output["hits"]
        if artifact:
            output["artifact_id"] = artifact.artifact_id
        return ToolExecutionResult(ok=True, output=output)

    def knowledge_search(
        self,
        query: str,
        top_k: int = 8,
        path_hint: Optional[str] = None,
        source_types: Optional[List[str]] = None,
    ) -> KnowledgeSearchResult:
        if self.knowledge_index is not None:
            return self.knowledge_index.search(
                query=query,
                top_k=top_k,
                path_hint=path_hint,
                source_types=source_types or None,
            )
        candidates = []
        for path in self._iter_workspace_files(limit=200):
            relative = str(path.relative_to(self.repo_root))
            preview = safe_preview(path, 900)
            score = score_overlap(query, f"{relative}\n{preview}")
            if score <= 0:
                continue
            candidates.append(
                {
                    "chunk_id": relative,
                    "source_type": "workspace",
                    "source_ref": f"file://{relative}",
                    "title": relative,
                    "snippet": preview,
                    "score": round(score, 3),
                    "metadata": {"path": relative},
                }
            )
        candidates.sort(key=lambda item: item["score"], reverse=True)
        return KnowledgeSearchResult.model_validate(
            {
                "query": query,
                "top_k": top_k,
                "path_hint": path_hint,
                "source_types": source_types or [],
                "hits": candidates[:top_k],
                "used_fallback": True,
                "source_coverage": {"workspace": len(candidates[:top_k])} if candidates else {},
                "status": {"ready": False, "fallback_mode": True, "model_name": None},
            }
        )

    def resolve_path(self, raw_path: str) -> Path:
        target = (self.repo_root / raw_path).resolve() if not Path(raw_path).is_absolute() else Path(raw_path).resolve()
        if self.repo_root not in target.parents and target != self.repo_root:
            raise ValueError("Path escapes the workspace root")
        return target

    def _iter_workspace_files(self, limit: Optional[int] = None):
        count = 0
        for path in self.repo_root.rglob("*"):
            if not path.is_file():
                continue
            relative = str(path.relative_to(self.repo_root))
            if any(relative == prefix or relative.startswith(prefix + "/") for prefix in self.excluded_prefixes):
                continue
            if path.name.startswith(".env"):
                continue
            if path.stat().st_size > 120_000:
                continue
            yield path
            count += 1
            if limit and count >= limit:
                break
