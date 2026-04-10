from __future__ import annotations

import asyncio
import difflib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from ..settings import HarnessLabSettings
from ..types import ActionPlan, SandboxResult, SandboxSpec, SandboxStatus, SandboxTrace
from ..utils import compact_text, new_id, utc_now


class SandboxManager:
    """Docker-backed execution boundary for high-risk tools."""

    def __init__(self, settings: HarnessLabSettings, database: Any) -> None:
        self.settings = settings
        self.repo_root = Path(database.repo_root)
        self.artifact_root = Path(database.artifact_root)
        self.docker_bin = settings.docker_bin
        self.default_image = settings.sandbox_image
        self.default_timeout_seconds = settings.sandbox_timeout_seconds
        self.excluded_prefixes = [
            ".git",
            "frontend/node_modules",
            "frontend/dist",
            "backend/data",
            "__pycache__",
            ".pytest_cache",
        ]

    def requires_sandbox(self, action: ActionPlan) -> bool:
        if action.tool_name in {"shell", "git", "http_fetch"}:
            return True
        return action.tool_name == "filesystem" and action.payload.get("action") == "write_file"

    def sandbox_spec_for(self, action: ActionPlan, approval_token: Optional[str] = None) -> SandboxSpec:
        network_policy = "none"
        if action.tool_name == "http_fetch":
            network_policy = "restricted"
        return SandboxSpec(
            sandbox_mode="docker",
            image=self.default_image,
            workspace_mount="/workspace",
            working_dir="/workspace",
            network_policy=network_policy,  # type: ignore[arg-type]
            read_only_rootfs=True,
            timeout_seconds=self.default_timeout_seconds,
            approval_token=approval_token,
        )

    def status(self) -> SandboxStatus:
        docker_ready, docker_error = self._probe_docker_ready()
        image_ready = self._probe_image_ready() if docker_ready else False
        active_runs = self._active_container_count() if docker_ready else 0
        return SandboxStatus(
            sandbox_backend=self.settings.sandbox_backend,
            docker_ready=docker_ready,
            sandbox_image_ready=image_ready,
            sandbox_active_runs=active_runs,
            sandbox_failures=0,
            image=self.default_image,
            fallback_mode=not docker_ready or not image_ready,
            last_probe_error=docker_error if not docker_ready else (None if image_ready else f"Sandbox image missing: {self.default_image}"),
        )

    async def execute_action(
        self,
        action: ActionPlan,
        sandbox_spec: Optional[SandboxSpec] = None,
        approval_token: Optional[str] = None,
    ) -> SandboxResult:
        spec = sandbox_spec or self.sandbox_spec_for(action, approval_token=approval_token)
        before_state = self._workspace_state()
        started_at = utc_now()
        sandbox_id = new_id("sandbox")
        container_id = f"harness-lab-{sandbox_id}"

        if action.tool_name == "filesystem" and action.payload.get("action") == "write_file" and not approval_token:
            trace = SandboxTrace(
                sandbox_id=sandbox_id,
                sandbox_mode="docker",
                image=spec.image,
                container_id=container_id,
                network_policy=spec.network_policy,
                started_at=started_at,
                finished_at=utc_now(),
                timed_out=False,
                changed_paths=[],
                used_approval_token=False,
                exit_code=None,
                ok=False,
                error="Missing approval token for sandboxed filesystem mutation.",
                docker_command=[],
            )
            return SandboxResult(ok=False, sandbox_trace=trace, error=trace.error)

        docker_command, cleanup_dir = self._build_docker_command(action, spec, container_id)
        try:
            try:
                process = await asyncio.create_subprocess_exec(
                    *docker_command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            except Exception as exc:  # noqa: BLE001
                trace = SandboxTrace(
                    sandbox_id=sandbox_id,
                    sandbox_mode="docker",
                    image=spec.image,
                    container_id=container_id,
                    network_policy=spec.network_policy,
                    started_at=started_at,
                    finished_at=utc_now(),
                    timed_out=False,
                    changed_paths=[],
                    used_approval_token=bool(approval_token),
                    exit_code=None,
                    ok=False,
                    error=compact_text(str(exc), 1_000),
                    docker_command=docker_command,
                )
                return SandboxResult(ok=False, sandbox_trace=trace, error=trace.error)
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=max(1, spec.timeout_seconds + 2))
                timed_out = False
            except asyncio.TimeoutError:
                process.kill()
                stdout, stderr = await process.communicate()
                timed_out = True
        finally:
            if cleanup_dir:
                shutil.rmtree(cleanup_dir, ignore_errors=True)

        finished_at = utc_now()
        after_state = self._workspace_state()
        changed_paths = self._changed_paths(before_state, after_state)
        patch = self._build_patch(before_state, after_state, changed_paths)
        exit_code = None if timed_out else process.returncode
        parsed_output = self._parsed_output(action, stdout.decode("utf-8", errors="replace"))
        ok = (exit_code == 0) and not timed_out
        stderr_text = compact_text(stderr.decode("utf-8", errors="replace"), 4000)
        stdout_text = compact_text(stdout.decode("utf-8", errors="replace"), 4000)
        error = None if ok else ("Sandbox execution timed out" if timed_out else stderr_text or f"Command exited with {exit_code}")
        trace = SandboxTrace(
            sandbox_id=sandbox_id,
            sandbox_mode="docker",
            image=spec.image,
            container_id=container_id,
            network_policy=spec.network_policy,
            started_at=started_at,
            finished_at=finished_at,
            timed_out=timed_out,
            changed_paths=changed_paths,
            used_approval_token=bool(approval_token),
            exit_code=exit_code,
            ok=ok,
            error=error,
            docker_command=docker_command,
        )
        return SandboxResult(
            ok=ok,
            stdout=stdout_text,
            stderr=stderr_text,
            exit_code=exit_code,
            timed_out=timed_out,
            changed_paths=changed_paths,
            patch=patch,
            parsed_output=parsed_output,
            sandbox_trace=trace,
            error=error,
        )

    def _build_docker_command(
        self,
        action: ActionPlan,
        spec: SandboxSpec,
        container_id: str,
    ) -> tuple[list[str], Optional[Path]]:
        writable_workspace = action.tool_name == "filesystem" and action.payload.get("action") == "write_file"
        workspace_mount = f"type=bind,src={self.repo_root},dst={spec.workspace_mount}"
        if not writable_workspace:
            workspace_mount = f"{workspace_mount},readonly"
        command = [
            self.docker_bin,
            "run",
            "--rm",
            "--name",
            container_id,
            "--label",
            "harness-lab.sandbox=1",
            "--workdir",
            spec.working_dir,
            "--mount",
            workspace_mount,
        ]
        if spec.read_only_rootfs:
            command.append("--read-only")
            command.extend(["--tmpfs", "/tmp"])
        if spec.network_policy == "none":
            command.extend(["--network", "none"])
        else:
            command.extend(["--network", "bridge"])

        cleanup_dir: Optional[Path] = None
        tool_command: list[str]
        if action.tool_name == "shell":
            tool_command = ["sh", "-lc", str(action.payload.get("command", ""))]
        elif action.tool_name == "git":
            git_action = str(action.payload.get("action", "status") or "status")
            git_command_map = {
                "status": "git status --short",
                "diff": "git diff --stat",
                "log": "git log --oneline -5",
            }
            tool_command = ["sh", "-lc", git_command_map.get(git_action, "git status --short")]
        elif action.tool_name == "http_fetch":
            url = str(action.payload.get("url", "") or "")
            python_script = (
                "import json,sys,urllib.request;"
                "url=sys.argv[1];"
                "with urllib.request.urlopen(url, timeout=10) as r:"
                " body=r.read(8000).decode('utf-8', errors='replace');"
                " print(json.dumps({'url': url, 'status': getattr(r, 'status', 200), 'body': body}))"
            )
            tool_command = ["python", "-c", python_script, url]
        elif action.tool_name == "filesystem" and action.payload.get("action") == "write_file":
            cleanup_dir = Path(tempfile.mkdtemp(prefix="hlab-sandbox-"))
            payload_file = cleanup_dir / "content.txt"
            payload_file.write_text(str(action.payload.get("content", "")), encoding="utf-8")
            command.extend(
                [
                    "--mount",
                    f"type=bind,src={cleanup_dir},dst=/sandbox-input,readonly",
                ]
            )
            target = str(action.payload.get("path", "") or "")
            python_script = (
                "from pathlib import Path;import sys;"
                "target=Path(sys.argv[1]);"
                "content=Path('/sandbox-input/content.txt').read_text(encoding='utf-8');"
                "target.parent.mkdir(parents=True, exist_ok=True);"
                "target.write_text(content, encoding='utf-8');"
                "print(target.as_posix())"
            )
            tool_command = ["python", "-c", python_script, target]
        else:
            tool_command = ["sh", "-lc", "printf 'unsupported sandbox action' >&2; exit 2"]

        command.append(spec.image)
        command.extend(tool_command)
        return command, cleanup_dir

    def _workspace_state(self) -> Dict[str, Dict[str, str]]:
        state: Dict[str, Dict[str, str]] = {}
        for path in self.repo_root.rglob("*"):
            if not path.is_file():
                continue
            relative = str(path.relative_to(self.repo_root))
            if any(relative == prefix or relative.startswith(prefix + "/") for prefix in self.excluded_prefixes):
                continue
            if path.stat().st_size > 200_000:
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            state[relative] = {
                "content": content,
                "digest": str(hash(content)),
            }
        return state

    @staticmethod
    def _changed_paths(before_state: Dict[str, Dict[str, str]], after_state: Dict[str, Dict[str, str]]) -> list[str]:
        changed: list[str] = []
        for relative in sorted(set(before_state) | set(after_state)):
            if before_state.get(relative, {}).get("digest") != after_state.get(relative, {}).get("digest"):
                changed.append(relative)
        return changed

    @staticmethod
    def _build_patch(
        before_state: Dict[str, Dict[str, str]],
        after_state: Dict[str, Dict[str, str]],
        changed_paths: list[str],
    ) -> str:
        segments: list[str] = []
        for relative in changed_paths[:20]:
            before = before_state.get(relative, {}).get("content", "")
            after = after_state.get(relative, {}).get("content", "")
            segments.append(
                "\n".join(
                    difflib.unified_diff(
                        before.splitlines(),
                        after.splitlines(),
                        fromfile=relative,
                        tofile=relative,
                        lineterm="",
                    )
                )
            )
        return "\n\n".join(segment for segment in segments if segment).strip()

    @staticmethod
    def _parsed_output(action: ActionPlan, stdout: str) -> Dict[str, Any]:
        if action.tool_name == "http_fetch":
            try:
                return json.loads(stdout)
            except Exception:  # noqa: BLE001
                return {}
        return {}

    def _probe_docker_ready(self) -> tuple[bool, Optional[str]]:
        if shutil.which(self.docker_bin) is None:
            return False, f"Docker binary not found: {self.docker_bin}"
        try:
            result = subprocess.run(
                [self.docker_bin, "version", "--format", "{{.Server.Version}}"],
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        return result.returncode == 0, None if result.returncode == 0 else compact_text(result.stderr, 1_000)

    def _probe_image_ready(self) -> bool:
        try:
            result = subprocess.run(
                [self.docker_bin, "image", "inspect", self.default_image],
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception:  # noqa: BLE001
            return False
        return result.returncode == 0

    def _active_container_count(self) -> int:
        try:
            result = subprocess.run(
                [self.docker_bin, "ps", "-q", "--filter", "label=harness-lab.sandbox=1"],
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception:  # noqa: BLE001
            return 0
        if result.returncode != 0:
            return 0
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        return len(lines)
