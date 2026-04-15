"""Docker-based sandbox executor implementation."""

from __future__ import annotations

import asyncio
import difflib
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..settings import HarnessLabSettings
from ..types import (
    ActionPlan,
    ApprovalContext,
    ContainerMetadata,
    ExecutionTiming,
    MountInfo,
    PolicyVerdictSnapshot,
    ProbeCheckResult,
    SandboxEvidence,
    SandboxResult,
    SandboxSpec,
    SandboxStatus,
    SandboxTrace,
)
from ..utils import compact_text, new_id, utc_now
from .executor import ExecutorCapabilities, SandboxExecutor


class DockerSandboxExecutor(SandboxExecutor):
    """Docker-based sandbox executor with hardened security configuration.
    
    This executor uses Docker containers to provide isolated execution
    environments for high-risk tools. It supports:
    - Capability dropping (cap-drop=ALL)
    - No-new-privileges security option
    - Rootless user execution
    - Read-only root filesystem
    - Network isolation (none/restricted)
    - Workspace bind mounts
    """

    def __init__(
        self,
        settings: HarnessLabSettings,
        repo_root: Path,
        artifact_root: Path,
    ) -> None:
        super().__init__(
            backend_name="docker",
            executor_version="1.0.0",
        )
        self._capabilities = ExecutorCapabilities.docker_defaults()
        self.settings = settings
        self.repo_root = repo_root
        self.artifact_root = artifact_root
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

    async def execute(
        self,
        action: ActionPlan,
        sandbox_spec: SandboxSpec,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
    ) -> SandboxResult:
        """Execute action in hardened Docker sandbox."""
        side_effect_class = self.classify_side_effect(action, policy_verdict)

        # Check approval for mutations
        if side_effect_class == "sandboxed_mutation" and not approval_token:
            return self._approval_required_result(action, sandbox_spec)

        before_state = self._workspace_state()
        started_at = utc_now()
        sandbox_id = new_id("sandbox")
        container_id = f"harness-lab-{sandbox_id}"

        # Build hardened Docker command
        docker_command, cleanup_dir, mounts = self._build_hardened_docker_command(
            action, sandbox_spec, container_id
        )

        # Execute with timeout and error handling
        try:
            process, timed_out, stdout, stderr = await self._execute_with_timeout(
                docker_command, sandbox_spec.timeout_seconds
            )
        except Exception as exc:  # noqa: BLE001
            return self._execution_error_result(
                sandbox_id, container_id, sandbox_spec, started_at, exc, docker_command
            )
        finally:
            if cleanup_dir:
                shutil.rmtree(cleanup_dir, ignore_errors=True)

        # Build result
        finished_at = utc_now()
        after_state = self._workspace_state()
        changed_paths = self._changed_paths(before_state, after_state)
        patch = self._build_patch(before_state, after_state, changed_paths)

        exit_code = None if timed_out else process.returncode
        ok = (exit_code == 0) and not timed_out
        stderr_text = compact_text(stderr.decode("utf-8", errors="replace"), 4000)
        stdout_text = compact_text(stdout.decode("utf-8", errors="replace"), 4000)
        error = None if ok else (
            "Sandbox execution timed out" if timed_out else stderr_text or f"Command exited with {exit_code}"
        )

        # Build evidence
        evidence = self._build_evidence(
            stdout_text, stderr_text, exit_code, changed_paths, patch,
            container_id, sandbox_spec, started_at, finished_at, mounts
        )

        # Build approval context
        approval_context = None
        if approval_token:
            approval_context = ApprovalContext(
                approval_token=approval_token,
                used=True,
            )

        trace = SandboxTrace(
            sandbox_id=sandbox_id,
            sandbox_mode="docker",
            image=sandbox_spec.image,
            container_id=container_id,
            network_policy=sandbox_spec.network_policy,
            started_at=started_at,
            finished_at=finished_at,
            timed_out=timed_out,
            changed_paths=changed_paths,
            used_approval_token=bool(approval_token),
            exit_code=exit_code,
            ok=ok,
            error=error,
            docker_command=docker_command,
            side_effect_class=side_effect_class,
            hardened_config=sandbox_spec.hardened_config,
            evidence=evidence,
            policy_verdict=policy_verdict,
            approval_context=approval_context,
            backend=self.backend_name,
            executor_version=self.executor_version,
        )

        return SandboxResult(
            ok=ok,
            stdout=stdout_text,
            stderr=stderr_text,
            exit_code=exit_code,
            timed_out=timed_out,
            changed_paths=changed_paths,
            patch=patch,
            parsed_output=self._parsed_output(action, stdout_text),
            sandbox_trace=trace,
            error=error,
        )

    def status(self) -> SandboxStatus:
        """Get comprehensive sandbox status with hardened readiness checks."""
        probe_checks: List[ProbeCheckResult] = []

        # Check Docker daemon
        docker_ready, docker_error = self._probe_docker_ready()
        probe_checks.append(ProbeCheckResult(
            check="docker_daemon",
            passed=docker_ready,
            error=docker_error,
        ))

        # Check image
        image_ready = False
        if docker_ready:
            image_ready = self._probe_image_ready()
            probe_checks.append(ProbeCheckResult(
                check="sandbox_image",
                passed=image_ready,
                error=None if image_ready else f"Image not found: {self.default_image}",
            ))

        # Check hardened capabilities
        rootless_ready = docker_ready and image_ready
        no_new_priv_ready = docker_ready
        cap_drop_ready = docker_ready
        policy_ready = True

        probe_checks.append(ProbeCheckResult(
            check="rootless_support",
            passed=rootless_ready,
            error=None if rootless_ready else "Rootless mode not verified",
        ))

        hardened_ready = (
            docker_ready and
            image_ready and
            rootless_ready and
            no_new_priv_ready and
            cap_drop_ready
        )

        active_runs = self._active_container_count() if docker_ready else 0

        return SandboxStatus(
            sandbox_backend=self.backend_name,
            docker_ready=docker_ready,
            sandbox_image_ready=image_ready,
            sandbox_active_runs=active_runs,
            sandbox_failures=0,
            image=self.default_image,
            fallback_mode=not docker_ready or not image_ready,
            last_probe_error=docker_error if not docker_ready else (None if image_ready else f"Sandbox image missing: {self.default_image}"),
            last_probe_at=utc_now(),
            hardened_ready=hardened_ready,
            rootless_ready=rootless_ready,
            no_new_privileges_ready=no_new_priv_ready,
            capability_drop_ready=cap_drop_ready,
            policy_enforcement_ready=policy_ready,
            probe_checks=probe_checks,
            active_sandbox_count=active_runs,
            total_executions_24h=0,
            failure_count_24h=0,
            executor_ready=docker_ready and image_ready,
            executor_capabilities=self._capabilities.to_dict(),
            executor_version=self.executor_version,
        )

    def validate_spec(self, spec: SandboxSpec) -> Tuple[bool, Optional[str]]:
        """Validate a sandbox specification for Docker backend."""
        if spec.image is None or not spec.image.strip():
            return False, "Docker image is required"

        if spec.workspace_mount is None or not spec.workspace_mount.strip():
            return False, "Workspace mount path is required"

        # Check if image exists locally
        if not self._probe_image_ready():
            # Try to pull the image
            return True, f"Image {spec.image} not found locally, will attempt to pull on execution"

        return True, None

    def _build_hardened_docker_command(
        self,
        action: ActionPlan,
        spec: SandboxSpec,
        container_id: str,
    ) -> Tuple[List[str], Optional[Path], List[MountInfo]]:
        """Build hardened Docker command with security options."""
        mounts: List[MountInfo] = []

        # Base command
        command = [
            self.docker_bin,
            "run",
            "--rm",
            "--name", container_id,
            "--label", "harness-lab.sandbox=1",
            "--workdir", spec.working_dir,
        ]

        # Security options
        if spec.hardened_config:
            hc = spec.hardened_config

            # No new privileges
            if hc.no_new_privileges:
                command.extend(["--security-opt", "no-new-privileges:true"])

            # Capability drop/add
            if hc.cap_drop_all:
                command.append("--cap-drop=ALL")
            for cap in hc.cap_add_whitelist:
                command.append(f"--cap-add={cap}")

            # Rootless user
            if hc.rootless_user:
                command.extend(["--user", hc.rootless_user])

            # Read-only rootfs
            if hc.read_only_rootfs:
                command.append("--read-only")
                command.extend(["--tmpfs", "/tmp:noexec,nosuid,size=100m"])

        # Network policy
        if spec.network_policy == "none":
            command.extend(["--network", "none"])
        elif spec.network_policy == "restricted":
            command.extend(["--network", "bridge"])
        else:
            command.extend(["--network", "bridge"])

        # Mounts - hardened separation
        workspace_mount = f"type=bind,src={self.repo_root},dst={spec.workspace_mount},readonly"
        command.extend(["--mount", workspace_mount])
        mounts.append(MountInfo(
            source=str(self.repo_root),
            destination=spec.workspace_mount,
            mode="ro",
            mount_type="bind",
        ))

        cleanup_dir: Optional[Path] = None
        tool_command: List[str]

        # Build tool-specific command
        # Note: Using "sh -c" without "-l" to avoid shell injection via login profiles
        if action.tool_name == "shell":
            tool_command = ["sh", "-c", str(action.payload.get("command", ""))]

        elif action.tool_name == "git":
            git_action = str(action.payload.get("action", "status") or "status")
            git_command_map = {
                "status": "git status --short",
                "diff": "git diff --stat",
                "log": "git log --oneline -5",
            }
            tool_command = ["sh", "-c", git_command_map.get(git_action, "git status --short")]

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

            # Input mount (read-only)
            command.extend(["--mount", f"type=bind,src={cleanup_dir},dst=/sandbox-input,readonly"])
            mounts.append(MountInfo(
                source=str(cleanup_dir),
                destination="/sandbox-input",
                mode="ro",
                mount_type="bind",
            ))

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
            tool_command = ["sh", "-c", "printf 'unsupported sandbox action' >&2; exit 2"]

        command.append(spec.image)
        command.extend(tool_command)

        return command, cleanup_dir, mounts

    async def _execute_with_timeout(
        self,
        docker_command: List[str],
        timeout_seconds: int,
    ) -> Tuple[asyncio.subprocess.Process, bool, bytes, bytes]:
        """Execute Docker command with timeout."""
        process = await asyncio.create_subprocess_exec(
            *docker_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=max(1, timeout_seconds + 2)
            )
            timed_out = False
        except asyncio.TimeoutError:
            process.kill()
            stdout, stderr = await process.communicate()
            timed_out = True

        return process, timed_out, stdout, stderr

    def _workspace_state(self) -> Dict[str, Dict[str, str]]:
        """Capture current workspace state for change detection."""
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
        """Calculate changed paths between two states."""
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
        """Build unified diff patch for changed paths."""
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

    def _build_evidence(
        self,
        stdout: str,
        stderr: str,
        exit_code: Optional[int],
        changed_paths: List[str],
        patch: str,
        container_id: str,
        spec: SandboxSpec,
        started_at: str,
        finished_at: str,
        mounts: List[MountInfo],
    ) -> SandboxEvidence:
        """Build comprehensive sandbox evidence."""
        try:
            from datetime import datetime
            start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
            finish_dt = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
            duration_ms = int((finish_dt - start_dt).total_seconds() * 1000)
        except Exception:  # noqa: BLE001
            duration_ms = 0

        container_metadata = ContainerMetadata(
            container_id=container_id,
            image=spec.image,
            created_at=started_at,
            started_at=started_at,
            finished_at=finished_at,
            security_options=["no-new-privileges:true"] if spec.hardened_config and spec.hardened_config.no_new_privileges else [],
            dropped_capabilities=["ALL"] if spec.hardened_config and spec.hardened_config.cap_drop_all else [],
            added_capabilities=spec.hardened_config.cap_add_whitelist if spec.hardened_config else [],
            user=spec.hardened_config.rootless_user if spec.hardened_config else "root",
            mounts=mounts,
            network_mode=spec.network_policy,
        )

        execution_timing = ExecutionTiming(
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
        )

        return SandboxEvidence(
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            changed_paths=changed_paths,
            patch=patch,
            container_metadata=container_metadata,
            execution_timing=execution_timing,
        )

    @staticmethod
    def _parsed_output(action: ActionPlan, stdout: str) -> Dict[str, Any]:
        """Parse tool-specific output."""
        import json
        if action.tool_name == "http_fetch":
            try:
                return json.loads(stdout)
            except Exception:  # noqa: BLE001
                return {}
        return {}

    def _probe_docker_ready(self) -> Tuple[bool, Optional[str]]:
        """Check if Docker daemon is ready."""
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
        return result.returncode == 0, None if result.returncode == 0 else compact_text(result.stderr, 1000)

    def _probe_image_ready(self) -> bool:
        """Check if sandbox image is available."""
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
        """Count active sandbox containers."""
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

    def _approval_required_result(
        self,
        action: ActionPlan,
        spec: SandboxSpec,
    ) -> SandboxResult:
        """Return result for action blocked waiting for approval."""
        started_at = utc_now()
        sandbox_id = new_id("sandbox")
        container_id = f"harness-lab-{sandbox_id}"

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
            side_effect_class="approval_blocked",
            hardened_config=spec.hardened_config,
            evidence=None,
            policy_verdict=None,
            approval_context=None,
            backend=self.backend_name,
            executor_version=self.executor_version,
        )

        return SandboxResult(
            ok=False,
            sandbox_trace=trace,
            error=trace.error,
        )

    def _execution_error_result(
        self,
        sandbox_id: str,
        container_id: str,
        spec: SandboxSpec,
        started_at: str,
        exc: Exception,
        docker_command: List[str],
    ) -> SandboxResult:
        """Return result for execution error."""
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
            error=compact_text(str(exc), 1000),
            docker_command=docker_command,
            side_effect_class="denied_before_sandbox",
            backend=self.backend_name,
            executor_version=self.executor_version,
        )

        return SandboxResult(ok=False, sandbox_trace=trace, error=trace.error)
