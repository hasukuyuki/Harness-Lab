"""Local MicroVM-style sandbox executor implementation."""

from __future__ import annotations

import asyncio
import difflib
import json
import os
import shutil
import sys
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


class MicroVMSandboxExecutor(SandboxExecutor):
    """Firecracker-style local runner that exposes a stable `microvm` backend."""

    def __init__(
        self,
        settings: HarnessLabSettings,
        repo_root: Path,
        artifact_root: Path,
    ) -> None:
        super().__init__(backend_name="microvm", executor_version="0.1.0")
        self._capabilities = ExecutorCapabilities.microvm_defaults()
        self.settings = settings
        self.repo_root = repo_root
        self.artifact_root = artifact_root
        self.microvm_binary = settings.microvm_binary
        self.kernel_image = settings.microvm_kernel_image
        self.rootfs_image = settings.microvm_rootfs_image
        self.microvm_timeout_seconds = settings.microvm_timeout_seconds
        self.enable_snapshots = settings.microvm_enable_snapshots
        self.microvm_workdir = Path(
            settings.microvm_workdir
            or (artifact_root.parent / "microvm")
        )
        self.excluded_prefixes = [
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
        side_effect_class = self.classify_side_effect(action, policy_verdict)
        if side_effect_class == "sandboxed_mutation" and not approval_token:
            return self._approval_required_result(sandbox_spec)

        started_at = utc_now()
        sandbox_id = new_id("sandbox")
        vm_id = f"hlab-microvm-{sandbox_id}"
        self.microvm_workdir.mkdir(parents=True, exist_ok=True)
        runner_root = Path(tempfile.mkdtemp(prefix="hlab-microvm-", dir=self.microvm_workdir))
        guest_workspace = runner_root / "workspace"
        include_git = action.tool_name == "git"
        try:
            self._copy_workspace(guest_workspace, include_git=include_git)
            before_state = self._workspace_state_for(guest_workspace)
            if side_effect_class != "sandboxed_mutation":
                self._make_workspace_read_only(guest_workspace)

            runner_command = self._build_runner_command(action)
            process, timed_out, stdout, stderr = await self._execute_with_timeout(
                runner_command,
                max(1, sandbox_spec.timeout_seconds or self.microvm_timeout_seconds),
                cwd=guest_workspace,
                network_policy=sandbox_spec.network_policy,
            )

            after_state = self._workspace_state_for(guest_workspace)
            changed_paths = self._changed_paths(before_state, after_state)
            patch = self._build_patch(before_state, after_state, changed_paths)

            exit_code = None if timed_out else process.returncode
            ok = (exit_code == 0) and not timed_out
            stderr_text = compact_text(stderr.decode("utf-8", errors="replace"), 4000)
            stdout_text = compact_text(stdout.decode("utf-8", errors="replace"), 4000)
            error = None if ok else (
                "MicroVM execution timed out"
                if timed_out
                else stderr_text or f"Command exited with {exit_code}"
            )

            if ok and side_effect_class == "sandboxed_mutation":
                self._sync_changes_to_repo(guest_workspace, changed_paths)

            finished_at = utc_now()
            evidence = self._build_evidence(
                stdout_text,
                stderr_text,
                exit_code,
                changed_paths,
                patch,
                vm_id,
                sandbox_spec,
                started_at,
                finished_at,
            )
            approval_context = None
            if approval_token:
                approval_context = ApprovalContext(
                    approval_token=approval_token,
                    used=True,
                )
            trace = SandboxTrace(
                sandbox_id=sandbox_id,
                sandbox_mode="microvm",
                image=sandbox_spec.image,
                container_id=None,
                network_policy=sandbox_spec.network_policy,
                started_at=started_at,
                finished_at=finished_at,
                timed_out=timed_out,
                changed_paths=changed_paths,
                used_approval_token=bool(approval_token),
                exit_code=exit_code,
                ok=ok,
                error=error,
                docker_command=runner_command,
                side_effect_class=side_effect_class,
                hardened_config=sandbox_spec.hardened_config,
                evidence=evidence,
                policy_verdict=policy_verdict,
                approval_context=approval_context,
                backend=self.backend_name,
                executor_version=self.executor_version,
                vm_id=vm_id,
                guest_image=self.rootfs_image or sandbox_spec.image,
                kernel_image=self.kernel_image,
                snapshot_id=None,
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
        except Exception as exc:  # noqa: BLE001
            return self._execution_error_result(
                sandbox_id=sandbox_id,
                sandbox_spec=sandbox_spec,
                started_at=started_at,
                error=exc,
                runner_command=self._build_runner_command(action),
                vm_id=vm_id,
            )
        finally:
            shutil.rmtree(runner_root, ignore_errors=True)

    def status(self) -> SandboxStatus:
        binary_ready, binary_error = self._probe_binary()
        kernel_ready, kernel_error = self._probe_required_path(self.kernel_image, "kernel image")
        rootfs_ready, rootfs_error = self._probe_required_path(self.rootfs_image, "rootfs image")
        workdir_ready, workdir_error = self._probe_workdir()
        networking_ready = binary_ready
        snapshot_ready = self.enable_snapshots and binary_ready and kernel_ready and rootfs_ready
        executor_ready = binary_ready and kernel_ready and rootfs_ready and workdir_ready
        probe_checks = [
            ProbeCheckResult(check="microvm_binary", passed=binary_ready, error=binary_error),
            ProbeCheckResult(check="microvm_kernel", passed=kernel_ready, error=kernel_error),
            ProbeCheckResult(check="microvm_rootfs", passed=rootfs_ready, error=rootfs_error),
            ProbeCheckResult(check="microvm_jailer_or_runner", passed=workdir_ready, error=workdir_error),
            ProbeCheckResult(
                check="microvm_networking",
                passed=networking_ready,
                error=None if networking_ready else "MicroVM runner is not available for controlled networking.",
            ),
            ProbeCheckResult(
                check="microvm_snapshot_support",
                passed=snapshot_ready,
                error=None if snapshot_ready else "Snapshot support is disabled in the local MicroVM runner.",
            ),
        ]
        last_error = next((item.error for item in probe_checks if item.error), None)
        return SandboxStatus(
            sandbox_backend=self.backend_name,
            docker_ready=False,
            sandbox_image_ready=rootfs_ready,
            sandbox_active_runs=0,
            sandbox_failures=0,
            image=self.rootfs_image,
            fallback_mode=not executor_ready,
            last_probe_error=last_error,
            last_probe_at=utc_now(),
            hardened_ready=executor_ready,
            rootless_ready=executor_ready,
            no_new_privileges_ready=True,
            capability_drop_ready=True,
            policy_enforcement_ready=True,
            probe_checks=probe_checks,
            active_sandbox_count=0,
            total_executions_24h=0,
            failure_count_24h=0,
            executor_ready=executor_ready,
            executor_capabilities=self._capabilities.to_dict(),
            executor_version=self.executor_version,
        )

    def validate_spec(self, spec: SandboxSpec) -> Tuple[bool, Optional[str]]:
        binary_ready, binary_error = self._probe_binary()
        if not binary_ready:
            return False, binary_error
        kernel_ready, kernel_error = self._probe_required_path(self.kernel_image, "kernel image")
        if not kernel_ready:
            return False, kernel_error
        rootfs_ready, rootfs_error = self._probe_required_path(self.rootfs_image, "rootfs image")
        if not rootfs_ready:
            return False, rootfs_error
        if spec.network_policy == "restricted" and not self.supports("supports_network_restricted"):
            return False, "Restricted networking is not supported by the configured MicroVM runner."
        return True, None

    def spec_for_action(
        self,
        action: ActionPlan,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
    ) -> SandboxSpec:
        spec = super().spec_for_action(action, approval_token, policy_verdict)
        spec.sandbox_mode = "microvm"
        spec.image = self.rootfs_image or "harness-lab/microvm:local"
        spec.timeout_seconds = self.microvm_timeout_seconds or spec.timeout_seconds
        spec.backend_hint = self.backend_name
        return spec

    def _probe_binary(self) -> Tuple[bool, Optional[str]]:
        if os.path.sep in self.microvm_binary:
            candidate = Path(self.microvm_binary)
            if candidate.exists() and os.access(candidate, os.X_OK):
                return True, None
            return False, f"MicroVM binary not found or not executable: {self.microvm_binary}"
        resolved = shutil.which(self.microvm_binary)
        if resolved:
            return True, None
        return False, f"MicroVM binary not found: {self.microvm_binary}"

    @staticmethod
    def _probe_required_path(path_value: Optional[str], label: str) -> Tuple[bool, Optional[str]]:
        if not path_value:
            return False, f"MicroVM {label} is not configured."
        candidate = Path(path_value)
        if candidate.exists():
            return True, None
        return False, f"MicroVM {label} not found: {path_value}"

    def _probe_workdir(self) -> Tuple[bool, Optional[str]]:
        try:
            self.microvm_workdir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            return False, f"MicroVM workdir is not writable: {exc}"
        return True, None

    async def _execute_with_timeout(
        self,
        command: List[str],
        timeout_seconds: int,
        cwd: Path,
        network_policy: str,
    ) -> Tuple[asyncio.subprocess.Process, bool, bytes, bytes]:
        env = os.environ.copy()
        env["HARNESS_MICROVM_NETWORK_POLICY"] = network_policy
        env["HARNESS_MICROVM_KERNEL_IMAGE"] = self.kernel_image or ""
        env["HARNESS_MICROVM_ROOTFS_IMAGE"] = self.rootfs_image or ""
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(cwd),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=max(1, timeout_seconds + 2),
            )
            timed_out = False
        except asyncio.TimeoutError:
            process.kill()
            stdout, stderr = await process.communicate()
            timed_out = True
        return process, timed_out, stdout, stderr

    def _build_runner_command(self, action: ActionPlan) -> List[str]:
        if action.tool_name == "shell":
            return ["sh", "-lc", str(action.payload.get("command", ""))]
        if action.tool_name == "git":
            git_action = str(action.payload.get("action", "status") or "status")
            git_command_map = {
                "status": "git status --short",
                "diff": "git diff --stat",
                "log": "git log --oneline -5",
            }
            return ["sh", "-lc", git_command_map.get(git_action, "git status --short")]
        if action.tool_name == "http_fetch":
            python_bin = self._python_runner()
            script = (
                "import json,sys,urllib.request;"
                "url=sys.argv[1];"
                "with urllib.request.urlopen(url, timeout=10) as r:"
                " body=r.read(8000).decode('utf-8', errors='replace');"
                " print(json.dumps({'url': url, 'status': getattr(r, 'status', 200), 'body': body}))"
            )
            return [python_bin, "-c", script, str(action.payload.get("url", "") or "")]
        if action.tool_name == "filesystem" and action.payload.get("action") == "write_file":
            python_bin = self._python_runner()
            script = (
                "from pathlib import Path;import sys;"
                "target=Path(sys.argv[1]);"
                "content=sys.argv[2];"
                "target.parent.mkdir(parents=True, exist_ok=True);"
                "target.write_text(content, encoding='utf-8');"
                "print(target.as_posix())"
            )
            return [
                python_bin,
                "-c",
                script,
                str(action.payload.get("path", "") or ""),
                str(action.payload.get("content", "")),
            ]
        return ["sh", "-lc", "printf 'unsupported microvm action' >&2; exit 2"]

    def _python_runner(self) -> str:
        candidate = Path(self.microvm_binary)
        name = candidate.name if candidate.name else self.microvm_binary
        if "python" in name:
            return self.microvm_binary
        return sys.executable

    def _copy_workspace(self, destination: Path, include_git: bool) -> None:
        def _ignore(dirpath: str, names: List[str]) -> set[str]:
            base = Path(dirpath)
            rel = base.relative_to(self.repo_root)
            ignored: set[str] = set()
            for name in names:
                candidate = name if str(rel) == "." else f"{rel.as_posix()}/{name}"
                if not include_git and name == ".git":
                    ignored.add(name)
                    continue
                if any(
                    candidate == prefix or candidate.startswith(prefix + "/")
                    for prefix in self.excluded_prefixes
                ):
                    ignored.add(name)
            return ignored

        shutil.copytree(self.repo_root, destination, ignore=_ignore, symlinks=True)

    def _make_workspace_read_only(self, root: Path) -> None:
        for path in sorted(root.rglob("*"), reverse=True):
            if path.is_dir():
                path.chmod(0o555)
            else:
                path.chmod(0o444)
        root.chmod(0o555)

    def _workspace_state_for(self, root: Path) -> Dict[str, Dict[str, str]]:
        state: Dict[str, Dict[str, str]] = {}
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            relative = str(path.relative_to(root))
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
    def _changed_paths(before_state: Dict[str, Dict[str, str]], after_state: Dict[str, Dict[str, str]]) -> List[str]:
        changed: List[str] = []
        for relative in sorted(set(before_state) | set(after_state)):
            if before_state.get(relative, {}).get("digest") != after_state.get(relative, {}).get("digest"):
                changed.append(relative)
        return changed

    @staticmethod
    def _build_patch(
        before_state: Dict[str, Dict[str, str]],
        after_state: Dict[str, Dict[str, str]],
        changed_paths: List[str],
    ) -> str:
        segments: List[str] = []
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

    def _sync_changes_to_repo(self, guest_workspace: Path, changed_paths: List[str]) -> None:
        for relative in changed_paths:
            guest_path = guest_workspace / relative
            host_path = self.repo_root / relative
            if guest_path.exists():
                host_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(guest_path, host_path)
            elif host_path.exists():
                host_path.unlink()

    def _build_evidence(
        self,
        stdout: str,
        stderr: str,
        exit_code: Optional[int],
        changed_paths: List[str],
        patch: str,
        vm_id: str,
        spec: SandboxSpec,
        started_at: str,
        finished_at: str,
    ) -> SandboxEvidence:
        try:
            from datetime import datetime

            start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            finish_dt = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
            duration_ms = int((finish_dt - start_dt).total_seconds() * 1000)
        except Exception:  # noqa: BLE001
            duration_ms = 0

        metadata = ContainerMetadata(
            container_id=vm_id,
            image=spec.image,
            created_at=started_at,
            started_at=started_at,
            finished_at=finished_at,
            security_options=["microvm-local-runner"],
            dropped_capabilities=[],
            added_capabilities=[],
            user=self.settings.sandbox_rootless_user,
            mounts=[
                MountInfo(
                    source=str(self.repo_root),
                    destination=spec.workspace_mount,
                    mode="rw" if changed_paths else "ro",
                    mount_type="copy",
                )
            ],
            network_mode=spec.network_policy,
        )
        timing = ExecutionTiming(
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
            container_metadata=metadata,
            execution_timing=timing,
        )

    @staticmethod
    def _parsed_output(action: ActionPlan, stdout: str) -> Dict[str, Any]:
        if action.tool_name == "http_fetch":
            try:
                return json.loads(stdout)
            except Exception:  # noqa: BLE001
                return {}
        return {}

    def _approval_required_result(self, spec: SandboxSpec) -> SandboxResult:
        started_at = utc_now()
        sandbox_id = new_id("sandbox")
        vm_id = f"hlab-microvm-{sandbox_id}"
        trace = SandboxTrace(
            sandbox_id=sandbox_id,
            sandbox_mode="microvm",
            image=spec.image,
            container_id=None,
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
            vm_id=vm_id,
            guest_image=self.rootfs_image or spec.image,
            kernel_image=self.kernel_image,
        )
        return SandboxResult(
            ok=False,
            sandbox_trace=trace,
            error=trace.error,
        )

    def _execution_error_result(
        self,
        sandbox_id: str,
        sandbox_spec: SandboxSpec,
        started_at: str,
        error: Exception,
        runner_command: List[str],
        vm_id: str,
    ) -> SandboxResult:
        trace = SandboxTrace(
            sandbox_id=sandbox_id,
            sandbox_mode="microvm",
            image=sandbox_spec.image,
            container_id=None,
            network_policy=sandbox_spec.network_policy,
            started_at=started_at,
            finished_at=utc_now(),
            timed_out=False,
            changed_paths=[],
            used_approval_token=False,
            exit_code=None,
            ok=False,
            error=compact_text(str(error), 1000),
            docker_command=runner_command,
            side_effect_class="denied_before_sandbox",
            backend=self.backend_name,
            executor_version=self.executor_version,
            vm_id=vm_id,
            guest_image=self.rootfs_image or sandbox_spec.image,
            kernel_image=self.kernel_image,
        )
        return SandboxResult(ok=False, sandbox_trace=trace, error=trace.error)
