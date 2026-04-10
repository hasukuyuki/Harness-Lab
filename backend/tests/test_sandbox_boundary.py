from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from backend.app.harness_lab.boundary.gateway import ToolGateway
from backend.app.harness_lab.types import ActionPlan, ArtifactRef, SandboxResult, SandboxSpec, SandboxStatus, SandboxTrace
from backend.app.harness_lab.utils import utc_now
from backend.app.harness_lab.workers.runtime_client import LocalArtifactStore


class _NoopConstraints:
    def verify(self, subject: str, payload: dict[str, Any], constraint_set_id: str | None = None):  # noqa: ARG002
        return []


class FakeSandboxManager:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self.calls: list[ActionPlan] = []

    def requires_sandbox(self, action: ActionPlan) -> bool:
        return action.tool_name in {"shell", "git", "http_fetch"} or (
            action.tool_name == "filesystem" and action.payload.get("action") == "write_file"
        )

    def sandbox_spec_for(self, action: ActionPlan, approval_token: str | None = None) -> SandboxSpec:
        return SandboxSpec(
            sandbox_mode="docker",
            image="harness-lab/sandbox:local",
            workspace_mount="/workspace",
            working_dir="/workspace",
            network_policy="restricted" if action.tool_name == "http_fetch" else "none",
            read_only_rootfs=True,
            timeout_seconds=20,
            approval_token=approval_token,
        )

    def status(self) -> SandboxStatus:
        return SandboxStatus(
            sandbox_backend="docker",
            docker_ready=True,
            sandbox_image_ready=True,
            sandbox_active_runs=0,
            sandbox_failures=0,
            image="harness-lab/sandbox:local",
            fallback_mode=False,
            last_probe_error=None,
        )

    async def execute_action(
        self,
        action: ActionPlan,
        sandbox_spec: SandboxSpec | None = None,  # noqa: ARG002
        approval_token: str | None = None,
    ) -> SandboxResult:
        self.calls.append(action)
        changed_paths: list[str] = []
        patch = ""
        stdout = ""
        parsed_output: dict[str, Any] = {}
        if action.tool_name == "filesystem" and action.payload.get("action") == "write_file":
            target = self.repo_root / str(action.payload["path"])
            before = target.read_text(encoding="utf-8") if target.exists() else ""
            after = str(action.payload.get("content", ""))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(after, encoding="utf-8")
            changed_paths = [str(Path(action.payload["path"]))]
            patch = f"--- {action.payload['path']}\n+++ {action.payload['path']}\n-{before}\n+{after}\n"
            stdout = str(target)
        elif action.tool_name == "shell":
            stdout = str(self.repo_root)
        elif action.tool_name == "http_fetch":
            parsed_output = {"url": str(action.payload.get("url", "")), "status": 200, "body": "ok"}

        trace = SandboxTrace(
            sandbox_id="sandbox_test",
            sandbox_mode="docker",
            image="harness-lab/sandbox:local",
            container_id="container_test",
            network_policy="restricted" if action.tool_name == "http_fetch" else "none",
            started_at=utc_now(),
            finished_at=utc_now(),
            timed_out=False,
            changed_paths=changed_paths,
            used_approval_token=bool(approval_token),
            exit_code=0,
            ok=True,
            error=None,
            docker_command=["docker", "run"],
        )
        return SandboxResult(
            ok=True,
            stdout=stdout,
            stderr="",
            exit_code=0,
            timed_out=False,
            changed_paths=changed_paths,
            patch=patch,
            parsed_output=parsed_output,
            sandbox_trace=trace,
            error=None,
        )


def test_tool_gateway_routes_high_risk_tools_through_sandbox(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    store = LocalArtifactStore(repo_root=repo_root, artifact_root=repo_root / "artifacts")
    sandbox = FakeSandboxManager(repo_root)
    gateway = ToolGateway(store, _NoopConstraints(), sandbox_manager=sandbox)

    shell_result = asyncio.run(
        gateway.execute(
            "run_sandbox",
            ActionPlan(tool_name="shell", subject="tool.shell.execute", payload={"command": "pwd"}),
        )
    )
    assert shell_result.ok is True
    assert "sandbox_trace" in shell_result.output

    write_result = asyncio.run(
        gateway.execute(
            "run_sandbox",
            ActionPlan(
                tool_name="filesystem",
                subject="tool.filesystem.write_file",
                payload={
                    "action": "write_file",
                    "path": "notes/sandbox.txt",
                    "content": "sandboxed write",
                    "_approval_token": "approval:test:approve",
                },
            ),
        )
    )
    assert write_result.ok is True
    assert write_result.output["written_path"] == "notes/sandbox.txt"
    assert write_result.output["changed_paths"] == ["notes/sandbox.txt"]
    assert (repo_root / "notes" / "sandbox.txt").read_text(encoding="utf-8") == "sandboxed write"
    assert write_result.output["patch_artifact_id"]
    assert write_result.output["backup_artifact_id"]
    assert len(sandbox.calls) == 2


def test_low_risk_filesystem_actions_remain_host_local(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    sample = repo_root / "docs" / "readme.txt"
    sample.parent.mkdir(parents=True, exist_ok=True)
    sample.write_text("hello sandbox", encoding="utf-8")
    store = LocalArtifactStore(repo_root=repo_root, artifact_root=repo_root / "artifacts")
    sandbox = FakeSandboxManager(repo_root)
    gateway = ToolGateway(store, _NoopConstraints(), sandbox_manager=sandbox)

    read_result = asyncio.run(
        gateway.execute(
            "run_local",
            ActionPlan(
                tool_name="filesystem",
                subject="tool.filesystem.read_file",
                payload={"action": "read_file", "path": "docs/readme.txt"},
            ),
        )
    )
    assert read_result.ok is True
    assert "sandbox_trace" not in read_result.output
    assert sandbox.calls == []
