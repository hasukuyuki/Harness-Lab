"""Unit tests for the real MicroVM sandbox backend."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.harness_lab.boundary.microvm_executor import MicroVMSandboxExecutor
from app.harness_lab.boundary.sandbox import SandboxManager
from app.harness_lab.settings import HarnessLabSettings
from app.harness_lab.types import ActionPlan


class FakeDatabase:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = str(repo_root)
        self.artifact_root = str(repo_root / "artifacts")


@pytest.fixture
def microvm_settings(tmp_path: Path) -> HarnessLabSettings:
    kernel = tmp_path / "vmlinux.bin"
    rootfs = tmp_path / "rootfs.img"
    workdir = tmp_path / "microvm-workdir"
    kernel.write_text("kernel", encoding="utf-8")
    rootfs.write_text("rootfs", encoding="utf-8")
    return HarnessLabSettings(
        HARNESS_DB_URL="postgresql://test:test@localhost/test",
        HARNESS_REDIS_URL="redis://localhost:6379/0",
        HARNESS_MICROVM_BINARY="python3",
        HARNESS_MICROVM_KERNEL_IMAGE=str(kernel),
        HARNESS_MICROVM_ROOTFS_IMAGE=str(rootfs),
        HARNESS_MICROVM_WORKDIR=str(workdir),
    )


@pytest.fixture
def microvm_executor(tmp_path: Path, microvm_settings: HarnessLabSettings) -> MicroVMSandboxExecutor:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "README.md").write_text("hello", encoding="utf-8")
    return MicroVMSandboxExecutor(
        settings=microvm_settings,
        repo_root=repo_root,
        artifact_root=repo_root / "artifacts",
    )


def test_sandbox_manager_registers_real_microvm_backend(tmp_path: Path, microvm_settings: HarnessLabSettings):
    manager = SandboxManager(microvm_settings, FakeDatabase(tmp_path))
    assert "microvm" in manager.registry.list_backends()
    assert manager.registry.get("microvm") is not None


def test_microvm_spec_uses_stable_backend_fields(microvm_executor: MicroVMSandboxExecutor):
    action = ActionPlan(
        tool_name="shell",
        subject="tool.shell.execute",
        payload={"command": "printf hello"},
    )
    spec = microvm_executor.spec_for_action(action)
    assert spec.sandbox_mode == "microvm"
    assert spec.backend_hint == "microvm"
    assert spec.image.endswith("rootfs.img")


def test_microvm_status_reports_ready_when_assets_exist(microvm_executor: MicroVMSandboxExecutor):
    status = microvm_executor.status()
    checks = {item.check: item for item in status.probe_checks}
    assert status.executor_ready is True
    assert status.sandbox_backend == "microvm"
    assert checks["microvm_binary"].passed is True
    assert checks["microvm_kernel"].passed is True
    assert checks["microvm_rootfs"].passed is True


def test_microvm_status_reports_degraded_when_assets_missing(tmp_path: Path):
    settings = HarnessLabSettings(
        HARNESS_DB_URL="postgresql://test:test@localhost/test",
        HARNESS_REDIS_URL="redis://localhost:6379/0",
        HARNESS_MICROVM_BINARY="python3",
        HARNESS_MICROVM_KERNEL_IMAGE=str(tmp_path / "missing-kernel"),
        HARNESS_MICROVM_ROOTFS_IMAGE=str(tmp_path / "missing-rootfs"),
        HARNESS_MICROVM_WORKDIR=str(tmp_path / "microvm-workdir"),
    )
    executor = MicroVMSandboxExecutor(
        settings=settings,
        repo_root=tmp_path / "repo",
        artifact_root=tmp_path / "repo" / "artifacts",
    )
    status = executor.status()
    assert status.executor_ready is False
    assert status.last_probe_error is not None


def test_microvm_shell_execution_returns_vm_trace(microvm_executor: MicroVMSandboxExecutor):
    action = ActionPlan(
        tool_name="shell",
        subject="tool.shell.execute",
        payload={"command": "printf microvm"},
    )
    spec = microvm_executor.spec_for_action(action)
    result = asyncio.run(microvm_executor.execute(action, spec))
    assert result.ok is True
    assert result.stdout == "microvm"
    assert result.sandbox_trace.backend == "microvm"
    assert result.sandbox_trace.sandbox_mode == "microvm"
    assert result.sandbox_trace.vm_id is not None
    assert result.sandbox_trace.kernel_image
    assert result.sandbox_trace.guest_image


def test_microvm_write_file_requires_approval_token(microvm_executor: MicroVMSandboxExecutor):
    action = ActionPlan(
        tool_name="filesystem",
        subject="tool.filesystem.write_file",
        payload={"action": "write_file", "path": "notes/test.txt", "content": "hello"},
    )
    spec = microvm_executor.spec_for_action(action)
    result = asyncio.run(microvm_executor.execute(action, spec))
    assert result.ok is False
    assert "approval token" in (result.error or "").lower()


def test_microvm_write_file_syncs_back_to_repo(microvm_executor: MicroVMSandboxExecutor):
    action = ActionPlan(
        tool_name="filesystem",
        subject="tool.filesystem.write_file",
        payload={"action": "write_file", "path": "notes/test.txt", "content": "hello from microvm"},
    )
    spec = microvm_executor.spec_for_action(action, approval_token="approval:test:approve")
    result = asyncio.run(microvm_executor.execute(action, spec, approval_token="approval:test:approve"))
    assert result.ok is True
    assert "notes/test.txt" in result.changed_paths
    assert "notes/test.txt" in result.patch
    assert (microvm_executor.repo_root / "notes" / "test.txt").read_text(encoding="utf-8") == "hello from microvm"
