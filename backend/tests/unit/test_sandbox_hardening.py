"""Unit tests for hardened sandbox functionality."""

from __future__ import annotations

import asyncio
import pytest
from pathlib import Path

from app.harness_lab.boundary.sandbox import SandboxManager
from app.harness_lab.boundary.docker_executor import DockerSandboxExecutor
from app.harness_lab.settings import HarnessLabSettings
from app.harness_lab.types import (
    ActionPlan,
    HardenedSandboxConfig,
    PolicyVerdictSnapshot,
    SideEffectClass,
)


class FakeDatabase:
    """Fake database for testing."""
    def __init__(self, repo_root: Path):
        self.repo_root = str(repo_root)
        self.artifact_root = str(repo_root / "artifacts")


@pytest.fixture
def sandbox_manager(tmp_path):
    """Create a sandbox manager for testing."""
    settings = HarnessLabSettings(
        HARNESS_DB_URL="postgresql://test:test@localhost/test",
        HARNESS_REDIS_URL="redis://localhost:6379/0",
    )
    db = FakeDatabase(tmp_path)
    return SandboxManager(settings, db)


@pytest.fixture
def docker_executor(tmp_path):
    """Create a Docker sandbox executor for testing."""
    settings = HarnessLabSettings(
        HARNESS_DB_URL="postgresql://test:test@localhost/test",
        HARNESS_REDIS_URL="redis://localhost:6379/0",
    )
    return DockerSandboxExecutor(
        settings=settings,
        repo_root=tmp_path,
        artifact_root=tmp_path / "artifacts",
    )


class TestHardenedDockerCommand:
    """Tests for hardened Docker command generation."""

    def test_hardened_config_includes_no_new_privileges(self, docker_executor):
        """Verify hardened config includes no-new-privileges."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        config = docker_executor._build_hardened_config(action)
        
        assert config.no_new_privileges is True
        assert "no-new-privileges:true" in config.security_options

    def test_hardened_config_cap_drop_all(self, docker_executor):
        """Verify hardened config drops all capabilities."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        config = docker_executor._build_hardened_config(action)
        
        assert config.cap_drop_all is True

    def test_hardened_config_rootless_user(self, docker_executor):
        """Verify hardened config uses rootless user."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        config = docker_executor._build_hardened_config(action)
        
        assert config.rootless_user == "1000:1000"

    def test_capability_whitelist_for_write_file(self, docker_executor):
        """Verify write_file gets appropriate capability whitelist."""
        action = ActionPlan(
            tool_name="filesystem",
            subject="tool.filesystem.write_file",
            payload={"action": "write_file", "path": "test.txt", "content": "hello"},
        )
        caps = docker_executor._get_capability_whitelist(action)
        
        assert "CAP_CHOWN" in caps
        assert "CAP_DAC_OVERRIDE" in caps

    def test_capability_whitelist_for_shell(self, docker_executor):
        """Verify shell gets minimal capability whitelist."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "ls"},
        )
        caps = docker_executor._get_capability_whitelist(action)
        
        assert "CAP_DAC_OVERRIDE" in caps

    def test_capability_whitelist_for_git(self, docker_executor):
        """Verify git gets empty capability whitelist."""
        action = ActionPlan(
            tool_name="git",
            subject="tool.git.status",
            payload={"action": "status"},
        )
        caps = docker_executor._get_capability_whitelist(action)
        
        assert caps == []

    def test_docker_command_includes_security_options(self, docker_executor, tmp_path):
        """Verify Docker command includes security options."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        spec = docker_executor.spec_for_action(action)
        
        cmd, cleanup_dir, mounts = docker_executor._build_hardened_docker_command(
            action, spec, "test-container"
        )
        
        # Check security options
        assert "--security-opt" in cmd
        assert "no-new-privileges:true" in cmd
        
        # Check capabilities
        assert "--cap-drop=ALL" in cmd
        
        # Check user
        assert "--user" in cmd
        assert "1000:1000" in cmd
        
        # Check read-only
        assert "--read-only" in cmd

    def test_docker_command_network_none_for_shell(self, docker_executor, tmp_path):
        """Verify shell uses network=none."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        spec = docker_executor.spec_for_action(action)
        
        cmd, _, _ = docker_executor._build_hardened_docker_command(
            action, spec, "test-container"
        )
        
        network_idx = cmd.index("--network")
        assert cmd[network_idx + 1] == "none"

    def test_docker_command_network_restricted_for_http_fetch(self, docker_executor, tmp_path):
        """Verify http_fetch uses network=restricted (bridge)."""
        action = ActionPlan(
            tool_name="http_fetch",
            subject="tool.http_fetch",
            payload={"url": "https://example.com"},
        )
        spec = docker_executor.spec_for_action(action)
        
        assert spec.network_policy == "restricted"
        
        cmd, _, _ = docker_executor._build_hardened_docker_command(
            action, spec, "test-container"
        )
        
        network_idx = cmd.index("--network")
        # Currently restricted maps to bridge
        assert cmd[network_idx + 1] == "bridge"


class TestSideEffectClassification:
    """Tests for side effect classification."""

    def test_write_file_classified_as_sandboxed_mutation(self, sandbox_manager):
        """Verify write_file is classified as sandboxed_mutation."""
        action = ActionPlan(
            tool_name="filesystem",
            subject="tool.filesystem.write_file",
            payload={"action": "write_file", "path": "test.txt", "content": "hello"},
        )
        classification = sandbox_manager.classify_side_effect(action)
        
        assert classification == "sandboxed_mutation"

    def test_shell_classified_as_sandboxed_read(self, sandbox_manager):
        """Verify shell is classified as sandboxed_read."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "ls"},
        )
        classification = sandbox_manager.classify_side_effect(action)
        
        assert classification == "sandboxed_read"

    def test_git_classified_as_sandboxed_read(self, sandbox_manager):
        """Verify git is classified as sandboxed_read."""
        action = ActionPlan(
            tool_name="git",
            subject="tool.git.status",
            payload={"action": "status"},
        )
        classification = sandbox_manager.classify_side_effect(action)
        
        assert classification == "sandboxed_read"

    def test_http_fetch_classified_as_sandboxed_read(self, sandbox_manager):
        """Verify http_fetch is classified as sandboxed_read."""
        action = ActionPlan(
            tool_name="http_fetch",
            subject="tool.http_fetch",
            payload={"url": "https://example.com"},
        )
        classification = sandbox_manager.classify_side_effect(action)
        
        assert classification == "sandboxed_read"

    def test_denied_by_policy_classification(self, sandbox_manager):
        """Verify denied actions are classified correctly."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "rm -rf /"},
        )
        verdict = PolicyVerdictSnapshot(
            decision="deny",
            subject="tool.shell.execute",
            rule_id="rule_001",
        )
        classification = sandbox_manager.classify_side_effect(action, verdict)
        
        assert classification == "denied_before_sandbox"

    def test_approval_required_classification(self, sandbox_manager):
        """Verify approval-required actions are classified correctly."""
        action = ActionPlan(
            tool_name="filesystem",
            subject="tool.filesystem.write_file",
            payload={"action": "write_file", "path": "test.txt", "content": "hello"},
        )
        verdict = PolicyVerdictSnapshot(
            decision="approval_required",
            subject="tool.filesystem.write_file",
            rule_id="rule_002",
        )
        classification = sandbox_manager.classify_side_effect(action, verdict)
        
        assert classification == "approval_blocked"


class TestSandboxSpec:
    """Tests for sandbox spec generation."""

    def test_spec_includes_hardened_config(self, docker_executor):
        """Verify sandbox spec includes hardened config."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        spec = docker_executor.spec_for_action(action)
        
        assert spec.hardened_config is not None
        assert spec.hardened_config.no_new_privileges is True

    def test_spec_network_policy_for_shell(self, docker_executor):
        """Verify shell spec uses network=none."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        spec = docker_executor.spec_for_action(action)
        
        assert spec.network_policy == "none"

    def test_spec_network_policy_for_http_fetch(self, docker_executor):
        """Verify http_fetch spec uses network=restricted."""
        action = ActionPlan(
            tool_name="http_fetch",
            subject="tool.http_fetch",
            payload={"url": "https://example.com"},
        )
        spec = docker_executor.spec_for_action(action)
        
        assert spec.network_policy == "restricted"

    def test_spec_includes_approval_token(self, docker_executor):
        """Verify spec includes approval token when provided."""
        action = ActionPlan(
            tool_name="filesystem",
            subject="tool.filesystem.write_file",
            payload={"action": "write_file", "path": "test.txt", "content": "hello"},
        )
        spec = docker_executor.spec_for_action(
            action, approval_token="approval:test:approve"
        )
        
        assert spec.approval_token == "approval:test:approve"

    def test_spec_includes_backend_fields(self, docker_executor):
        """Verify spec includes backend and executor_version fields."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        spec = docker_executor.spec_for_action(action)
        
        assert spec.backend_hint == "docker"
        assert spec.sandbox_mode == "docker"


class TestSandboxStatus:
    """Tests for sandbox status reporting."""

    def test_status_includes_executor_fields(self, docker_executor):
        """Verify status includes executor readiness fields."""
        status = docker_executor.status()
        
        # Check executor fields exist
        assert hasattr(status, "executor_ready")
        assert hasattr(status, "executor_capabilities")
        assert hasattr(status, "fallback_mode")

    def test_docker_executor_capabilities(self, docker_executor):
        """Verify Docker executor reports correct capabilities."""
        status = docker_executor.status()
        
        assert status.executor_ready is True
        assert status.executor_capabilities["supports_mutation"] is True
        assert status.executor_capabilities["supports_network_restricted"] is True
        assert status.executor_capabilities["supports_rootless"] is True
        assert status.executor_capabilities["supports_snapshot"] is False
        assert status.fallback_mode is False


class TestRequiresSandbox:
    """Tests for sandbox requirement detection."""

    def test_shell_requires_sandbox(self, sandbox_manager):
        """Verify shell requires sandbox."""
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "ls"},
        )
        assert sandbox_manager.requires_sandbox(action) is True

    def test_git_requires_sandbox(self, sandbox_manager):
        """Verify git requires sandbox."""
        action = ActionPlan(
            tool_name="git",
            subject="tool.git.status",
            payload={"action": "status"},
        )
        assert sandbox_manager.requires_sandbox(action) is True

    def test_http_fetch_requires_sandbox(self, sandbox_manager):
        """Verify http_fetch requires sandbox."""
        action = ActionPlan(
            tool_name="http_fetch",
            subject="tool.http_fetch",
            payload={"url": "https://example.com"},
        )
        assert sandbox_manager.requires_sandbox(action) is True

    def test_write_file_requires_sandbox(self, sandbox_manager):
        """Verify write_file requires sandbox."""
        action = ActionPlan(
            tool_name="filesystem",
            subject="tool.filesystem.write_file",
            payload={"action": "write_file", "path": "test.txt", "content": "hello"},
        )
        assert sandbox_manager.requires_sandbox(action) is True

    def test_read_file_does_not_require_sandbox(self, sandbox_manager):
        """Verify read_file does not require sandbox."""
        action = ActionPlan(
            tool_name="filesystem",
            subject="tool.filesystem.read_file",
            payload={"action": "read_file", "path": "test.txt"},
        )
        assert sandbox_manager.requires_sandbox(action) is False

    def test_list_dir_does_not_require_sandbox(self, sandbox_manager):
        """Verify list_dir does not require sandbox."""
        action = ActionPlan(
            tool_name="filesystem",
            subject="tool.filesystem.list_dir",
            payload={"action": "list_dir", "path": "."},
        )
        assert sandbox_manager.requires_sandbox(action) is False


class TestSandboxExecutorRegistry:
    """Tests for sandbox executor registry."""

    def test_registry_has_docker_backend(self, sandbox_manager):
        """Verify registry includes Docker backend."""
        registry = sandbox_manager.registry
        assert "docker" in registry.list_backends()
        assert registry.get("docker") is not None

    def test_registry_has_microvm_stub_backend(self, sandbox_manager):
        """Verify registry includes MicroVM stub backend."""
        registry = sandbox_manager.registry
        assert "microvm_stub" in registry.list_backends()
        assert registry.get("microvm_stub") is not None

    def test_registry_list_backends(self, sandbox_manager):
        """Verify registry can list all backends."""
        backends = sandbox_manager.registry.list_backends()
        assert "docker" in backends
        assert "microvm_stub" in backends


class TestSandboxBackendSelector:
    """Tests for sandbox backend selector."""

    def test_selector_defaults_to_docker(self, sandbox_manager):
        """Verify selector defaults to Docker backend."""
        executor = sandbox_manager.selector.select()
        assert executor is not None
        assert executor.backend_name == "docker"

    def test_selector_selects_docker_explicitly(self, sandbox_manager):
        """Verify selector can select Docker explicitly."""
        executor = sandbox_manager.selector.select("docker")
        assert executor is not None
        assert executor.backend_name == "docker"

    def test_selector_selects_microvm_stub(self, sandbox_manager):
        """Verify selector can select MicroVM stub."""
        executor = sandbox_manager.selector.select("microvm_stub")
        assert executor is not None
        assert executor.backend_name == "microvm_stub"

    def test_selector_invalid_backend_falls_back_to_default(self, sandbox_manager):
        """Verify unknown backend hints fall back to configured/default backend."""
        executor = sandbox_manager.selector.select("invalid_backend")
        assert executor.backend_name == "docker"


class TestStubMicroVMSandboxExecutor:
    """Tests for MicroVM stub executor."""

    def test_stub_executor_not_ready(self):
        """Verify stub executor reports not ready."""
        from app.harness_lab.boundary.microvm_stub_executor import StubMicroVMSandboxExecutor
        
        executor = StubMicroVMSandboxExecutor()
        status = executor.status()
        
        assert status.executor_ready is False
        assert status.fallback_mode is True

    def test_stub_executor_capabilities(self):
        """Verify stub executor reports minimal capabilities."""
        from app.harness_lab.boundary.microvm_stub_executor import StubMicroVMSandboxExecutor
        
        executor = StubMicroVMSandboxExecutor()
        assert executor.capabilities.supports_mutation is False
        assert executor.capabilities.supports_network_restricted is False
        assert executor.capabilities.supports_rootless is False
        assert executor.capabilities.supports_snapshot is False

    def test_stub_executor_execute_returns_not_ready_result(self):
        """Verify stub executor returns a not-ready result."""
        from app.harness_lab.boundary.microvm_stub_executor import StubMicroVMSandboxExecutor
        from app.harness_lab.types import ActionPlan
        
        executor = StubMicroVMSandboxExecutor()
        action = ActionPlan(
            tool_name="shell",
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        spec = executor.spec_for_action(action)
        result = asyncio.run(executor.execute(action, spec))
        assert result.ok is False
        assert result.error is not None
        assert "not yet implemented" in result.error
