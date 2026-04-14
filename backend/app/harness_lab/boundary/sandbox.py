"""Sandbox orchestration facade for pluggable execution backends.

This module provides the SandboxManager as an orchestration layer that
coordinates sandbox execution across different backends (Docker, MicroVM, etc.)
through the SandboxExecutor abstraction.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from ..settings import HarnessLabSettings
from ..types import (
    ActionPlan,
    PolicyVerdictSnapshot,
    SideEffectClass,
    SandboxResult,
    SandboxSpec,
    SandboxStatus,
)
from .docker_executor import DockerSandboxExecutor
from .microvm_executor import MicroVMSandboxExecutor
from .executor import SandboxBackendSelector, SandboxExecutorRegistry
from .microvm_stub_executor import StubMicroVMSandboxExecutor


class SandboxManager:
    """Orchestration facade for sandbox execution backends.
    
    The SandboxManager is responsible for:
    - Coordinating execution across different sandbox backends
    - Managing the executor registry and backend selection
    - Providing a unified interface for sandbox operations
    - Archiving execution evidence and traces
    
    It does NOT contain specific Docker command construction logic;
    that is delegated to the appropriate SandboxExecutor implementation.
    """

    def __init__(self, settings: HarnessLabSettings, database: Any) -> None:
        self.settings = settings
        self.repo_root = Path(database.repo_root)
        self.artifact_root = Path(database.artifact_root)
        
        # Initialize executor registry
        self._registry = SandboxExecutorRegistry()
        self._init_executors(settings, database)
        
        # Initialize backend selector
        self._selector = SandboxBackendSelector(self._registry, settings)

    def _init_executors(self, settings: HarnessLabSettings, database: Any) -> None:
        """Initialize and register sandbox executor backends."""
        # Register Docker executor (default, production-ready)
        docker_executor = DockerSandboxExecutor(
            settings=settings,
            repo_root=self.repo_root,
            artifact_root=self.artifact_root,
        )
        self._registry.register("docker", docker_executor, default=True)

        microvm_executor = MicroVMSandboxExecutor(
            settings=settings,
            repo_root=self.repo_root,
            artifact_root=self.artifact_root,
        )
        self._registry.register("microvm", microvm_executor, default=False)

        # Register MicroVM stub executor (for testing abstraction layer)
        microvm_stub = StubMicroVMSandboxExecutor()
        self._registry.register("microvm_stub", microvm_stub, default=False)

    @property
    def registry(self) -> SandboxExecutorRegistry:
        """Get the executor registry."""
        return self._registry

    @property
    def selector(self) -> SandboxBackendSelector:
        """Get the backend selector."""
        return self._selector

    def requires_sandbox(self, action: ActionPlan) -> bool:
        """Determine if action requires sandboxed execution.
        
        Delegates to the default executor's capability assessment.
        """
        default_executor = self._registry.get_default()
        if default_executor is None:
            # Fallback: use basic classification
            if action.tool_name in {"shell", "git", "http_fetch"}:
                return True
            return action.tool_name == "filesystem" and action.payload.get("action") == "write_file"
        return default_executor.requires_sandbox(action)

    def classify_side_effect(
        self,
        action: ActionPlan,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
        backend_hint: Optional[str] = None,
    ) -> SideEffectClass:
        """Classify side effects through the selected executor."""
        executor = self._selector.select(backend_hint)
        return executor.classify_side_effect(action, policy_verdict)  # type: ignore[return-value]

    def sandbox_spec_for(
        self,
        action: ActionPlan,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
        backend_hint: Optional[str] = None,
    ) -> SandboxSpec:
        """Generate sandbox spec for an action.
        
        Uses the selected backend's spec generation logic.
        """
        executor = self._selector.select(backend_hint)
        spec = executor.spec_for_action(action, approval_token, policy_verdict)
        
        # Ensure backend_hint is set
        if spec.backend_hint is None:
            spec.backend_hint = executor.backend_name
            
        return spec

    def status(self, backend: Optional[str] = None) -> SandboxStatus:
        """Get sandbox status for a specific backend or the default.
        
        Args:
            backend: Backend name to check, or None for configured/default
            
        Returns:
            SandboxStatus with health checks and capability readiness
        """
        if backend:
            executor = self._registry.get(backend)
            if executor is None:
                # Return degraded status for unknown backend
                from ..utils import utc_now
                return SandboxStatus(
                    sandbox_backend=backend,
                    docker_ready=False,
                    sandbox_image_ready=False,
                    sandbox_active_runs=0,
                    sandbox_failures=0,
                    image=None,
                    fallback_mode=True,
                    last_probe_error=f"Unknown backend: {backend}",
                    last_probe_at=utc_now(),
                    hardened_ready=False,
                    rootless_ready=False,
                    no_new_privileges_ready=False,
                    capability_drop_ready=False,
                    policy_enforcement_ready=False,
                    probe_checks=[],
                    active_sandbox_count=0,
                    total_executions_24h=0,
                    failure_count_24h=0,
                    executor_ready=False,
                    executor_capabilities={},
                    executor_version=None,
                )
            return executor.status()
        
        # Get status for configured/default backend
        executor = self._selector.select()
        status = executor.status()
        status.executor_status = self._registry.get_status(self._selector.get_configured_backend_name())
        return status

    def get_all_statuses(self) -> dict[str, SandboxStatus]:
        """Get status for all registered backends.
        
        Returns:
            Dict mapping backend names to their statuses
        """
        return self._registry.get_all_statuses()

    async def execute_action(
        self,
        action: ActionPlan,
        sandbox_spec: Optional[SandboxSpec] = None,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
        backend_hint: Optional[str] = None,
    ) -> SandboxResult:
        """Execute action using the appropriate sandbox backend.
        
        This method:
        1. Selects the appropriate executor based on backend_hint or configuration
        2. Generates a sandbox spec if not provided
        3. Delegates execution to the selected executor
        4. Returns the execution result with trace and evidence
        
        Args:
            action: The action to execute
            sandbox_spec: Optional pre-built sandbox spec
            approval_token: Optional token for approved mutations
            policy_verdict: Optional policy verdict for audit trail
            backend_hint: Hint for backend selection
            
        Returns:
            SandboxResult with execution outcome and evidence
        """
        # Select executor
        executor = self._selector.select(backend_hint)
        
        # Generate spec if not provided
        spec = sandbox_spec or self.sandbox_spec_for(
            action, approval_token, policy_verdict, backend_hint
        )
        
        # Ensure spec has backend hint
        if spec.backend_hint is None:
            spec.backend_hint = executor.backend_name
        
        # Delegate execution to the selected executor
        return await executor.execute(action, spec, approval_token, policy_verdict)

    def get_configured_backend(self) -> str:
        """Get the name of the currently configured backend.
        
        Returns:
            Configured backend name
        """
        return self._selector.get_configured_backend_name()

    def is_backend_ready(self, backend: Optional[str] = None) -> bool:
        """Check if a backend is ready for execution.
        
        Args:
            backend: Backend name to check, or None for configured/default
            
        Returns:
            True if the backend is ready
        """
        return self._selector.is_backend_ready(backend)

    def list_backends(self) -> list[str]:
        """List all registered backend names.
        
        Returns:
            List of backend names
        """
        return self._registry.list_backends()

    def get_backend_capabilities(self, backend: Optional[str] = None) -> dict[str, bool]:
        """Get capabilities for a backend.
        
        Args:
            backend: Backend name, or None for configured/default
            
        Returns:
            Dict of capability names to boolean values
        """
        if backend:
            executor = self._registry.get(backend)
        else:
            executor = self._selector.select()
            
        if executor is None:
            return {}
            
        return executor.capabilities.to_dict()

    def validate_spec(
        self,
        spec: SandboxSpec,
        backend: Optional[str] = None,
    ) -> tuple[bool, Optional[str]]:
        """Validate a sandbox specification for a backend.
        
        Args:
            spec: The sandbox specification to validate
            backend: Backend name, or None for configured/default
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if backend:
            executor = self._registry.get(backend)
        else:
            executor = self._selector.select(spec.backend_hint)
            
        if executor is None:
            return False, f"No executor available for backend: {backend or spec.backend_hint}"
            
        return executor.validate_spec(spec)
