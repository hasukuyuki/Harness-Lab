"""Sandbox executor abstraction layer for pluggable backends.

This module provides the core abstraction for sandbox execution backends,
allowing Docker, MicroVM (Firecracker/gVisor), and other execution environments
to be used interchangeably.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from ..types import (
    ActionPlan,
    PolicyVerdictSnapshot,
    SandboxResult,
    SandboxSpec,
    SandboxStatus,
)


class ExecutorCapabilities:
    """Capability matrix for a sandbox executor backend.
    
    Defines what features the backend supports, enabling control plane
to make informed decisions about execution routing and fallback strategies.
    """

    def __init__(
        self,
        supports_mutation: bool = False,
        supports_network_restricted: bool = False,
        supports_rootless: bool = False,
        supports_snapshot: bool = False,
        supports_live_migration: bool = False,
        supports_custom_seccomp: bool = False,
        supports_cgroup_v2: bool = False,
    ) -> None:
        self.supports_mutation = supports_mutation
        self.supports_network_restricted = supports_network_restricted
        self.supports_rootless = supports_rootless
        self.supports_snapshot = supports_snapshot
        self.supports_live_migration = supports_live_migration
        self.supports_custom_seccomp = supports_custom_seccomp
        self.supports_cgroup_v2 = supports_cgroup_v2

    def to_dict(self) -> Dict[str, bool]:
        """Convert capabilities to dictionary for API responses."""
        return {
            "supports_mutation": self.supports_mutation,
            "supports_network_restricted": self.supports_network_restricted,
            "supports_rootless": self.supports_rootless,
            "supports_snapshot": self.supports_snapshot,
            "supports_live_migration": self.supports_live_migration,
            "supports_custom_seccomp": self.supports_custom_seccomp,
            "supports_cgroup_v2": self.supports_cgroup_v2,
        }

    @classmethod
    def docker_defaults(cls) -> "ExecutorCapabilities":
        """Default capabilities for Docker backend."""
        return cls(
            supports_mutation=True,
            supports_network_restricted=True,
            supports_rootless=True,
            supports_snapshot=False,  # Docker checkpoint is not yet enabled
            supports_live_migration=False,
            supports_custom_seccomp=True,
            supports_cgroup_v2=True,
        )

    @classmethod
    def microvm_stub_defaults(cls) -> "ExecutorCapabilities":
        """Default capabilities for MicroVM stub backend."""
        return cls(
            supports_mutation=False,  # Stub does not support real execution
            supports_network_restricted=False,
            supports_rootless=False,
            supports_snapshot=False,
            supports_live_migration=False,
            supports_custom_seccomp=False,
            supports_cgroup_v2=False,
        )

    @classmethod
    def microvm_defaults(cls) -> "ExecutorCapabilities":
        """Default capabilities for the local MicroVM runner backend."""
        return cls(
            supports_mutation=True,
            supports_network_restricted=True,
            supports_rootless=True,
            supports_snapshot=False,
            supports_live_migration=False,
            supports_custom_seccomp=False,
            supports_cgroup_v2=False,
        )


class SandboxExecutor(ABC):
    """Abstract base class for sandbox execution backends.
    
    Implementations must provide:
    - execute: Run an action within the sandbox
    - status: Report backend health and readiness
    - validate_spec: Validate a sandbox specification
    - supports: Check if a capability is available
    
    The executor is responsible for:
    - Container/VM lifecycle management
    - Security policy enforcement (capabilities, seccomp, etc.)
    - Network isolation
    - Evidence collection (stdout, stderr, exit codes, file changes)
    """

    def __init__(self, backend_name: str, executor_version: str) -> None:
        self._backend_name = backend_name
        self._executor_version = executor_version
        self._capabilities = ExecutorCapabilities()

    @property
    def backend_name(self) -> str:
        """Unique identifier for this backend type."""
        return self._backend_name

    @property
    def executor_version(self) -> str:
        """Version string for this executor implementation."""
        return self._executor_version

    @property
    def capabilities(self) -> ExecutorCapabilities:
        """Capability matrix for this executor."""
        return self._capabilities

    @abstractmethod
    async def execute(
        self,
        action: ActionPlan,
        sandbox_spec: SandboxSpec,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
    ) -> SandboxResult:
        """Execute an action within the sandbox.
        
        Args:
            action: The action plan to execute
            sandbox_spec: Configuration for the sandbox environment
            approval_token: Optional token for approved mutations
            policy_verdict: Optional policy verdict for audit trail
            
        Returns:
            SandboxResult with execution outcome and evidence
        """
        ...

    @abstractmethod
    def status(self) -> SandboxStatus:
        """Get current backend status and readiness.
        
        Returns:
            SandboxStatus with health checks and capability readiness
        """
        ...

    @abstractmethod
    def validate_spec(self, spec: SandboxSpec) -> Tuple[bool, Optional[str]]:
        """Validate a sandbox specification for this backend.
        
        Args:
            spec: The sandbox specification to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        ...

    def supports(self, capability: str) -> bool:
        """Check if a specific capability is supported.
        
        Args:
            capability: Name of the capability to check
            
        Returns:
            True if the capability is supported
        """
        return getattr(self._capabilities, capability, False)

    def spec_for_action(
        self,
        action: ActionPlan,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
    ) -> SandboxSpec:
        """Generate a sandbox specification for an action.
        
        Default implementation provides basic spec generation.
        Subclasses may override for backend-specific optimizations.
        
        Args:
            action: The action to generate spec for
            approval_token: Optional approval token
            policy_verdict: Optional policy verdict
            
        Returns:
            Configured SandboxSpec
        """
        from ..types import HardenedSandboxConfig, SandboxNetworkPolicy

        # Determine network policy based on action type
        network_policy: SandboxNetworkPolicy = "none"
        if action.tool_name == "http_fetch":
            network_policy = "restricted"

        # Build hardened config
        hardened_config = self._build_hardened_config(action)

        return SandboxSpec(
            sandbox_mode=self._backend_name,
            image="harness-lab/sandbox:local",  # Default image
            workspace_mount="/workspace",
            working_dir="/workspace",
            network_policy=network_policy,
            read_only_rootfs=True,
            timeout_seconds=20,
            approval_token=approval_token,
            hardened_config=hardened_config,
            backend_hint=self._backend_name,
        )

    def _build_hardened_config(self, action: ActionPlan) -> Optional[Any]:
        """Build hardened sandbox configuration for an action.
        
        Args:
            action: The action to build config for
            
        Returns:
            HardenedSandboxConfig or None if not supported
        """
        from ..types import HardenedSandboxConfig

        cap_whitelist = self._get_capability_whitelist(action)

        return HardenedSandboxConfig(
            no_new_privileges=True,
            cap_drop_all=True,
            cap_add_whitelist=cap_whitelist,
            rootless_user="1000:1000",
            read_only_rootfs=True,
            security_options=["no-new-privileges:true"],
        )

    def _get_capability_whitelist(self, action: ActionPlan) -> List[str]:
        """Get minimum capability whitelist for action type.
        
        Args:
            action: The action to get capabilities for
            
        Returns:
            List of capability strings
        """
        if action.tool_name == "filesystem" and action.payload.get("action") == "write_file":
            # File writes need minimal capabilities
            return ["CAP_CHOWN", "CAP_DAC_OVERRIDE"]

        if action.tool_name == "shell":
            # Shell commands may need various capabilities
            return ["CAP_DAC_OVERRIDE"]

        # git, http_fetch need minimal capabilities
        return []

    def classify_side_effect(
        self,
        action: ActionPlan,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
    ) -> str:
        """Classify the side effect class of an action.
        
        Args:
            action: The action to classify
            policy_verdict: Optional policy verdict
            
        Returns:
            Side effect classification string
        """
        from ..types.base import SideEffectClass

        # Check if denied by policy before sandbox
        if policy_verdict and policy_verdict.decision == "deny":
            return "denied_before_sandbox"

        # Check if approval blocked
        if policy_verdict and policy_verdict.decision == "approval_required":
            return "approval_blocked"

        # Check action type
        if action.tool_name in {"shell", "git"}:
            return "sandboxed_read"

        if action.tool_name == "http_fetch":
            return "sandboxed_read"

        if action.tool_name == "filesystem":
            fs_action = action.payload.get("action")
            if fs_action == "write_file":
                return "sandboxed_mutation"
            # read_file, list_dir are reads
            return "host_local_read"  # These don't go through sandbox

        return "sandboxed_read"

    def requires_sandbox(self, action: ActionPlan) -> bool:
        """Determine if action requires sandboxed execution.
        
        Args:
            action: The action to check
            
        Returns:
            True if sandbox execution is required
        """
        if action.tool_name in {"shell", "git", "http_fetch"}:
            return True
        return action.tool_name == "filesystem" and action.payload.get("action") == "write_file"


class SandboxExecutorRegistry:
    """Registry for sandbox executor backends.
    
    Maintains a mapping of backend names to executor implementations,
    enabling dynamic backend selection based on configuration.
    """

    def __init__(self) -> None:
        self._executors: Dict[str, SandboxExecutor] = {}
        self._default_backend: Optional[str] = None

    def register(self, name: str, executor: SandboxExecutor, default: bool = False) -> None:
        """Register an executor backend.
        
        Args:
            name: Unique name for this backend
            executor: The executor implementation
            default: Whether to set as default backend
        """
        self._executors[name] = executor
        if default or self._default_backend is None:
            self._default_backend = name

    def get(self, name: str) -> Optional[SandboxExecutor]:
        """Get an executor by name.
        
        Args:
            name: Backend name
            
        Returns:
            The executor implementation or None if not found
        """
        return self._executors.get(name)

    def get_default(self) -> Optional[SandboxExecutor]:
        """Get the default executor.
        
        Returns:
            The default executor or None if none registered
        """
        if self._default_backend is None:
            return None
        return self._executors.get(self._default_backend)

    def list_backends(self) -> List[str]:
        """List all registered backend names.
        
        Returns:
            List of backend names
        """
        return list(self._executors.keys())

    def get_default_backend_name(self) -> Optional[str]:
        """Get the name of the default backend.
        
        Returns:
            Default backend name or None
        """
        return self._default_backend

    def get_all_statuses(self) -> Dict[str, SandboxStatus]:
        """Get status for all registered backends.
        
        Returns:
            Dict mapping backend names to their statuses
        """
        return {name: executor.status() for name, executor in self._executors.items()}

    def get_status(self, active_backend_name: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive executor registry status.
        
        Returns:
            Dict with active backend info and all backend statuses
        """
        from ..utils import utc_now
        
        all_backends: Dict[str, Dict[str, Any]] = {}
        for name, executor in self._executors.items():
            status = executor.status()
            all_backends[name] = {
                "ready": getattr(status, 'executor_ready', False) or getattr(status, 'docker_ready', False),
                "capabilities": executor.capabilities.to_dict(),
                "version": executor.executor_version,
                "backend_name": executor.backend_name,
                "fallback_mode": getattr(status, 'fallback_mode', False),
            }
        
        resolved_active_backend = active_backend_name or self._default_backend
        active_backend = all_backends.get(resolved_active_backend, {}) if resolved_active_backend else {}

        return {
            "active_backend": active_backend,
            "active_backend_name": resolved_active_backend,
            "all_backends": all_backends,
            "backend_count": len(self._executors),
            "timestamp": utc_now(),
        }


class SandboxBackendSelector:
    """Selects appropriate sandbox backend based on configuration and capabilities.
    
    Uses HARNESS_SANDBOX_BACKEND environment variable to determine
    which backend to use, with fallback to default.
    """

    def __init__(self, registry: SandboxExecutorRegistry, settings: Any) -> None:
        self.registry = registry
        self.settings = settings
        self._configured_backend: Optional[str] = getattr(settings, 'sandbox_backend', None)

    def select(self, hint: Optional[str] = None) -> SandboxExecutor:
        """Select the appropriate executor backend.
        
        Selection priority:
        1. Explicit hint from dispatch envelope
        2. Configured backend from settings
        3. Default backend from registry
        
        Args:
            hint: Optional backend hint from control plane
            
        Returns:
            Selected executor implementation
            
        Raises:
            RuntimeError: If no suitable backend is available
        """
        # Priority 1: Explicit hint
        if hint and hint in self.registry.list_backends():
            executor = self.registry.get(hint)
            if executor:
                return executor

        # Priority 2: Configured backend
        if self._configured_backend and self._configured_backend in self.registry.list_backends():
            executor = self.registry.get(self._configured_backend)
            if executor:
                return executor

        # Priority 3: Default backend
        default = self.registry.get_default()
        if default:
            return default

        raise RuntimeError("No sandbox executor backend available")

    def select_with_fallback(self, hint: Optional[str] = None) -> Tuple[SandboxExecutor, bool]:
        """Select backend with fallback mode indicator.
        
        Args:
            hint: Optional backend hint
            
        Returns:
            Tuple of (selected executor, is_fallback_mode)
        """
        try:
            executor = self.select(hint)
            # Check if we're using a fallback
            is_fallback = (
                hint is not None and
                hint != self._configured_backend and
                executor.backend_name != hint
            )
            return executor, is_fallback
        except RuntimeError:
            # If primary selection fails, try to get any available backend
            default = self.registry.get_default()
            if default:
                return default, True
            raise

    def get_configured_backend_name(self) -> str:
        """Get the name of the configured backend.
        
        Returns:
            Configured backend name or "unknown"
        """
        return self._configured_backend or self.registry.get_default_backend_name() or "unknown"

    def is_backend_ready(self, name: Optional[str] = None) -> bool:
        """Check if a backend is ready for execution.
        
        Args:
            name: Backend name to check, or None for configured/default
            
        Returns:
            True if the backend is ready
        """
        if name is None:
            name = self._configured_backend or self.registry.get_default_backend_name()
        if name is None:
            return False

        executor = self.registry.get(name)
        if executor is None:
            return False

        status = executor.status()
        return getattr(status, 'executor_ready', False) or getattr(status, 'docker_ready', False)
