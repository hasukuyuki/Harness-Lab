"""Stub MicroVM sandbox executor for testing backend abstraction.

This module provides a stub implementation of the MicroVM sandbox executor
that returns predictable "not_ready" responses. It is used to:
- Validate the backend abstraction layer works correctly
- Test routing and fallback logic without actual MicroVM infrastructure
- Provide a template for future Firecracker/gVisor implementations
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ..types import (
    ActionPlan,
    PolicyVerdictSnapshot,
    ProbeCheckResult,
    SandboxResult,
    SandboxSpec,
    SandboxStatus,
    SandboxTrace,
)
from ..utils import new_id, utc_now
from .executor import ExecutorCapabilities, SandboxExecutor


class StubMicroVMSandboxExecutor(SandboxExecutor):
    """Stub MicroVM executor that returns not_ready status.
    
    This executor is a placeholder for future Firecracker/gVisor integration.
    It demonstrates the capability model and provides clear error messages
    when MicroVM execution is requested but not available.
    
    Future implementations should:
    - Implement actual VM lifecycle management
    - Support MicroVM-specific features (snapshot, live migration)
    - Integrate with Firecracker or gVisor APIs
    """

    def __init__(self) -> None:
        super().__init__(
            backend_name="microvm_stub",
            executor_version="0.1.0-stub",
        )
        self._capabilities = ExecutorCapabilities.microvm_stub_defaults()
        self._stub_reason = (
            "MicroVM backend is not yet implemented. "
            "This is a stub executor for testing the backend abstraction layer. "
            "Use HARNESS_SANDBOX_BACKEND=microvm or docker for real execution."
        )

    async def execute(
        self,
        action: ActionPlan,
        sandbox_spec: SandboxSpec,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
    ) -> SandboxResult:
        """Return a not_ready result for MicroVM stub.
        
        This method does not perform actual execution. Instead, it returns
        a SandboxResult with ok=False and a clear error message indicating
        that the MicroVM backend is not yet implemented.
        
        This ensures that:
        - Systems using microvm_stub explicitly fail rather than silently fallback
        - Error messages are clear about the backend status
        - Testing can verify the abstraction layer routes correctly
        """
        started_at = utc_now()
        sandbox_id = new_id("sandbox")
        
        # Build a trace that clearly indicates this is a stub backend
        trace = SandboxTrace(
            sandbox_id=sandbox_id,
            sandbox_mode="microvm_stub",
            image=sandbox_spec.image or "harness-lab/microvm:stub",
            container_id=None,  # No actual container in stub mode
            network_policy=sandbox_spec.network_policy,
            started_at=started_at,
            finished_at=utc_now(),
            timed_out=False,
            changed_paths=[],
            used_approval_token=bool(approval_token),
            exit_code=None,
            ok=False,
            error=self._stub_reason,
            docker_command=[],  # No Docker command for MicroVM
            side_effect_class=self.classify_side_effect(action, policy_verdict),
            hardened_config=sandbox_spec.hardened_config,
            evidence=None,  # No evidence from stub execution
            policy_verdict=policy_verdict,
            approval_context=None,
            backend=self.backend_name,
            executor_version=self.executor_version,
        )

        return SandboxResult(
            ok=False,
            stdout="",
            stderr=self._stub_reason,
            exit_code=None,
            timed_out=False,
            changed_paths=[],
            patch="",
            parsed_output={
                "stub": True,
                "backend": self.backend_name,
                "reason": self._stub_reason,
            },
            sandbox_trace=trace,
            error=self._stub_reason,
        )

    def status(self) -> SandboxStatus:
        """Return degraded status for MicroVM stub backend.
        
        The status clearly indicates:
        - executor_ready=False: Backend is not ready for execution
        - fallback_mode=True: System should fallback to another backend
        - executor_capabilities: Shows what the stub supports (nothing)
        - probe_checks: Detailed breakdown of why it's not ready
        """
        probe_checks: List[ProbeCheckResult] = [
            ProbeCheckResult(
                check="microvm_daemon",
                passed=False,
                error="MicroVM daemon (Firecracker/gVisor) is not configured",
            ),
            ProbeCheckResult(
                check="microvm_image",
                passed=False,
                error="MicroVM rootfs image is not available",
            ),
            ProbeCheckResult(
                check="microvm_kernel",
                passed=False,
                error="MicroVM kernel is not configured",
            ),
            ProbeCheckResult(
                check="microvm_snapshot_support",
                passed=False,
                error="MicroVM snapshot support is not implemented (stub)",
            ),
            ProbeCheckResult(
                check="microvm_live_migration",
                passed=False,
                error="MicroVM live migration is not implemented (stub)",
            ),
        ]

        return SandboxStatus(
            sandbox_backend=self.backend_name,
            docker_ready=False,  # MicroVM is not Docker
            sandbox_image_ready=False,
            sandbox_active_runs=0,
            sandbox_failures=0,
            image=None,
            fallback_mode=True,  # Always in fallback mode
            last_probe_error=self._stub_reason,
            last_probe_at=utc_now(),
            hardened_ready=False,
            rootless_ready=False,
            no_new_privileges_ready=False,
            capability_drop_ready=False,
            policy_enforcement_ready=False,
            probe_checks=probe_checks,
            active_sandbox_count=0,
            total_executions_24h=0,
            failure_count_24h=0,
            executor_ready=False,  # Key field: executor is not ready
            executor_capabilities=self._capabilities.to_dict(),
            executor_version=self.executor_version,
        )

    def validate_spec(self, spec: SandboxSpec) -> Tuple[bool, Optional[str]]:
        """Validate that the spec is compatible with MicroVM stub.
        
        The stub accepts all specs but returns validation warnings
        indicating that actual execution will fail.
        
        Returns:
            Tuple of (is_valid=False, error_message)
        """
        return False, (
            f"MicroVM stub backend cannot execute specs. "
            f"Spec image: {spec.image}, "
            f"network_policy: {spec.network_policy}. "
            f"Use docker backend for actual execution."
        )

    def get_stub_info(self) -> Dict[str, Any]:
        """Get detailed information about the stub backend.
        
        Returns:
            Dict with stub metadata for debugging and documentation
        """
        return {
            "backend_name": self.backend_name,
            "executor_version": self.executor_version,
            "is_stub": True,
            "stub_reason": self._stub_reason,
            "capabilities": self._capabilities.to_dict(),
            "future_features": [
                "Firecracker MicroVM lifecycle management",
                "gVisor sandboxed execution",
                "VM snapshot and restore",
                "Live migration between hosts",
                "Custom seccomp profiles",
                "Cgroup v2 resource limits",
            ],
            "migration_path": (
                "To enable MicroVM execution:\n"
                "1. Configure HARNESS_SANDBOX_BACKEND=microvm\n"
                "2. Provide MicroVM rootfs and kernel images\n"
                "3. Point HARNESS_MICROVM_BINARY to a runner binary\n"
                "4. Replace the stub with a production executor if deeper VM isolation is needed\n"
            ),
        }

    def spec_for_action(
        self,
        action: ActionPlan,
        approval_token: Optional[str] = None,
        policy_verdict: Optional[PolicyVerdictSnapshot] = None,
    ) -> SandboxSpec:
        """Generate a MicroVM-compatible sandbox spec.
        
        Even though this is a stub, we generate a proper spec
        that a real MicroVM implementation would use.
        """
        # Call parent to get base spec
        spec = super().spec_for_action(action, approval_token, policy_verdict)
        
        # Override with MicroVM-specific settings
        spec.sandbox_mode = "microvm_stub"
        spec.image = "harness-lab/microvm:stub"  # Stub image reference
        
        return spec
