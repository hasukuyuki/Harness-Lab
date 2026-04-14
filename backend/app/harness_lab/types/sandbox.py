"""Sandbox and tool execution types."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import SandboxMode, SandboxNetworkPolicy, SideEffectClass


class HardenedSandboxConfig(BaseModel):
    """Hardened sandbox configuration applied to container."""
    no_new_privileges: bool = True
    cap_drop_all: bool = True
    cap_add_whitelist: List[str] = Field(default_factory=list)
    rootless_user: str = "1000:1000"
    read_only_rootfs: bool = True
    security_options: List[str] = Field(default_factory=list)


class ExecutionTiming(BaseModel):
    """Execution timing metadata."""
    started_at: str
    finished_at: str
    duration_ms: int


class MountInfo(BaseModel):
    """Container mount information."""
    source: str
    destination: str
    mode: str  # "ro" or "rw"
    mount_type: str  # "bind" or "tmpfs"


class ContainerMetadata(BaseModel):
    """Container execution metadata for audit trails."""
    container_id: str
    image: str
    created_at: str
    started_at: str
    finished_at: str
    security_options: List[str] = Field(default_factory=list)
    dropped_capabilities: List[str] = Field(default_factory=list)
    added_capabilities: List[str] = Field(default_factory=list)
    user: str = "root"
    mounts: List[MountInfo] = Field(default_factory=list)
    network_mode: str = "none"


class SandboxEvidence(BaseModel):
    """Complete evidence package from sandbox execution."""
    stdout: str = ""
    stderr: str = ""
    exit_code: Optional[int] = None
    changed_paths: List[str] = Field(default_factory=list)
    patch: str = ""  # unified diff
    container_metadata: Optional[ContainerMetadata] = None
    execution_timing: Optional[ExecutionTiming] = None


class PolicyVerdictSnapshot(BaseModel):
    """Policy verdict snapshot for sandbox trace."""
    decision: str  # "allow", "approval_required", "deny"
    subject: str
    rule_id: Optional[str] = None
    matched_rule: Optional[str] = None


class ApprovalContext(BaseModel):
    """Approval context for sandboxed mutations."""
    approval_token: Optional[str] = None
    approval_id: Optional[str] = None
    decision: Optional[str] = None  # "approve", "approve_once", "deny"
    used: bool = False


class SandboxSpec(BaseModel):
    """Specification for sandbox execution."""
    sandbox_mode: SandboxMode = "docker"
    image: str
    workspace_mount: str = "/workspace"
    working_dir: str = "/workspace"
    network_policy: SandboxNetworkPolicy = "none"
    read_only_rootfs: bool = True
    timeout_seconds: int = 20
    approval_token: Optional[str] = None
    # Hardened config
    hardened_config: Optional[HardenedSandboxConfig] = None
    # Backend hint for executor selection
    backend_hint: Optional[str] = None


class SandboxTrace(BaseModel):
    """Trace of sandbox execution with hardened audit trails."""
    sandbox_id: str
    sandbox_mode: SandboxMode = "docker"
    image: str
    container_id: Optional[str] = None
    network_policy: SandboxNetworkPolicy = "none"
    started_at: str
    finished_at: str
    timed_out: bool = False
    changed_paths: List[str] = Field(default_factory=list)
    used_approval_token: bool = False
    exit_code: Optional[int] = None
    ok: bool = False
    error: Optional[str] = None
    docker_command: List[str] = Field(default_factory=list)
    # Hardened fields
    side_effect_class: SideEffectClass = "sandboxed_read"
    hardened_config: Optional[HardenedSandboxConfig] = None
    evidence: Optional[SandboxEvidence] = None
    policy_verdict: Optional[PolicyVerdictSnapshot] = None
    approval_context: Optional[ApprovalContext] = None
    # Backend identification for cross-backend replay and troubleshooting
    backend: Optional[str] = None
    executor_version: Optional[str] = None
    vm_id: Optional[str] = None
    guest_image: Optional[str] = None
    kernel_image: Optional[str] = None
    snapshot_id: Optional[str] = None


class SandboxResult(BaseModel):
    """Result of sandbox execution."""
    ok: bool
    stdout: str = ""
    stderr: str = ""
    exit_code: Optional[int] = None
    timed_out: bool = False
    changed_paths: List[str] = Field(default_factory=list)
    patch: str = ""
    parsed_output: Dict[str, Any] = Field(default_factory=dict)
    sandbox_trace: SandboxTrace
    error: Optional[str] = None


class ProbeCheckResult(BaseModel):
    """Individual probe check result for sandbox readiness."""
    check: str
    passed: bool
    error: Optional[str] = None


class SandboxStatus(BaseModel):
    """Status of sandbox backend with hardened readiness."""
    sandbox_backend: str = "docker"
    docker_ready: bool = False
    sandbox_image_ready: bool = False
    sandbox_active_runs: int = 0
    sandbox_failures: int = 0
    image: Optional[str] = None
    fallback_mode: bool = False
    last_probe_error: Optional[str] = None
    last_probe_at: Optional[str] = None
    # Hardened readiness
    hardened_ready: bool = False
    rootless_ready: bool = False
    no_new_privileges_ready: bool = False
    capability_drop_ready: bool = False
    policy_enforcement_ready: bool = False
    probe_checks: List[ProbeCheckResult] = Field(default_factory=list)
    # Runtime stats
    active_sandbox_count: int = 0
    total_executions_24h: int = 0
    failure_count_24h: int = 0
    # Executor abstraction fields
    executor_ready: bool = False
    executor_capabilities: Dict[str, bool] = Field(default_factory=dict)
    executor_version: Optional[str] = None
    executor_status: Optional[Dict[str, Any]] = Field(default=None, description="Detailed executor status from registry")


class ToolCallRecord(BaseModel):
    """Record of a tool execution."""
    tool_name: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    ok: bool
    output: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    created_at: str
