"""Sandbox and tool execution types."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import SandboxMode, SandboxNetworkPolicy


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


class SandboxTrace(BaseModel):
    """Trace of sandbox execution."""
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


class SandboxStatus(BaseModel):
    """Status of sandbox backend."""
    sandbox_backend: str = "docker"
    docker_ready: bool = False
    sandbox_image_ready: bool = False
    sandbox_active_runs: int = 0
    sandbox_failures: int = 0
    image: Optional[str] = None
    fallback_mode: bool = False
    last_probe_error: Optional[str] = None


class ToolCallRecord(BaseModel):
    """Record of a tool execution."""
    tool_name: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    ok: bool
    output: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    created_at: str
