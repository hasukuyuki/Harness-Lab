"""Worker, Lease, and Fleet types."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from .base import AgentRole, WorkerState, MissionStatus, AttemptStatus, LeaseStatus


class WorkerSnapshot(BaseModel):
    """Snapshot of worker state."""
    worker_id: str
    label: str
    state: WorkerState
    drain_state: Literal["active", "draining"] = "active"
    capabilities: List[str] = Field(default_factory=list)
    role_profile: Optional[AgentRole] = None
    hostname: Optional[str] = None
    pid: Optional[int] = None
    labels: List[str] = Field(default_factory=list)
    eligible_labels: List[str] = Field(default_factory=list)
    worker_class: str = "general"
    execution_mode: str = "embedded"
    heartbeat_at: str
    lease_count: int = 0
    version: str = "v1"
    current_run_id: Optional[str] = None
    current_task_node_id: Optional[str] = None
    current_lease_id: Optional[str] = None
    sandbox_backend: Optional[str] = None
    sandbox_ready: bool = False
    last_error: Optional[str] = None
    created_at: str
    updated_at: str


class Mission(BaseModel):
    """A mission representing a run's execution mission."""
    mission_id: str
    session_id: str
    run_id: str
    status: MissionStatus
    created_at: str
    updated_at: str


class TaskAttempt(BaseModel):
    """An attempt to execute a task node."""
    attempt_id: str
    run_id: str
    task_node_id: str
    worker_id: Optional[str] = None
    lease_id: Optional[str] = None
    status: AttemptStatus
    retry_index: int = 0
    summary: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    created_at: str
    updated_at: str


class WorkerLease(BaseModel):
    """A lease granting a worker exclusive rights to execute a task."""
    lease_id: str
    worker_id: str
    run_id: str
    task_node_id: str
    attempt_id: str
    status: LeaseStatus
    approval_token: Optional[str] = None
    expires_at: str
    heartbeat_at: str
    created_at: str
    updated_at: str


class DispatchEnvelope(BaseModel):
    """Envelope containing task dispatch information for a worker."""
    lease_id: str
    attempt_id: str
    run_id: str
    task_node_id: str
    task_node: "TaskNode"
    intent: "IntentDeclaration"
    mission_id: Optional[str] = None
    context_packet_ref: Optional[str] = None
    prompt_frame_ref: Optional[str] = None
    policy_verdicts: List["PolicyVerdict"] = Field(default_factory=list)
    budget: Dict[str, Any] = Field(default_factory=dict)
    tool_policy: Dict[str, Any] = Field(default_factory=dict)
    approval_token: Optional[str] = None
    agent_role: AgentRole = "executor"
    handoff_packet_ref: Optional[str] = None
    mission_phase: Optional[str] = None
    sandbox_mode: "SandboxMode" = "host_local"
    sandbox_spec: Optional["SandboxSpec"] = None
    network_policy: "SandboxNetworkPolicy" = "none"
    requires_sandbox: bool = False
    required_labels: List[str] = Field(default_factory=list)
    preferred_labels: List[str] = Field(default_factory=list)
    queue_shard: str = "default"
    lease_timeout_seconds: int = 30
    heartbeat_interval_seconds: int = 10
    run_status_hint: Optional[str] = None
    created_at: str


class WorkerEventRecord(BaseModel):
    """A single event from a worker."""
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class WorkerEventBatch(BaseModel):
    """Batch of events from a worker."""
    lease_id: str
    events: List[WorkerEventRecord] = Field(default_factory=list)
    model_calls: List["ModelCallTrace"] = Field(default_factory=list)
    tool_calls: List["ToolCallRecord"] = Field(default_factory=list)
    artifacts: List["ArtifactRef"] = Field(default_factory=list)
    recovery_events: List["RecoveryEvent"] = Field(default_factory=list)
    emitted_at: str


class LeaseTransitionContext(BaseModel):
    """Context for a lease state transition."""
    lease: WorkerLease
    attempt: TaskAttempt
    run: "ResearchRun"
    session: "ResearchSession"
    task_node: "TaskNode"
    timestamp: str


class RunCoordinationSnapshot(BaseModel):
    """Snapshot of run coordination state."""
    run_id: str
    mission_status: Optional[str] = None
    counts_by_status: Dict[str, int] = Field(default_factory=dict)
    node_ids_by_status: Dict[str, List[str]] = Field(default_factory=dict)
    dispatch_blockers: List[Dict[str, Any]] = Field(default_factory=list)
    active_lease_id: Optional[str] = None
    current_attempt_id: Optional[str] = None
    updated_at: str


class WorkerHealthSummary(BaseModel):
    """Summary of worker health."""
    worker_id: str
    derived_state: WorkerState
    active_lease_count: int = 0
    recent_lease_ids: List[str] = Field(default_factory=list)
    recent_error_events: List[Dict[str, Any]] = Field(default_factory=list)
    last_event_types: List[str] = Field(default_factory=list)
    last_heartbeat_at: str
    current_run_id: Optional[str] = None
    current_task_node_id: Optional[str] = None


class StuckRunSummary(BaseModel):
    """Summary of a potentially stuck run."""
    run_id: str
    session_id: str
    status: "RunStatus"
    mission_status: Optional[MissionStatus] = None
    reason: str
    age_seconds: int
    updated_at: str


class DispatchConstraint(BaseModel):
    """Constraints for dispatching a task to a worker."""
    agent_role: Optional[AgentRole] = None
    required_capabilities: List[str] = Field(default_factory=list)
    required_labels: List[str] = Field(default_factory=list)
    preferred_labels: List[str] = Field(default_factory=list)
    execution_mode: Optional[str] = None
    requires_sandbox: bool = False
    risk_level: str = "low"
    queue_shard: str = "default"


class WorkerDrainRequest(BaseModel):
    """Request to drain a worker."""
    reason: Optional[str] = None


class QueueShardStatus(BaseModel):
    """Status of a queue shard."""
    shard: str
    depth: int
    sample_tasks: List[Dict[str, str]] = Field(default_factory=list)


class FleetStatusReport(BaseModel):
    """Report on fleet status."""
    worker_count: int = 0
    active_workers: List[str] = Field(default_factory=list)
    draining_workers: List[str] = Field(default_factory=list)
    offline_workers: List[str] = Field(default_factory=list)
    unhealthy_workers: List[str] = Field(default_factory=list)
    workers_by_role: Dict[str, int] = Field(default_factory=dict)
    queue_depth_by_shard: Dict[str, int] = Field(default_factory=dict)
    lease_reclaim_rate: float = 0.0
    stuck_run_count: int = 0
    late_callback_count: int = 0


# Request types
class WorkerRegisterRequest(BaseModel):
    """Request to register a worker."""
    worker_id: Optional[str] = None
    label: str = "local-worker"
    capabilities: List[str] = Field(default_factory=list)
    role_profile: Optional[AgentRole] = None
    hostname: Optional[str] = None
    pid: Optional[int] = None
    labels: List[str] = Field(default_factory=list)
    execution_mode: str = "embedded"
    sandbox_backend: Optional[str] = None
    sandbox_ready: bool = False
    version: str = "v1"


class WorkerHeartbeatRequest(BaseModel):
    """Request to update worker heartbeat."""
    state: WorkerState = "idle"
    lease_count: int = 0
    current_run_id: Optional[str] = None
    current_task_node_id: Optional[str] = None
    current_lease_id: Optional[str] = None
    role_profile: Optional[AgentRole] = None
    sandbox_backend: Optional[str] = None
    sandbox_ready: Optional[bool] = None
    last_error: Optional[str] = None


class WorkerPollRequest(BaseModel):
    """Request to poll for tasks."""
    max_tasks: int = 1


class WorkerPollResponse(BaseModel):
    """Response to worker poll."""
    dispatches: List[DispatchEnvelope] = Field(default_factory=list)


class LeaseSweepReport(BaseModel):
    """Report of lease sweep operation."""
    scanned: int = 0
    reclaimed: int = 0
    expired_lease_ids: List[str] = Field(default_factory=list)


class LeaseCompletionRequest(BaseModel):
    """Request to complete a lease."""
    worker_event_batch: Optional[WorkerEventBatch] = None
    summary: Optional[str] = None


class LeaseFailureRequest(BaseModel):
    """Request to fail a lease."""
    worker_event_batch: Optional[WorkerEventBatch] = None
    error: str


class LeaseReleaseRequest(BaseModel):
    """Request to release a lease."""
    reason: Optional[str] = None
