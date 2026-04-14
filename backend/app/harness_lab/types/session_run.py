"""Session, Run, and Task graph types."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import ActionPlan, ContextLayer, SessionStatus, RunStatus, AgentRole


class ContextBlock(BaseModel):
    """A block of context assembled for prompt rendering."""
    context_block_id: str
    layer: ContextLayer
    type: str
    title: str
    source_ref: str
    content: str
    score: float
    token_estimate: int
    selected: bool
    dependencies: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PromptSection(BaseModel):
    """A section within a prompt frame."""
    section_key: str
    title: str
    content: str
    token_estimate: int
    source_refs: List[str] = Field(default_factory=list)


class PromptFrame(BaseModel):
    """Assembled prompt with sections and metadata."""
    prompt_frame_id: str
    template_id: str
    sections: List[PromptSection] = Field(default_factory=list)
    total_token_estimate: int
    truncated_blocks: List[str] = Field(default_factory=list)
    created_at: str


class TaskNode(BaseModel):
    """A node in the task graph representing a unit of work."""
    node_id: str
    label: str
    kind: str
    role: str = "executor"
    agent_role: AgentRole = "executor"
    status: str = "planned"
    dependencies: List[str] = Field(default_factory=list)
    context_packet_ref: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TaskEdge(BaseModel):
    """An edge in the task graph representing dependency."""
    edge_id: str
    source: str
    target: str
    kind: str = "depends_on"


class TaskGraph(BaseModel):
    """Graph of tasks to be executed."""
    task_graph_id: str
    nodes: List[TaskNode] = Field(default_factory=list)
    edges: List[TaskEdge] = Field(default_factory=list)
    execution_strategy: str = "single_worker_wave_ready"


class ExecutionTrace(BaseModel):
    """Complete trace of a run execution."""
    trace_id: str
    session_id: str
    prompt_frame_id: str
    constraint_document_id: Optional[str] = None
    constraint_root_document_id: Optional[str] = None
    constraint_version: Optional[str] = None
    intent_declaration: "IntentDeclaration"
    model_calls: List["ModelCallTrace"] = Field(default_factory=list)
    context_blocks: List[ContextBlock] = Field(default_factory=list)
    policy_verdicts: List["PolicyVerdict"] = Field(default_factory=list)
    tool_calls: List["ToolCallRecord"] = Field(default_factory=list)
    recovery_events: List["RecoveryEvent"] = Field(default_factory=list)
    artifacts: List["ArtifactRef"] = Field(default_factory=list)
    status: str
    created_at: str
    updated_at: str


class RolloutInfo(BaseModel):
    """Rollout metadata for tracking canary/baseline cohort assignment."""
    candidate_id: Optional[str] = None
    target_version_id: Optional[str] = None
    rollout_ring: Optional[str] = None  # baseline, candidate, default
    cohort: Optional[str] = None  # baseline, canary
    matched_scope: Optional[Dict[str, Any]] = None  # {type, value, description}
    rollout_reason: Optional[str] = None  # e.g., "percentage_match", "session_tag", "explicit_override"
    recorded_at: Optional[str] = None


class ResearchSession(BaseModel):
    """A research session with goal and configuration."""
    session_id: str
    goal: str
    status: SessionStatus
    active_policy_id: str
    workflow_template_id: Optional[str] = None
    constraint_set_id: str
    constraint_root_document_id: Optional[str] = None
    constraint_version: Optional[str] = None
    context_profile_id: str
    prompt_template_id: str
    model_profile_id: str
    execution_mode: str
    context: Dict[str, Any] = Field(default_factory=dict)
    intent_declaration: Optional["IntentDeclaration"] = None
    intent_model_call: Optional["ModelCallTrace"] = None
    task_graph: Optional[TaskGraph] = None
    # Rollout tracking
    rollout_info: Optional[RolloutInfo] = None
    tags: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str


class ResearchRun(BaseModel):
    """An execution run of a session."""
    run_id: str
    session_id: str
    status: RunStatus
    mission_id: Optional[str] = None
    policy_id: Optional[str] = None
    workflow_template_id: Optional[str] = None
    constraint_set_id: Optional[str] = None
    constraint_root_document_id: Optional[str] = None
    constraint_version: Optional[str] = None
    assigned_worker_id: Optional[str] = None
    current_attempt_id: Optional[str] = None
    active_lease_id: Optional[str] = None
    prompt_frame: Optional[PromptFrame] = None
    execution_trace: Optional[ExecutionTrace] = None
    result: Dict[str, Any] = Field(default_factory=dict)
    # Rollout tracking - mirrors session but allows per-run granularity
    rollout_info: Optional[RolloutInfo] = None
    cohort: Optional[str] = None  # baseline, canary (deprecated: use rollout_info.cohort)
    created_at: str
    updated_at: str


class ExperimentRun(BaseModel):
    """An experiment comparing multiple harness configurations."""
    experiment_id: str
    scenario_suite: str
    harness_ids: List[str] = Field(default_factory=list)
    status: str
    metrics: Dict[str, Any] = Field(default_factory=dict)
    trace_refs: List[str] = Field(default_factory=list)
    winner: Optional[str] = None
    created_at: str
    updated_at: str


class IntentDeclaration(BaseModel):
    """Declared intent from natural language goal."""
    intent_id: str
    task_type: str
    intent: str
    confidence: float
    risk_mode: str
    suggested_action: ActionPlan
    model_profile_id: str
    created_at: str


# Request types
class ContextAssembleRequest(BaseModel):
    """Request to assemble context blocks."""
    goal: Optional[str] = None
    session_id: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    context_profile_id: Optional[str] = None


class PromptRenderRequest(BaseModel):
    """Request to render prompt frame."""
    session_id: str


class SessionRequest(BaseModel):
    """Request to create a session."""
    goal: str
    context: Dict[str, Any] = Field(default_factory=dict)
    constraint_set_id: Optional[str] = None
    context_profile_id: Optional[str] = None
    prompt_template_id: Optional[str] = None
    model_profile_id: Optional[str] = None
    workflow_template_id: Optional[str] = None
    execution_mode: str = "single_worker"


class RunRequest(BaseModel):
    """Request to create and start a run."""
    session_id: Optional[str] = None
    goal: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    constraint_set_id: Optional[str] = None
    context_profile_id: Optional[str] = None
    prompt_template_id: Optional[str] = None
    model_profile_id: Optional[str] = None
    workflow_template_id: Optional[str] = None
    execution_mode: str = "single_worker"


class IntentRequest(BaseModel):
    """Request to declare intent."""
    goal: str
    session_id: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    model_profile_id: Optional[str] = None
