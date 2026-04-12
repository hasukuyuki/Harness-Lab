"""Base types and literal definitions."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# Status Literals
SessionStatus = Literal[
    "configured",
    "running",
    "awaiting_approval",
    "awaiting_escalation",
    "completed",
    "failed",
]
RunStatus = Literal[
    "queued",
    "planning",
    "running",
    "awaiting_approval",
    "awaiting_escalation",
    "recovering",
    "completed",
    "failed",
    "cancelled",
]
ConstraintStatus = Literal["candidate", "published", "archived"]
ProfileStatus = Literal["candidate", "published", "archived"]
VerdictDecision = Literal["allow", "approval_required", "deny"]
ApprovalDecision = Literal["approve", "deny", "approve_once"]
ApprovalStatus = Literal["pending", "approved", "denied", "consumed"]
ContextLayer = Literal["structure", "task", "history", "index"]
CandidateKind = Literal["policy", "workflow"]
CandidatePublishStatus = Literal[
    "draft",
    "evaluating",
    "awaiting_approval",
    "publish_ready",
    "canary",          # In canary rollout phase
    "published",       # Fully promoted to default
    "rolled_back",     # Rolled back to baseline
    "rejected",        # Failed evaluation
]

# Rollout ring definitions
RolloutRing = Literal["baseline", "candidate", "default"]

# Canary scope types
CanaryScopeType = Literal[
    "session_tag",
    "worker_label",
    "goal_pattern",
    "explicit_override",
    "percentage",
]
EvaluationStatus = Literal["pending", "passed", "failed"]
EvaluationSuite = Literal["replay", "benchmark"]
WorkerState = Literal["registering", "idle", "leased", "executing", "draining", "offline", "unhealthy"]
MissionStatus = Literal["queued", "running", "awaiting_approval", "completed", "failed", "cancelled"]
AttemptStatus = Literal["leased", "running", "completed", "failed", "blocked", "released", "expired"]
LeaseStatus = Literal["leased", "running", "completed", "failed", "released", "expired"]
KnowledgeSourceType = Literal["workspace", "docs", "artifacts"]
KnowledgeReindexScope = Literal["workspace", "docs", "artifacts", "all"]
SandboxMode = Literal["host_local", "docker"]
SandboxNetworkPolicy = Literal["none", "restricted", "default"]
AgentRole = Literal["planner", "researcher", "executor", "reviewer", "recovery"]
ReviewDecision = Literal["accept", "request_repair", "escalate", "complete"]
FailureClusterSignatureType = Literal[
    "handoff_breakdown",
    "review_reject_loop",
    "repair_path_failure",
    "approval_sandbox_friction",
    "role_mismatch_or_starvation",
]


class ActionPlan(BaseModel):
    """Planned action to be executed."""
    tool_name: str
    subject: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    summary: str = ""


class ModelProviderSettings(BaseModel):
    """Configuration for model provider."""
    provider: str
    api_key_present: bool
    base_url: str
    model_name: str
    model_ready: bool
    fallback_mode: bool


class ModelCallTrace(BaseModel):
    """Trace of a model API call."""
    provider: str
    model_name: str
    latency_ms: int
    used_fallback: bool = False
    failure_reason: Optional[str] = None
