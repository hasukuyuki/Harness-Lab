"""Policy, Constraint, and Approval types."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import (
    ConstraintStatus,
    ProfileStatus,
    VerdictDecision,
    ApprovalDecision,
    ApprovalStatus,
)


class PolicyVerdict(BaseModel):
    """Verdict from policy constraint evaluation."""
    verdict_id: str
    subject: str
    decision: VerdictDecision
    reason: str
    matched_rule: str
    created_at: str


class ConstraintDocument(BaseModel):
    """A document containing natural language constraints."""
    document_id: str
    title: str
    body: str
    scope: str
    status: ConstraintStatus
    tags: List[str] = Field(default_factory=list)
    priority: int = 50
    source: str = "manual"
    version: str = "v1"
    created_at: str
    updated_at: str


class ContextProfile(BaseModel):
    """Profile for context assembly."""
    context_profile_id: str
    name: str
    description: str
    status: ProfileStatus
    config: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class PromptTemplate(BaseModel):
    """Template for prompt rendering."""
    prompt_template_id: str
    name: str
    description: str
    status: ProfileStatus
    sections: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str


class ModelProfile(BaseModel):
    """Profile for model configuration."""
    model_profile_id: str
    name: str
    provider: str
    profile: str
    status: ProfileStatus
    config: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class HarnessPolicy(BaseModel):
    """Complete harness policy combining constraints, context, prompt, and model."""
    policy_id: str
    name: str
    status: ProfileStatus
    constraint_set_id: str
    context_profile_id: str
    prompt_template_id: str
    model_profile_id: str
    tool_policy: Dict[str, Any] = Field(default_factory=dict)
    model_routing: Dict[str, Any] = Field(default_factory=dict)
    repair_policy: Dict[str, Any] = Field(default_factory=dict)
    budget_policy: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class WorkflowTemplateVersion(BaseModel):
    """Versioned workflow template."""
    workflow_id: str
    parent_id: Optional[str] = None
    name: str
    description: str
    scope: str = "global"
    status: ProfileStatus
    dag: Dict[str, Any] = Field(default_factory=dict)
    role_map: Dict[str, Any] = Field(default_factory=dict)
    gates: List[Dict[str, Any]] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class ApprovalRequestModel(BaseModel):
    """Model for approval request."""
    approval_id: str
    run_id: str
    verdict_id: str
    subject: str
    summary: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    status: ApprovalStatus
    decision: Optional[ApprovalDecision] = None
    created_at: str
    updated_at: str


# Request types
class ConstraintCreateRequest(BaseModel):
    """Request to create a constraint document."""
    title: str
    body: str
    scope: str = "global"
    tags: List[str] = Field(default_factory=list)
    priority: int = 50
    source: str = "manual"


class ConstraintVerifyRequest(BaseModel):
    """Request to verify constraints."""
    subject: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    constraint_set_id: Optional[str] = None


class PolicyCompareRequest(BaseModel):
    """Request to compare policies."""
    policy_ids: List[str] = Field(default_factory=list)


class WorkflowCompareRequest(BaseModel):
    """Request to compare workflows."""
    workflow_ids: List[str] = Field(default_factory=list)


class ApprovalDecisionRequest(BaseModel):
    """Request to make an approval decision."""
    decision: ApprovalDecision
