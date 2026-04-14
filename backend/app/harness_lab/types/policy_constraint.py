"""Policy, Constraint, and Approval types."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from .base import (
    ConstraintStatus,
    ProfileStatus,
    VerdictDecision,
    ApprovalDecision,
    ApprovalStatus,
)


class ConstraintMatch(BaseModel):
    """Result of matching a single rule against a context."""
    rule_id: str
    matched: bool
    condition_results: List[Dict[str, Any]] = Field(default_factory=list)
    context_snapshot: Dict[str, Any] = Field(default_factory=dict)


class MatchedRuleInfo(BaseModel):
    """Information about a matched rule for explanation purposes."""
    rule_id: str
    subject_pattern: str
    decision: VerdictDecision
    priority: int
    source_document_id: Optional[str] = None
    source_document_version: Optional[str] = None
    matched_conditions: List[str] = Field(default_factory=list)
    reason: str


class ConstraintExplanation(BaseModel):
    """Detailed explanation of constraint evaluation."""
    subject: str
    final_decision: VerdictDecision
    final_reason: str
    matched_rules: List[MatchedRuleInfo] = Field(default_factory=list)
    evaluated_rules: int = 0
    used_fallback: bool = False
    fallback_reason: Optional[str] = None
    compilation_status: str = "not_compiled"  # success, partial, failed, not_compiled
    compiled_rule_count: int = 0
    context_snapshot: Dict[str, Any] = Field(default_factory=dict)


class PolicyVerdict(BaseModel):
    """Verdict from policy constraint evaluation."""
    verdict_id: str
    subject: str
    decision: VerdictDecision
    reason: str
    matched_rule: str
    created_at: str
    # Enhanced fields for semantic constraints
    rule_id: Optional[str] = None  # Stable rule identifier
    source_document_id: Optional[str] = None
    source_document_version: Optional[str] = None
    used_fallback: bool = False  # Whether this verdict used fallback logic
    explanation_summary: Optional[str] = None  # Human-readable summary


class ConstraintCompileSummary(BaseModel):
    """Summary of constraint compilation status."""
    status: str = "not_compiled"  # success, partial, failed, not_compiled
    compiled_at: Optional[str] = None
    rule_count: int = 0
    errors: List[str] = Field(default_factory=list)
    used_fallback: bool = False


class ConstraintCompileResult(BaseModel):
    """Result of constraint compilation."""
    document_id: str
    status: str  # success, partial, failed
    rules_compiled: int
    errors: List[str] = Field(default_factory=list)
    used_fallback: bool = False
    fallback_reason: Optional[str] = None


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
    # Version chain fields for governance
    root_document_id: Optional[str] = None  # Original document ID in the chain (self for new docs)
    parent_document_id: Optional[str] = None  # Previous version if this is a revision
    # Extended payload field for compiled rules (stored in payload_json)
    compiled: Optional[ConstraintCompileSummary] = None

    @model_validator(mode="before")
    @classmethod
    def _hydrate_version_chain_defaults(cls, data: Any) -> Any:
        """Backfill governance fields for bootstrap and legacy documents."""
        if isinstance(data, dict):
            document_id = data.get("document_id")
            data.setdefault("root_document_id", document_id)
            data.setdefault("parent_document_id", None)
        return data


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


class ConstraintVerifyResponse(BaseModel):
    """Enhanced response for constraint verification."""
    constraint_document_id: str
    constraint_root_document_id: str
    constraint_document_version: str
    verdicts: List[PolicyVerdict]
    final_verdict: PolicyVerdict
    explanation: ConstraintExplanation
    compiled_rule_count: int
    used_fallback: bool
    matched_rules: List[MatchedRuleInfo]


class ConstraintScenario(BaseModel):
    """Saved validation scenario for a constraint chain."""
    scenario_id: str
    root_document_id: str
    name: str
    subject: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    expected_decision: VerdictDecision
    tags: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str


class ConstraintScenarioCreateRequest(BaseModel):
    """Request to create a validation scenario."""
    root_document_id: str
    name: str
    subject: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    expected_decision: VerdictDecision
    tags: List[str] = Field(default_factory=list)


class ConstraintValidateRequest(BaseModel):
    """Request to validate a candidate constraint document."""
    scenario_ids: List[str] = Field(default_factory=list)


class ConstraintScenarioResult(BaseModel):
    """Validation result for a single saved scenario."""
    scenario_id: str
    name: str
    expected_decision: VerdictDecision
    actual_decision: VerdictDecision
    passed: bool
    hard_failure: bool = False
    used_fallback: bool = False
    matched_rule_ids: List[str] = Field(default_factory=list)
    matched_document_ids: List[str] = Field(default_factory=list)
    explanation: str


class ConstraintValidationReport(BaseModel):
    """Validation report for a constraint document candidate."""
    report_id: str
    document_id: str
    root_document_id: str
    document_version: str
    status: str
    compilation_status: str
    compiled_rule_count: int
    total_scenarios: int
    passed_scenarios: int
    failed_scenarios: int
    hard_failure_count: int = 0
    soft_deviation_count: int = 0
    blockers: List[str] = Field(default_factory=list)
    scenario_results: List[ConstraintScenarioResult] = Field(default_factory=list)
    created_at: str
    updated_at: str


class ConstraintPublishGateStatus(BaseModel):
    """Publish readiness for a constraint document candidate."""
    document_id: str
    root_document_id: str
    document_version: str
    publish_ready: bool
    compilation_ok: bool
    validation_ok: bool
    scenario_count: int
    hard_failure_count: int
    blockers: List[str] = Field(default_factory=list)
    latest_validation_report: Optional[ConstraintValidationReport] = None


class PolicyCompareRequest(BaseModel):
    """Request to compare policies."""
    policy_ids: List[str] = Field(default_factory=list)


class WorkflowCompareRequest(BaseModel):
    """Request to compare workflows."""
    workflow_ids: List[str] = Field(default_factory=list)


class ApprovalDecisionRequest(BaseModel):
    """Request to make an approval decision."""
    decision: ApprovalDecision


class ConstraintEngineStatus(BaseModel):
    """Status of the constraint engine for health endpoints."""
    constraint_engine_version: str = "v2"
    constraint_parser_ready: bool = True
    constraint_compiler_ready: bool = True
    constraint_fallback_mode: bool = False
    published_constraint_count: int = 0
    total_constraint_count: int = 0


class RuleCondition(BaseModel):
    """A single condition within a constraint rule."""
    field: str  # tool_name, action, path, command, network_mode, etc.
    operator: str  # eq, ne, contains, prefix, suffix, regex, in, not_in
    value: Any
    description: Optional[str] = None


class ConstraintRule(BaseModel):
    """A standardized constraint rule."""
    rule_id: str
    source_document_id: str
    subject_pattern: str
    conditions: List[RuleCondition] = Field(default_factory=list)
    decision: VerdictDecision
    priority: int = 50
    reason_template: str
    tags: List[str] = Field(default_factory=list)
    created_at: str
    
    def render_reason(self, context: Dict[str, Any]) -> str:
        """Render the reason template with runtime context."""
        try:
            return self.reason_template.format(**context)
        except (KeyError, ValueError):
            return self.reason_template


class CompiledConstraintSet(BaseModel):
    """A compiled set of constraint rules derived from a ConstraintDocument."""
    compiled_at: str
    document_id: str
    document_version: str
    rules: List[ConstraintRule] = Field(default_factory=list)
    compilation_status: str = "success"  # success, partial, failed
    compilation_errors: List[str] = Field(default_factory=list)
    used_fallback: bool = False
    fallback_reason: Optional[str] = None
    
    def get_rules_for_subject(self, subject: str) -> List[ConstraintRule]:
        """Get all rules that match the given subject."""
        matching = []
        for rule in self.rules:
            # Simple glob-style matching
            pattern = rule.subject_pattern
            if pattern == subject:
                matching.append(rule)
            elif pattern.endswith(".*") and subject.startswith(pattern[:-1]):
                matching.append(rule)
            elif pattern == "*":
                matching.append(rule)
            elif "*" in pattern:
                # Convert simple glob to prefix/suffix matching
                parts = pattern.split("*")
                if len(parts) == 2 and subject.startswith(parts[0]) and subject.endswith(parts[1]):
                    matching.append(rule)
        return sorted(matching, key=lambda r: r.priority)
