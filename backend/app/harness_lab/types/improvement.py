"""Improvement, evaluation, and failure cluster types."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from .base import CandidateKind, CandidatePublishStatus, EvaluationStatus, EvaluationSuite


class CanaryScope(BaseModel):
    """Scope definition for canary rollout."""
    scope_type: str  # session_tag, worker_label, goal_pattern, explicit_override, percentage
    scope_value: str
    description: Optional[str] = None


class BucketMetrics(BaseModel):
    """Metrics for a specific bucket (handoff, review, etc.)."""
    bucket_name: str
    baseline_count: int = 0
    canary_count: int = 0
    baseline_passed: int = 0
    canary_passed: int = 0
    baseline_failed: int = 0
    canary_failed: int = 0
    pass_rate_delta: float = 0.0
    regression_detected: bool = False


class CanaryMetrics(BaseModel):
    """Metrics collected during canary rollout - supports long-term observation."""
    # Sample sizes
    baseline_sample_size: int = 0
    canary_sample_size: int = 0
    
    # Core rates
    baseline_success_rate: float = 0.0
    canary_success_rate: float = 0.0
    baseline_safety_score: float = 0.0
    canary_safety_score: float = 0.0
    baseline_recovery_rate: float = 0.0
    canary_recovery_rate: float = 0.0
    baseline_approval_rate: float = 0.0
    canary_approval_rate: float = 0.0
    baseline_repair_rate: float = 0.0
    canary_repair_rate: float = 0.0
    
    # Deltas
    success_delta: float = 0.0
    safety_delta: float = 0.0
    recovery_delta: float = 0.0
    approval_delta: float = 0.0
    repair_delta: float = 0.0
    
    # Bucket-level metrics (handoff, review, approval_sandbox, role_dispatch)
    bucket_metrics: List[BucketMetrics] = Field(default_factory=list)
    
    # Regression detection
    regression_detected: bool = False
    regression_buckets: List[str] = Field(default_factory=list)
    
    # Sample sufficiency
    sufficient_sample: bool = False
    minimum_sample_reached: bool = False
    
    # Timestamps for long-term observation
    calculated_at: Optional[str] = None
    analysis_window_start: Optional[str] = None
    analysis_window_end: Optional[str] = None
    
    # Key blockers identified
    top_blockers: List[str] = Field(default_factory=list)
    failing_cohorts: List[str] = Field(default_factory=list)


class RolloutSnapshot(BaseModel):
    """Snapshot of rollout state for rollback records."""
    ring: str
    scope: Optional[CanaryScope] = None
    baseline_version_id: Optional[str] = None
    canary_metrics: Optional[CanaryMetrics] = None
    recommendation: Optional[str] = None  # promote, hold, rollback at snapshot time
    recommendation_reason: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None


class ImprovementCandidate(BaseModel):
    """Candidate improvement to policy or workflow."""
    candidate_id: str
    kind: CandidateKind
    target_id: str
    target_version_id: str
    baseline_version_id: Optional[str] = None
    change_set: Dict[str, Any] = Field(default_factory=dict)
    rationale: str
    eval_status: str = "pending"
    publish_status: CandidatePublishStatus = "draft"
    approved: bool = False
    requires_human_approval: bool = False
    # Rollout fields
    rollout_ring: Optional[str] = None  # baseline, candidate, default
    rollout_scope: Optional[CanaryScope] = None
    canary_metrics: Optional[CanaryMetrics] = None
    rollout_started_at: Optional[str] = None
    rollout_snapshot: Optional[RolloutSnapshot] = None  # For rollback records
    metrics: Dict[str, Any] = Field(default_factory=dict)
    evaluation_ids: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str


class EvaluationFailure(BaseModel):
    """A failure during evaluation."""
    kind: str
    severity: Literal["hard", "soft"]
    bucket: Optional[str] = None
    trace_ref: Optional[str] = None
    summary: str


class BenchmarkBucketResult(BaseModel):
    """Results for a benchmark bucket."""
    bucket: str
    total: int
    passed: int
    failed: int
    coverage: float
    regressions: List[str] = Field(default_factory=list)


class EvaluationSuiteManifest(BaseModel):
    """Manifest of an evaluation suite."""
    suite_id: str
    source: str
    trace_refs: List[str] = Field(default_factory=list)
    bucket_map: Dict[str, List[str]] = Field(default_factory=dict)
    eligibility: Dict[str, Any] = Field(default_factory=dict)
    generated_at: str


class EvaluationReport(BaseModel):
    """Report of an evaluation run."""
    evaluation_id: str
    candidate_id: Optional[str] = None
    suite: EvaluationSuite
    status: EvaluationStatus
    success_rate: float
    safety_score: float
    recovery_score: float
    regression_count: int
    suite_manifest: Optional[EvaluationSuiteManifest] = None
    bucket_results: List[BenchmarkBucketResult] = Field(default_factory=list)
    hard_failures: List[EvaluationFailure] = Field(default_factory=list)
    soft_regressions: List[EvaluationFailure] = Field(default_factory=list)
    coverage_gaps: List[str] = Field(default_factory=list)
    # Canary comparison fields
    baseline_vs_canary: Optional[Dict[str, Any]] = None  # Direct comparison data
    canary_sample_size: int = 0
    canary_success_delta: float = 0.0
    canary_safety_delta: float = 0.0
    canary_repair_delta: float = 0.0
    metrics: Dict[str, Any] = Field(default_factory=dict)
    trace_refs: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str


class RecommendationType:
    """Recommendation types for canary promotion decisions."""
    PROMOTE = "promote"
    HOLD = "hold"
    ROLLBACK = "rollback"


class RolloutRecommendation(BaseModel):
    """Recommendation for canary promotion with structured reasoning."""
    recommendation: str  # promote, hold, rollback
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)
    reason_summary: str
    blockers: List[str] = Field(default_factory=list)
    metric_deltas: Dict[str, float] = Field(default_factory=dict)
    failing_cohorts: List[str] = Field(default_factory=list)
    representative_runs: List[str] = Field(default_factory=list)  # Run IDs
    representative_clusters: List[str] = Field(default_factory=list)  # Cluster IDs
    # Which key buckets are missing or under-represented
    bucket_coverage_gaps: List[str] = Field(default_factory=list)
    # Whether this recommendation requires operator attention
    requires_operator_review: bool = False
    generated_at: str


class CohortSummary(BaseModel):
    """Summary of a rollout cohort for UI/API display."""
    cohort: str  # baseline, canary
    sample_size: int
    success_count: int
    failure_count: int
    success_rate: float
    recent_runs: List[str] = Field(default_factory=list)  # Run IDs
    top_blockers: List[str] = Field(default_factory=list)


class PublishGateStatus(BaseModel):
    """Status of publish gate for a candidate."""
    candidate_id: str
    replay_passed: bool
    benchmark_passed: bool
    approval_required: bool
    approval_satisfied: bool
    publish_ready: bool
    canary_ready: bool = False  # Ready to enter canary
    promote_ready: bool = False  # Ready to promote from canary to published
    canary_blockers: List[str] = Field(default_factory=list)
    promote_blockers: List[str] = Field(default_factory=list)
    blockers: List[str] = Field(default_factory=list)
    latest_replay_evaluation_id: Optional[str] = None
    latest_benchmark_evaluation_id: Optional[str] = None
    canary_metrics: Optional[CanaryMetrics] = None
    # Online learning additions
    current_recommendation: Optional[RolloutRecommendation] = None
    cohort_summaries: List[CohortSummary] = Field(default_factory=list)


class FailureCluster(BaseModel):
    """Cluster of similar failures."""
    cluster_id: str
    signature: str
    signature_type: "FailureClusterSignatureType"
    frequency: int
    affected_policies: List[str] = Field(default_factory=list)
    affected_workflows: List[str] = Field(default_factory=list)
    sample_run_ids: List[str] = Field(default_factory=list)
    sample_task_node_ids: List[str] = Field(default_factory=list)
    roles: List["AgentRole"] = Field(default_factory=list)
    handoff_pairs: List[str] = Field(default_factory=list)
    review_decisions: List["ReviewDecision"] = Field(default_factory=list)
    tool_names: List[str] = Field(default_factory=list)
    policy_decisions: List["VerdictDecision"] = Field(default_factory=list)
    sandbox_outcomes: List[str] = Field(default_factory=list)
    summary: str
    created_at: str
    updated_at: str


class ImprovementDiagnosisReport(BaseModel):
    """Report diagnosing failures for improvement."""
    generated_at: str
    trace_refs: List[str] = Field(default_factory=list)
    cluster_count: int = 0
    clusters: List[FailureCluster] = Field(default_factory=list)
    top_blockers: List[str] = Field(default_factory=list)
    signature_counts: Dict[str, int] = Field(default_factory=dict)


# Request types
class PolicyCandidateRequest(BaseModel):
    """Request to create a policy candidate."""
    policy_id: Optional[str] = None
    trace_refs: List[str] = Field(default_factory=list)
    rationale: Optional[str] = None


class WorkflowCandidateRequest(BaseModel):
    """Request to create a workflow candidate."""
    workflow_id: Optional[str] = None
    trace_refs: List[str] = Field(default_factory=list)
    rationale: Optional[str] = None


class ImprovementDiagnoseRequest(BaseModel):
    """Request to diagnose failures."""
    trace_refs: List[str] = Field(default_factory=list)


class EvaluationRequest(BaseModel):
    """Request to run evaluation."""
    candidate_id: Optional[str] = None
    trace_refs: List[str] = Field(default_factory=list)
    suite_config: Dict[str, Any] = Field(default_factory=dict)


class ExperimentRequest(BaseModel):
    """Request to run experiment."""
    scenario_suite: str = "golden_trace"
    harness_ids: List[str] = Field(default_factory=list)
    trace_refs: List[str] = Field(default_factory=list)


# Canary rollout request types
class CanaryStartRequest(BaseModel):
    """Request to start canary rollout."""
    scope_type: str = "percentage"  # session_tag, worker_label, goal_pattern, explicit_override, percentage
    scope_value: str = "10"  # For percentage: "10" means 10%
    description: Optional[str] = None


class CanaryPromoteRequest(BaseModel):
    """Request to promote canary to published."""
    force: bool = False  # Skip safety checks (not recommended)


class CanaryRollbackRequest(BaseModel):
    """Request to rollback canary."""
    reason: Optional[str] = None


class RolloutStatusResponse(BaseModel):
    """Response for rollout status."""
    candidate_id: str
    publish_status: str
    rollout_ring: Optional[str] = None
    rollout_scope: Optional[CanaryScope] = None
    canary_metrics: Optional[CanaryMetrics] = None
    gate_status: Optional[PublishGateStatus] = None
    baseline_version_id: Optional[str] = None
    target_version_id: str
    promote_ready: bool = False
    rollback_ready: bool = True  # Can always rollback
    blockers: List[str] = Field(default_factory=list)
    # Online learning additions
    recommendation: Optional[RolloutRecommendation] = None
    cohort_summary: List[CohortSummary] = Field(default_factory=list)
    recent_blockers: List[str] = Field(default_factory=list)
    last_analyzed_at: Optional[str] = None


class CohortFilterRequest(BaseModel):
    """Request to filter runs by cohort."""
    candidate_id: str
    cohort: Optional[str] = None  # baseline, canary, or None for all
    include_metadata: bool = True


class CohortRunsResponse(BaseModel):
    """Response with runs filtered by cohort."""
    candidate_id: str
    cohort: Optional[str]
    baseline_count: int
    canary_count: int
    runs: List[Dict[str, Any]] = Field(default_factory=list)


class AnalyzeRolloutRequest(BaseModel):
    """Request to analyze canary rollout and generate recommendation."""
    candidate_id: str
    force_recalculate: bool = False  # Ignore cached metrics
    analysis_window_hours: Optional[int] = None  # Default: all available data


class AnalyzeRolloutResponse(BaseModel):
    """Response from rollout analysis."""
    candidate_id: str
    analyzed_at: str
    canary_metrics: CanaryMetrics
    recommendation: RolloutRecommendation
    cohort_summary: List[CohortSummary]
    recent_failing_runs: List[Dict[str, Any]] = Field(default_factory=list)
