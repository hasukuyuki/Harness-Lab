"""Improvement, evaluation, and failure cluster types."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from .base import CandidateKind, CandidatePublishStatus, EvaluationStatus, EvaluationSuite


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
    metrics: Dict[str, Any] = Field(default_factory=dict)
    trace_refs: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str


class PublishGateStatus(BaseModel):
    """Status of publish gate for a candidate."""
    candidate_id: str
    replay_passed: bool
    benchmark_passed: bool
    approval_required: bool
    approval_satisfied: bool
    publish_ready: bool
    blockers: List[str] = Field(default_factory=list)
    latest_replay_evaluation_id: Optional[str] = None
    latest_benchmark_evaluation_id: Optional[str] = None


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
