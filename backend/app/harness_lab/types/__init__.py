"""Harness Lab type definitions.

Types are organized by domain for better maintainability.
All types are re-exported from this module for backward compatibility.
"""

# Base types and literals
from .base import (
    ActionPlan,
    ContextLayer,
    ModelProviderSettings,
    ModelCallTrace,
)
from .base import (
    SessionStatus,
    RunStatus,
    ConstraintStatus,
    ProfileStatus,
    VerdictDecision,
    ApprovalDecision,
    ApprovalStatus,
    CandidateKind,
    CandidatePublishStatus,
    EvaluationStatus,
    EvaluationSuite,
    WorkerState,
    MissionStatus,
    AttemptStatus,
    LeaseStatus,
    KnowledgeSourceType,
    KnowledgeReindexScope,
    SandboxMode,
    SandboxNetworkPolicy,
    AgentRole,
    ReviewDecision,
    FailureClusterSignatureType,
)

# Session and Run types
from .session_run import (
    ContextBlock,
    PromptSection,
    PromptFrame,
    TaskNode,
    TaskEdge,
    TaskGraph,
    ExecutionTrace,
    RolloutInfo,
    ResearchSession,
    ResearchRun,
    ExperimentRun,
    IntentDeclaration,
    ContextAssembleRequest,
    PromptRenderRequest,
    SessionRequest,
    RunRequest,
    IntentRequest,
)

# Worker and Lease types
from .worker_lease import (
    WorkerSnapshot,
    Mission,
    TaskAttempt,
    WorkerLease,
    DispatchEnvelope,
    WorkerEventRecord,
    WorkerEventBatch,
    LeaseTransitionContext,
    RunCoordinationSnapshot,
    WorkerHealthSummary,
    StuckRunSummary,
    DispatchConstraint,
    WorkerDrainRequest,
    QueueShardStatus,
    FleetStatusReport,
    WorkerRegisterRequest,
    WorkerHeartbeatRequest,
    WorkerPollRequest,
    WorkerPollResponse,
    LeaseSweepReport,
    LeaseCompletionRequest,
    LeaseFailureRequest,
    LeaseReleaseRequest,
)

# Policy and Constraint types
from .policy_constraint import (
    PolicyVerdict,
    ConstraintDocument,
    ConstraintCompileSummary,
    ConstraintCompileResult,
    ConstraintExplanation,
    ConstraintMatch,
    MatchedRuleInfo,
    RuleCondition,
    ConstraintRule,
    CompiledConstraintSet,
    ContextProfile,
    PromptTemplate,
    ModelProfile,
    HarnessPolicy,
    WorkflowTemplateVersion,
    ConstraintCreateRequest,
    ConstraintVerifyRequest,
    ConstraintVerifyResponse,
    PolicyCompareRequest,
    WorkflowCompareRequest,
    ApprovalRequestModel,
    ApprovalDecisionRequest,
    ConstraintEngineStatus,
)

# Sandbox types
from .sandbox import (
    SandboxSpec,
    SandboxTrace,
    SandboxResult,
    SandboxStatus,
    ToolCallRecord,
)

# Improvement types
from .improvement import (
    ImprovementCandidate,
    CanaryScope,
    BucketMetrics,
    CanaryMetrics,
    RolloutSnapshot,
    RecommendationType,
    RolloutRecommendation,
    CohortSummary,
    EvaluationFailure,
    BenchmarkBucketResult,
    EvaluationSuiteManifest,
    EvaluationReport,
    PublishGateStatus,
    FailureCluster,
    ImprovementDiagnosisReport,
    PolicyCandidateRequest,
    WorkflowCandidateRequest,
    ImprovementDiagnoseRequest,
    EvaluationRequest,
    ExperimentRequest,
    CanaryStartRequest,
    CanaryPromoteRequest,
    CanaryRollbackRequest,
    RolloutStatusResponse,
    CohortFilterRequest,
    CohortRunsResponse,
    AnalyzeRolloutRequest,
    AnalyzeRolloutResponse,
)

# Knowledge types
from .knowledge import (
    KnowledgeSearchHit,
    KnowledgeIndexStatus,
    KnowledgeSearchResult,
    KnowledgeSearchRequest,
    KnowledgeReindexRequest,
)

# Recovery and Handoff types
from .recovery import (
    RecoveryEvent,
    HandoffPacket,
    ReviewVerdict,
    MissionPhaseSnapshot,
)

# Tool types
from .tool import (
    ToolDescriptor,
    ToolExecutionResult,
)

# System types
from .system import (
    EventEnvelope,
    ArtifactRef,
    ArtifactStoreStatus,
    DoctorReport,
)

__all__ = [
    # Base
    "ActionPlan",
    "ContextLayer",
    "ModelProviderSettings",
    "ModelCallTrace",
    # Literals
    "SessionStatus",
    "RunStatus",
    "ConstraintStatus",
    "ProfileStatus",
    "VerdictDecision",
    "ApprovalDecision",
    "ApprovalStatus",
    "CandidateKind",
    "CandidatePublishStatus",
    "EvaluationStatus",
    "EvaluationSuite",
    "WorkerState",
    "MissionStatus",
    "AttemptStatus",
    "LeaseStatus",
    "KnowledgeSourceType",
    "KnowledgeReindexScope",
    "SandboxMode",
    "SandboxNetworkPolicy",
    "AgentRole",
    "ReviewDecision",
    "FailureClusterSignatureType",
    # Session/Run
    "ContextBlock",
    "PromptSection",
    "PromptFrame",
    "TaskNode",
    "TaskEdge",
    "TaskGraph",
    "ExecutionTrace",
    "RolloutInfo",
    "ResearchSession",
    "ResearchRun",
    "ExperimentRun",
    "IntentDeclaration",
    "ContextAssembleRequest",
    "PromptRenderRequest",
    "SessionRequest",
    "RunRequest",
    "IntentRequest",
    # Worker/Lease
    "WorkerSnapshot",
    "Mission",
    "TaskAttempt",
    "WorkerLease",
    "DispatchEnvelope",
    "WorkerEventRecord",
    "WorkerEventBatch",
    "LeaseTransitionContext",
    "RunCoordinationSnapshot",
    "WorkerHealthSummary",
    "StuckRunSummary",
    "DispatchConstraint",
    "WorkerDrainRequest",
    "QueueShardStatus",
    "FleetStatusReport",
    "WorkerRegisterRequest",
    "WorkerHeartbeatRequest",
    "WorkerPollRequest",
    "WorkerPollResponse",
    "LeaseSweepReport",
    "LeaseCompletionRequest",
    "LeaseFailureRequest",
    "LeaseReleaseRequest",
    # Policy/Constraint
    "PolicyVerdict",
    "ConstraintDocument",
    "ConstraintCompileSummary",
    "ConstraintCompileResult",
    "ConstraintExplanation",
    "ConstraintMatch",
    "MatchedRuleInfo",
    "RuleCondition",
    "ConstraintRule",
    "CompiledConstraintSet",
    "ContextProfile",
    "PromptTemplate",
    "ModelProfile",
    "HarnessPolicy",
    "WorkflowTemplateVersion",
    "ConstraintCreateRequest",
    "ConstraintVerifyRequest",
    "ConstraintVerifyResponse",
    "PolicyCompareRequest",
    "WorkflowCompareRequest",
    "ApprovalRequestModel",
    "ApprovalDecisionRequest",
    "ConstraintEngineStatus",
    # Sandbox
    "SandboxSpec",
    "SandboxTrace",
    "SandboxResult",
    "SandboxStatus",
    "ToolCallRecord",
    # Improvement
    "ImprovementCandidate",
    "CanaryScope",
    "BucketMetrics",
    "CanaryMetrics",
    "RolloutSnapshot",
    "RecommendationType",
    "RolloutRecommendation",
    "CohortSummary",
    "EvaluationFailure",
    "BenchmarkBucketResult",
    "EvaluationSuiteManifest",
    "EvaluationReport",
    "PublishGateStatus",
    "FailureCluster",
    "ImprovementDiagnosisReport",
    "PolicyCandidateRequest",
    "WorkflowCandidateRequest",
    "ImprovementDiagnoseRequest",
    "EvaluationRequest",
    "ExperimentRequest",
    "CanaryStartRequest",
    "CanaryPromoteRequest",
    "CanaryRollbackRequest",
    "RolloutStatusResponse",
    "CohortFilterRequest",
    "CohortRunsResponse",
    "AnalyzeRolloutRequest",
    "AnalyzeRolloutResponse",
    # Knowledge
    "KnowledgeSearchHit",
    "KnowledgeIndexStatus",
    "KnowledgeSearchResult",
    "KnowledgeSearchRequest",
    "KnowledgeReindexRequest",
    # Recovery
    "RecoveryEvent",
    "HandoffPacket",
    "ReviewVerdict",
    "MissionPhaseSnapshot",
    # Tool
    "ToolDescriptor",
    "ToolExecutionResult",
    # System
    "EventEnvelope",
    "ArtifactRef",
    "ArtifactStoreStatus",
    "DoctorReport",
]

# Rebuild Pydantic models after all cross-module forward references are loaded.
for _name in __all__:
    _obj = globals().get(_name)
    if hasattr(_obj, "model_rebuild"):
        try:
            _obj.model_rebuild(_types_namespace=globals())
        except Exception:  # noqa: BLE001
            # Some exported symbols are not Pydantic models or do not need rebuild.
            pass

del _name, _obj
