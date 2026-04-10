import type {
  CandidateCreationResponse,
  ConstraintCreateRequest,
  ConstraintDocument,
  ContextAssembly,
  EvaluationReport,
  ExperimentRun,
  FleetStatusReport,
  FailureCluster,
  HarnessPolicy,
  ImprovementCandidate,
  PublishGateStatus,
  PromptFrame,
  ReplayEnvelope,
  ResearchRun,
  ResearchSession,
  RunDetail,
  RunRequest,
  SessionDetail,
  SessionRequest,
  SettingsCatalog,
  QueueShardStatus,
  ApprovalRequest,
  WorkerLease,
  WorkerSnapshot,
  WorkflowTemplateVersion,
} from './types'

type ApiEnvelope<T> = {
  success: boolean
  data: T
}

async function request<T>(path: string, options: RequestInit = {}): Promise<ApiEnvelope<T>> {
  const response = await fetch(path, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({}))
    throw new Error(error.detail || response.statusText)
  }

  return response.json()
}

async function requestRaw<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({}))
    throw new Error(error.detail || response.statusText)
  }

  return response.json()
}

export const api = {
  listSessions: async () => request<ResearchSession[]>('/api/sessions'),
  createSession: async (payload: SessionRequest) =>
    request<ResearchSession>('/api/sessions', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  getSession: async (sessionId: string) => requestRaw<SessionDetail>(`/api/sessions/${sessionId}`),
  assembleContext: async (payload: Record<string, unknown>) =>
    request<ContextAssembly>('/api/context/assemble', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  renderPrompt: async (payload: { session_id: string }) =>
    request<PromptFrame>('/api/prompts/render', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  listConstraints: async () => request<ConstraintDocument[]>('/api/constraints'),
  createConstraint: async (payload: ConstraintCreateRequest) =>
    request<ConstraintDocument>('/api/constraints', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  publishConstraint: async (documentId: string) =>
    request<ConstraintDocument>(`/api/constraints/${documentId}/publish`, {
      method: 'POST',
    }),
  listRuns: async () => request<ResearchRun[]>('/api/runs'),
  createRun: async (payload: RunRequest) =>
    request<ResearchRun>('/api/runs', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  getRun: async (runId: string) => requestRaw<RunDetail>(`/api/runs/${runId}`),
  getReplay: async (replayId: string) => request<ReplayEnvelope>(`/api/replays/${replayId}`),
  listPolicies: async () => request<HarnessPolicy[]>('/api/policies'),
  comparePolicies: async (policyIds: string[]) =>
    request<Record<string, unknown>>('/api/policies/compare', {
      method: 'POST',
      body: JSON.stringify({ policy_ids: policyIds }),
    }),
  publishPolicy: async (policyId: string) =>
    request<HarnessPolicy>(`/api/policies/${policyId}/publish`, {
      method: 'POST',
    }),
  listExperiments: async () => request<ExperimentRun[]>('/api/experiments'),
  createExperiment: async (payload: { scenario_suite: string; harness_ids: string[]; trace_refs: string[] }) =>
    request<ExperimentRun>('/api/experiments', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  listWorkflows: async () => request<WorkflowTemplateVersion[]>('/api/workflows'),
  compareWorkflows: async (workflowIds: string[]) =>
    request<Record<string, unknown>>('/api/workflows/compare', {
      method: 'POST',
      body: JSON.stringify({ workflow_ids: workflowIds }),
    }),
  listCandidates: async () => request<ImprovementCandidate[]>('/api/candidates'),
  getCandidateGate: async (candidateId: string) => request<PublishGateStatus>(`/api/candidates/${candidateId}/gate`),
  createPolicyCandidate: async (payload: { policy_id?: string; trace_refs?: string[]; rationale?: string }) =>
    request<CandidateCreationResponse>('/api/improvement/candidates/policy', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  createWorkflowCandidate: async (payload: { workflow_id?: string; trace_refs?: string[]; rationale?: string }) =>
    request<CandidateCreationResponse>('/api/improvement/candidates/workflow', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  approveCandidate: async (candidateId: string) =>
    request<ImprovementCandidate>(`/api/candidates/${candidateId}/approve`, {
      method: 'POST',
    }),
  publishCandidate: async (candidateId: string) =>
    request<ImprovementCandidate>(`/api/candidates/${candidateId}/publish`, {
      method: 'POST',
    }),
  rollbackCandidate: async (candidateId: string) =>
    request<ImprovementCandidate>(`/api/candidates/${candidateId}/rollback`, {
      method: 'POST',
    }),
  listEvaluations: async () => request<EvaluationReport[]>('/api/evals'),
  getEvaluation: async (evaluationId: string) => request<EvaluationReport>(`/api/evals/${evaluationId}`),
  createReplayEvaluation: async (payload: { candidate_id?: string; trace_refs?: string[]; suite_config?: Record<string, unknown> }) =>
    request<EvaluationReport>('/api/evals/replay', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  createBenchmarkEvaluation: async (payload: { candidate_id?: string; trace_refs?: string[]; suite_config?: Record<string, unknown> }) =>
    request<EvaluationReport>('/api/evals/benchmark', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  listFailureClusters: async () => request<FailureCluster[]>('/api/failure-clusters'),
  diagnoseImprovement: async (payload: { trace_refs?: string[] } = {}) =>
    request<Record<string, unknown>>('/api/improvement/diagnose', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  listWorkers: async () => request<WorkerSnapshot[]>('/api/workers'),
  fleetStatus: async () => request<FleetStatusReport>('/api/fleet/status'),
  queueStatus: async () => request<QueueShardStatus[]>('/api/queues'),
  listLeases: async () => request<WorkerLease[]>('/api/leases'),
  registerWorker: async (payload: { label?: string; capabilities?: string[]; version?: string }) =>
    request<WorkerSnapshot>('/api/workers', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  drainWorker: async (workerId: string, reason?: string) =>
    request<WorkerSnapshot>(`/api/workers/${workerId}/drain`, {
      method: 'POST',
      body: JSON.stringify(reason ? { reason } : {}),
    }),
  resumeWorker: async (workerId: string) =>
    request<WorkerSnapshot>(`/api/workers/${workerId}/resume`, {
      method: 'POST',
    }),
  listApprovals: async () => request<ApprovalRequest[]>('/api/approvals'),
  resolveApproval: async (approvalId: string, decision: 'approve' | 'deny' | 'approve_once') =>
    request<ApprovalRequest>(`/api/approvals/${approvalId}/decision`, {
      method: 'POST',
      body: JSON.stringify({ decision }),
    }),
  settingsCatalog: async () => request<SettingsCatalog>('/api/settings/catalog'),
  health: async () => request<Record<string, unknown>>('/api/health'),
}
