export interface ActionPlan {
  tool_name: string
  subject: string
  payload: Record<string, unknown>
  summary: string
}

export interface IntentDeclaration {
  intent_id: string
  task_type: string
  intent: string
  confidence: number
  risk_mode: string
  suggested_action: ActionPlan
  model_profile_id: string
  created_at: string
}

export interface ModelCallTrace {
  provider: string
  model_name: string
  latency_ms: number
  used_fallback: boolean
  failure_reason?: string | null
}

export interface TaskGraph {
  task_graph_id: string
  nodes: Array<Record<string, unknown>>
  edges: Array<Record<string, unknown>>
  execution_strategy: string
}

export interface ResearchSession {
  session_id: string
  goal: string
  status: string
  active_policy_id: string
  workflow_template_id?: string | null
  constraint_set_id: string
  context_profile_id: string
  prompt_template_id: string
  model_profile_id: string
  execution_mode: string
  context: Record<string, unknown>
  intent_declaration?: IntentDeclaration
  intent_model_call?: ModelCallTrace | null
  task_graph?: TaskGraph
  created_at: string
  updated_at: string
}

export interface EventEnvelope {
  seq: number
  event_id: string
  session_id?: string | null
  run_id?: string | null
  event_type: string
  payload: Record<string, unknown>
  created_at: string
}

export interface SessionDetail {
  success: boolean
  data: ResearchSession
  workflow_template?: WorkflowTemplateVersion | null
  active_policy?: HarnessPolicy | null
  runs: ResearchRun[]
  events: EventEnvelope[]
}

export interface ContextBlock {
  context_block_id: string
  layer: string
  type: string
  title: string
  source_ref: string
  content: string
  score: number
  token_estimate: number
  selected: boolean
  dependencies: string[]
  metadata: Record<string, unknown>
}

export interface ContextAssembly {
  blocks: ContextBlock[]
  selection_summary: Record<string, unknown>
}

export interface PromptSection {
  section_key: string
  title: string
  content: string
  token_estimate: number
  source_refs: string[]
}

export interface PromptFrame {
  prompt_frame_id: string
  template_id: string
  sections: PromptSection[]
  total_token_estimate: number
  truncated_blocks: string[]
  created_at: string
}

export interface PolicyVerdict {
  verdict_id: string
  subject: string
  decision: string
  reason: string
  matched_rule: string
  created_at: string
}

export interface ToolCallRecord {
  tool_name: string
  payload: Record<string, unknown>
  ok: boolean
  output: Record<string, unknown>
  error?: string | null
  created_at: string
}

export interface RecoveryEvent {
  recovery_id: string
  kind: string
  summary: string
  created_at: string
}

export interface ArtifactRef {
  artifact_id: string
  run_id?: string | null
  artifact_type: string
  relative_path: string
  metadata: Record<string, unknown>
  created_at: string
}

export interface ExecutionTrace {
  trace_id: string
  session_id: string
  prompt_frame_id: string
  intent_declaration: IntentDeclaration
  model_calls: ModelCallTrace[]
  context_blocks: ContextBlock[]
  policy_verdicts: PolicyVerdict[]
  tool_calls: ToolCallRecord[]
  recovery_events: RecoveryEvent[]
  artifacts: ArtifactRef[]
  status: string
  created_at: string
  updated_at: string
}

export interface ResearchRun {
  run_id: string
  session_id: string
  status: string
  mission_id?: string | null
  policy_id?: string | null
  workflow_template_id?: string | null
  assigned_worker_id?: string | null
  current_attempt_id?: string | null
  active_lease_id?: string | null
  prompt_frame?: PromptFrame
  execution_trace?: ExecutionTrace
  result: Record<string, unknown>
  created_at: string
  updated_at: string
}

export interface Mission {
  mission_id: string
  session_id: string
  run_id: string
  status: string
  created_at: string
  updated_at: string
}

export interface TaskAttempt {
  attempt_id: string
  run_id: string
  task_node_id: string
  worker_id?: string | null
  lease_id?: string | null
  status: string
  retry_index: number
  summary?: string | null
  error?: string | null
  started_at?: string | null
  finished_at?: string | null
  created_at: string
  updated_at: string
}

export interface WorkerLease {
  lease_id: string
  worker_id: string
  run_id: string
  task_node_id: string
  attempt_id: string
  status: string
  approval_token?: string | null
  expires_at: string
  heartbeat_at: string
  created_at: string
  updated_at: string
}

export interface RunDetail {
  success: boolean
  data: ResearchRun
  session?: ResearchSession
  mission?: Mission | null
  mission_phase?: Record<string, unknown>
  handoffs?: Array<Record<string, unknown>>
  review_verdicts?: Array<Record<string, unknown>>
  role_timeline?: Array<Record<string, unknown>>
  coordination_snapshot?: Record<string, unknown>
  timeline_summary?: Record<string, unknown>
  status_summary?: Record<string, unknown>
  sandbox_summary?: Record<string, unknown>
  active_policy?: HarnessPolicy | null
  workflow_template?: WorkflowTemplateVersion | null
  worker?: WorkerSnapshot | null
  attempts: TaskAttempt[]
  leases: WorkerLease[]
  events: EventEnvelope[]
  approvals: ApprovalRequest[]
  artifacts: ArtifactRef[]
}

export interface ReplayEnvelope {
  replay_id?: string
  run: ResearchRun
  session: ResearchSession
  mission?: Mission | null
  events: EventEnvelope[]
  approvals: ApprovalRequest[]
  artifacts: ArtifactRef[]
  attempts?: TaskAttempt[]
  leases?: WorkerLease[]
}

export interface ConstraintDocument {
  document_id: string
  title: string
  body: string
  scope: string
  status: string
  tags: string[]
  priority: number
  source: string
  version: string
  created_at: string
  updated_at: string
}

export interface HarnessPolicy {
  policy_id: string
  name: string
  status: string
  constraint_set_id: string
  context_profile_id: string
  prompt_template_id: string
  model_profile_id: string
  repair_policy: Record<string, unknown>
  budget_policy: Record<string, unknown>
  metrics: Record<string, unknown>
  created_at: string
  updated_at: string
}

export interface WorkflowTemplateVersion {
  workflow_id: string
  parent_id?: string | null
  name: string
  description: string
  scope: string
  status: string
  dag: Record<string, unknown>
  role_map: Record<string, unknown>
  gates: Array<Record<string, unknown>>
  metrics: Record<string, unknown>
  created_at: string
  updated_at: string
}

export interface ExperimentRun {
  experiment_id: string
  scenario_suite: string
  harness_ids: string[]
  status: string
  metrics: Record<string, unknown>
  trace_refs: string[]
  winner?: string | null
  created_at: string
  updated_at: string
}

export interface ImprovementCandidate {
  candidate_id: string
  kind: 'policy' | 'workflow'
  target_id: string
  target_version_id: string
  baseline_version_id?: string | null
  change_set: Record<string, unknown>
  rationale: string
  eval_status: string
  publish_status: string
  approved: boolean
  requires_human_approval: boolean
  metrics: Record<string, unknown>
  evaluation_ids: string[]
  created_at: string
  updated_at: string
}

export interface EvaluationFailure {
  kind: string
  severity: 'hard' | 'soft'
  bucket?: string | null
  trace_ref?: string | null
  summary: string
}

export interface BenchmarkBucketResult {
  bucket: string
  total: number
  passed: number
  failed: number
  coverage: number
  regressions: string[]
}

export interface EvaluationSuiteManifest {
  suite_id: string
  source: string
  trace_refs: string[]
  bucket_map: Record<string, string[]>
  eligibility: Record<string, unknown>
  generated_at: string
}

export interface EvaluationReport {
  evaluation_id: string
  candidate_id?: string | null
  suite: 'replay' | 'benchmark'
  status: string
  success_rate: number
  safety_score: number
  recovery_score: number
  regression_count: number
  suite_manifest?: EvaluationSuiteManifest | null
  bucket_results: BenchmarkBucketResult[]
  hard_failures: EvaluationFailure[]
  soft_regressions: EvaluationFailure[]
  coverage_gaps: string[]
  metrics: Record<string, unknown>
  trace_refs: string[]
  created_at: string
  updated_at: string
}

export interface PublishGateStatus {
  candidate_id: string
  replay_passed: boolean
  benchmark_passed: boolean
  approval_required: boolean
  approval_satisfied: boolean
  publish_ready: boolean
  blockers: string[]
  latest_replay_evaluation_id?: string | null
  latest_benchmark_evaluation_id?: string | null
}

export interface FailureCluster {
  cluster_id: string
  signature: string
  signature_type: string
  frequency: number
  affected_policies: string[]
  affected_workflows: string[]
  sample_run_ids: string[]
  sample_task_node_ids: string[]
  roles: string[]
  handoff_pairs: string[]
  review_decisions: string[]
  tool_names: string[]
  policy_decisions: string[]
  sandbox_outcomes: string[]
  summary: string
  created_at: string
  updated_at: string
}

export interface WorkerSnapshot {
  worker_id: string
  label: string
  state: string
  drain_state?: string
  capabilities: string[]
  role_profile?: string | null
  hostname?: string | null
  pid?: number | null
  labels?: string[]
  eligible_labels?: string[]
  worker_class?: string
  execution_mode?: string
  heartbeat_at: string
  lease_count: number
  version: string
  current_run_id?: string | null
  current_task_node_id?: string | null
  current_lease_id?: string | null
  last_error?: string | null
  created_at: string
  updated_at: string
}

export interface ApprovalRequest {
  approval_id: string
  run_id: string
  verdict_id: string
  subject: string
  summary: string
  payload: Record<string, unknown>
  status: string
  decision?: string | null
  created_at: string
  updated_at: string
}

export interface SettingsCatalog {
  constraints: ConstraintDocument[]
  context_profiles: Array<Record<string, unknown>>
  prompt_templates: Array<Record<string, unknown>>
  model_profiles: Array<Record<string, unknown>>
  workflow_templates?: WorkflowTemplateVersion[]
  workers?: WorkerSnapshot[]
  tools: Array<Record<string, unknown>>
  model_provider?: Record<string, unknown>
  execution_plane?: Record<string, unknown>
  sandbox?: Record<string, unknown>
}

export interface QueueShardStatus {
  shard: string
  depth: number
  sample_tasks: Array<Record<string, string>>
}

export interface FleetStatusReport {
  worker_count: number
  active_workers: string[]
  draining_workers: string[]
  offline_workers: string[]
  unhealthy_workers: string[]
  workers_by_role: Record<string, number>
  queue_depth_by_shard: Record<string, number>
  lease_reclaim_rate: number
  stuck_run_count: number
  late_callback_count: number
}

export interface SessionRequest {
  goal: string
  context: Record<string, unknown>
  execution_mode: string
  constraint_set_id?: string
  context_profile_id?: string
  prompt_template_id?: string
  model_profile_id?: string
  workflow_template_id?: string
}

export interface RunRequest {
  session_id?: string
  goal?: string
  context?: Record<string, unknown>
  workflow_template_id?: string
  execution_mode?: string
}

export interface ConstraintCreateRequest {
  title: string
  body: string
  tags?: string[]
  priority?: number
}

export interface CandidateCreationResponse {
  candidate: ImprovementCandidate
  version: HarnessPolicy | WorkflowTemplateVersion
  observations: Record<string, unknown>
  diagnosis?: Record<string, unknown>
  evaluations?: EvaluationReport[]
  gate?: PublishGateStatus
}
