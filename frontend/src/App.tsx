import React, { useEffect, useMemo, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Divider,
  Input,
  Layout,
  List,
  Row,
  Select,
  Space,
  Statistic,
  Tag,
  Typography,
  message,
} from 'antd'
import {
  ApartmentOutlined,
  ExperimentOutlined,
  FileSearchOutlined,
  FileTextOutlined,
  FolderOpenOutlined,
  PlayCircleOutlined,
  RadarChartOutlined,
  SafetyCertificateOutlined,
  SettingOutlined,
} from '@ant-design/icons'

import { api } from './lab/api'
import { JsonCard } from './lab/components/JsonCard'
import { LabSidebar, type NavItem } from './lab/components/LabSidebar'
import { StatusPill } from './lab/components/StatusPill'
import type {
  ApprovalRequest,
  ConstraintDocument,
  ContextAssembly,
  EvaluationReport,
  EventEnvelope,
  ExperimentRun,
  FleetStatusReport,
  FailureCluster,
  HarnessPolicy,
  ImprovementCandidate,
  PromptFrame,
  PublishGateStatus,
  QueueShardStatus,
  ReplayEnvelope,
  ResearchRun,
  ResearchSession,
  SettingsCatalog,
  SessionDetail,
  WorkerLease,
  RunDetail,
  WorkerSnapshot,
  WorkflowTemplateVersion,
} from './lab/types'

const { Header, Content } = Layout
const { Title, Paragraph, Text } = Typography
const { TextArea } = Input

type SectionKey =
  | 'sessions'
  | 'constraints'
  | 'context'
  | 'prompts'
  | 'runs'
  | 'replays'
  | 'policies'
  | 'experiments'
  | 'settings'

const NAV_ITEMS: NavItem[] = [
  { key: 'sessions', label: 'Sessions', icon: <RadarChartOutlined /> },
  { key: 'constraints', label: 'Constraints', icon: <SafetyCertificateOutlined /> },
  { key: 'context', label: 'Context', icon: <ApartmentOutlined /> },
  { key: 'prompts', label: 'Prompts', icon: <FileTextOutlined /> },
  { key: 'runs', label: 'Runs', icon: <PlayCircleOutlined /> },
  { key: 'replays', label: 'Replays', icon: <FolderOpenOutlined /> },
  { key: 'policies', label: 'Policies', icon: <ExperimentOutlined /> },
  { key: 'experiments', label: 'Experiments', icon: <FileSearchOutlined /> },
  { key: 'settings', label: 'Settings', icon: <SettingOutlined /> },
]

function App() {
  const [section, setSection] = useState<SectionKey>('sessions')
  const [goal, setGoal] = useState('Inspect the current workspace safely and render a replayable Harness Lab trace.')
  const [pathHint, setPathHint] = useState('README.md')
  const [shellCommand, setShellCommand] = useState('')
  const [sessions, setSessions] = useState<ResearchSession[]>([])
  const [sessionDetail, setSessionDetail] = useState<SessionDetail | null>(null)
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null)
  const [constraints, setConstraints] = useState<ConstraintDocument[]>([])
  const [newConstraintTitle, setNewConstraintTitle] = useState('Exploratory network policy')
  const [newConstraintBody, setNewConstraintBody] = useState(
    'Allow HTTP GET for research, require review for mutable shell commands, and deny destructive shell patterns.'
  )
  const [contextPreview, setContextPreview] = useState<ContextAssembly | null>(null)
  const [promptFrame, setPromptFrame] = useState<PromptFrame | null>(null)
  const [runs, setRuns] = useState<ResearchRun[]>([])
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [runDetail, setRunDetail] = useState<RunDetail | null>(null)
  const [replay, setReplay] = useState<ReplayEnvelope | null>(null)
  const [policies, setPolicies] = useState<HarnessPolicy[]>([])
  const [policyCompareIds, setPolicyCompareIds] = useState<string[]>([])
  const [policyDiff, setPolicyDiff] = useState<Record<string, unknown> | null>(null)
  const [workflows, setWorkflows] = useState<WorkflowTemplateVersion[]>([])
  const [workflowCompareIds, setWorkflowCompareIds] = useState<string[]>([])
  const [workflowDiff, setWorkflowDiff] = useState<Record<string, unknown> | null>(null)
  const [candidates, setCandidates] = useState<ImprovementCandidate[]>([])
  const [selectedCandidateId, setSelectedCandidateId] = useState<string | null>(null)
  const [candidateGate, setCandidateGate] = useState<PublishGateStatus | null>(null)
  const [evaluations, setEvaluations] = useState<EvaluationReport[]>([])
  const [selectedEvaluationId, setSelectedEvaluationId] = useState<string | null>(null)
  const [failureClusters, setFailureClusters] = useState<FailureCluster[]>([])
  const [workers, setWorkers] = useState<WorkerSnapshot[]>([])
  const [leases, setLeases] = useState<WorkerLease[]>([])
  const [experiments, setExperiments] = useState<ExperimentRun[]>([])
  const [catalog, setCatalog] = useState<SettingsCatalog | null>(null)
  const [health, setHealth] = useState<Record<string, unknown> | null>(null)
  const [approvals, setApprovals] = useState<ApprovalRequest[]>([])
  const [fleetStatus, setFleetStatus] = useState<FleetStatusReport | null>(null)
  const [queueStatus, setQueueStatus] = useState<QueueShardStatus[]>([])

  const selectedSession = useMemo(
    () => sessions.find((sessionItem) => sessionItem.session_id === selectedSessionId) || null,
    [sessions, selectedSessionId]
  )

  const selectedRun = useMemo(
    () => runs.find((runItem) => runItem.run_id === selectedRunId) || null,
    [runs, selectedRunId]
  )

  const selectedCandidate = useMemo(
    () => candidates.find((candidateItem) => candidateItem.candidate_id === selectedCandidateId) || null,
    [candidates, selectedCandidateId]
  )

  const selectedEvaluation = useMemo(
    () => evaluations.find((evaluationItem) => evaluationItem.evaluation_id === selectedEvaluationId) || null,
    [evaluations, selectedEvaluationId]
  )

  const selectedCandidateEvaluations = useMemo(
    () => evaluations.filter((evaluationItem) => evaluationItem.candidate_id === selectedCandidateId),
    [evaluations, selectedCandidateId]
  )

  const refreshSessions = async (preferId?: string) => {
    const response = await api.listSessions()
    setSessions(response.data)
    const nextId = preferId || selectedSessionId || response.data[0]?.session_id || null
    setSelectedSessionId(nextId)
  }

  const refreshRuns = async (preferId?: string) => {
    const response = await api.listRuns()
    setRuns(response.data)
    const nextId = preferId || selectedRunId || response.data[0]?.run_id || null
    setSelectedRunId(nextId)
  }

  const refreshSharedData = async () => {
    const [
      constraintEnvelope,
      policyEnvelope,
      workflowEnvelope,
      candidateEnvelope,
      evaluationEnvelope,
      failureClusterEnvelope,
      workerEnvelope,
      leaseEnvelope,
      experimentEnvelope,
      approvalEnvelope,
      catalogEnvelope,
      healthEnvelope,
      fleetEnvelope,
      queueEnvelope,
    ] =
      await Promise.all([
        api.listConstraints(),
        api.listPolicies(),
        api.listWorkflows(),
        api.listCandidates(),
        api.listEvaluations(),
        api.listFailureClusters(),
        api.listWorkers(),
        api.listLeases(),
        api.listExperiments(),
        api.listApprovals(),
        api.settingsCatalog(),
        api.health(),
        api.fleetStatus(),
        api.queueStatus(),
      ])
    setConstraints(constraintEnvelope.data)
    setPolicies(policyEnvelope.data)
    setWorkflows(workflowEnvelope.data)
    setCandidates(candidateEnvelope.data)
    setSelectedCandidateId((current) => current || candidateEnvelope.data[0]?.candidate_id || null)
    setEvaluations(evaluationEnvelope.data)
    setSelectedEvaluationId((current) => current || evaluationEnvelope.data[0]?.evaluation_id || null)
    setFailureClusters(failureClusterEnvelope.data)
    setWorkers(workerEnvelope.data)
    setLeases(leaseEnvelope.data)
    setExperiments(experimentEnvelope.data)
    setApprovals(approvalEnvelope.data)
    setCatalog(catalogEnvelope.data)
    setHealth(healthEnvelope.data)
    setFleetStatus(fleetEnvelope.data)
    setQueueStatus(queueEnvelope.data)
  }

  const drainWorker = async (workerId: string) => {
    await api.drainWorker(workerId, 'drained from web mission control')
    message.success('Worker draining')
    await refreshSharedData()
  }

  const resumeWorker = async (workerId: string) => {
    await api.resumeWorker(workerId)
    message.success('Worker resumed')
    await refreshSharedData()
  }

  const refreshSessionResearch = async (sessionId: string) => {
    const [detail, contextEnvelope, promptEnvelope] = await Promise.all([
      api.getSession(sessionId),
      api.assembleContext({ session_id: sessionId }),
      api.renderPrompt({ session_id: sessionId }),
    ])
    setSessionDetail(detail)
    setContextPreview(contextEnvelope.data)
    setPromptFrame(promptEnvelope.data)
  }

  const refreshRunResearch = async (runId: string) => {
    const [detail, replayEnvelope] = await Promise.all([api.getRun(runId), api.getReplay(runId)])
    setRunDetail(detail)
    setReplay(replayEnvelope.data)
  }

  useEffect(() => {
    void Promise.all([refreshSessions(), refreshRuns(), refreshSharedData()])
  }, [])

  useEffect(() => {
    if (!selectedSessionId) return
    void refreshSessionResearch(selectedSessionId)
  }, [selectedSessionId])

  useEffect(() => {
    if (!selectedRunId) return
    void refreshRunResearch(selectedRunId)
  }, [selectedRunId])

  useEffect(() => {
    if (!selectedCandidateId) {
      setCandidateGate(null)
      return
    }
    void api
      .getCandidateGate(selectedCandidateId)
      .then((response) => setCandidateGate(response.data))
      .catch(() => setCandidateGate(null))
  }, [selectedCandidateId, selectedCandidate?.updated_at, evaluations.length])

  const createSession = async () => {
    try {
      const context: Record<string, unknown> = {}
      if (pathHint.trim()) context.path = pathHint.trim()
      if (shellCommand.trim()) context.shell_command = shellCommand.trim()
      const response = await api.createSession({
        goal,
        context,
        execution_mode: 'single_worker',
      })
      message.success('Session created')
      await refreshSessions(response.data.session_id)
      await refreshSharedData()
      setSection('sessions')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to create session')
    }
  }

  const createRun = async () => {
    try {
      const response = await api.createRun(
        selectedSessionId
          ? { session_id: selectedSessionId }
          : {
              goal,
              context: {
                ...(pathHint.trim() ? { path: pathHint.trim() } : {}),
                ...(shellCommand.trim() ? { shell_command: shellCommand.trim() } : {}),
              },
              execution_mode: 'single_worker',
            }
      )
      message.success('Run executed')
      await refreshRuns(response.data.run_id)
      await refreshSessions(selectedSessionId || response.data.session_id)
      await refreshSharedData()
      setSection('runs')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to run session')
    }
  }

  const createConstraint = async () => {
    try {
      await api.createConstraint({
        title: newConstraintTitle,
        body: newConstraintBody,
        tags: ['research', 'draft'],
        priority: 60,
      })
      message.success('Constraint document created')
      await refreshSharedData()
      setSection('constraints')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to create constraint')
    }
  }

  const resolveApproval = async (approvalId: string, decision: 'approve' | 'deny' | 'approve_once') => {
    try {
      await api.resolveApproval(approvalId, decision)
      message.success(`Approval ${decision}`)
      await refreshRuns(selectedRunId || undefined)
      await refreshSharedData()
      if (selectedRunId) {
        await refreshRunResearch(selectedRunId)
      }
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to resolve approval')
    }
  }

  const comparePolicies = async () => {
    try {
      const response = await api.comparePolicies(policyCompareIds)
      setPolicyDiff(response.data)
      setSection('policies')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to compare policies')
    }
  }

  const compareWorkflows = async () => {
    try {
      const response = await api.compareWorkflows(workflowCompareIds)
      setWorkflowDiff(response.data)
      setSection('policies')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to compare workflows')
    }
  }

  const publishPolicy = async (policyId: string) => {
    try {
      await api.publishPolicy(policyId)
      message.success('Policy published')
      await refreshSharedData()
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to publish policy')
    }
  }

  const createPolicyCandidate = async () => {
    try {
      await api.createPolicyCandidate({
        policy_id: policyCompareIds[0],
        trace_refs: selectedRunId ? [selectedRunId] : runs.slice(0, 3).map((run) => run.run_id),
      })
      message.success('Policy candidate generated')
      await refreshSharedData()
      setSection('policies')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to create policy candidate')
    }
  }

  const createWorkflowCandidate = async () => {
    try {
      await api.createWorkflowCandidate({
        workflow_id: workflowCompareIds[0],
        trace_refs: selectedRunId ? [selectedRunId] : runs.slice(0, 3).map((run) => run.run_id),
      })
      message.success('Workflow candidate generated')
      await refreshSharedData()
      setSection('policies')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to create workflow candidate')
    }
  }

  const approveCandidate = async (candidateId: string) => {
    try {
      await api.approveCandidate(candidateId)
      message.success('Candidate approved')
      await refreshSharedData()
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to approve candidate')
    }
  }

  const publishCandidate = async (candidateId: string) => {
    try {
      await api.publishCandidate(candidateId)
      message.success('Candidate published')
      await refreshSharedData()
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to publish candidate')
    }
  }

  const rollbackCandidate = async (candidateId: string) => {
    try {
      await api.rollbackCandidate(candidateId)
      message.success('Candidate rolled back')
      await refreshSharedData()
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to roll back candidate')
    }
  }

  const createEvaluation = async (suite: 'replay' | 'benchmark') => {
    try {
      const payload = {
        candidate_id: selectedCandidateId || undefined,
        trace_refs: selectedRunId ? [selectedRunId] : runs.slice(0, 3).map((run) => run.run_id),
        suite_config: { source: 'historical_traces' },
      }
      if (suite === 'replay') {
        await api.createReplayEvaluation(payload)
      } else {
        await api.createBenchmarkEvaluation(payload)
      }
      message.success(`${suite} evaluation recorded`)
      await refreshSharedData()
      setSection('experiments')
    } catch (error) {
      message.error(error instanceof Error ? error.message : `Failed to create ${suite} evaluation`)
    }
  }

  const registerWorker = async () => {
    try {
      await api.registerWorker({
        label: `operator-worker-${workers.length + 1}`,
        capabilities: ['filesystem', 'git', 'http_fetch', 'knowledge_search', 'model_reflection'],
        version: 'v1',
      })
      message.success('Worker registered')
      await refreshSharedData()
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to register worker')
    }
  }

  const createExperiment = async () => {
    try {
      const harnessIds = policyCompareIds.length > 0 ? policyCompareIds : policies.slice(0, 2).map((policy) => policy.policy_id)
      const traceRefs = selectedRunId ? [selectedRunId] : runs.slice(0, 2).map((run) => run.run_id)
      await api.createExperiment({
        scenario_suite: 'golden_trace',
        harness_ids: harnessIds,
        trace_refs: traceRefs,
      })
      message.success('Experiment recorded')
      await refreshSharedData()
      setSection('experiments')
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to create experiment')
    }
  }

  const renderSessions = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={10}>
        <Card className="lab-panel sticky-panel" title="New Research Session">
          <Space direction="vertical" size={16} className="w-full">
            <div>
              <Text className="lab-label">Goal</Text>
              <TextArea rows={6} value={goal} onChange={(event) => setGoal(event.target.value)} />
            </div>
            <div>
              <Text className="lab-label">Path Hint</Text>
              <Input value={pathHint} onChange={(event) => setPathHint(event.target.value)} placeholder="design/harness-architecture-design.md" />
            </div>
            <div>
              <Text className="lab-label">Optional Shell Command</Text>
              <Input
                value={shellCommand}
                onChange={(event) => setShellCommand(event.target.value)}
                placeholder={'rg -n "ConstraintEngine" backend/app'}
              />
            </div>
            <Alert
              type="info"
              showIcon
              message="Sessions snapshot intent, policy references, and task graph before a run is executed."
            />
            <Space>
              <Button type="primary" icon={<RadarChartOutlined />} onClick={createSession}>
                Create Session
              </Button>
              <Button icon={<PlayCircleOutlined />} onClick={createRun}>
                Run Harness Trace
              </Button>
            </Space>
          </Space>
        </Card>
      </Col>
      <Col xs={24} xl={14}>
        <Card className="lab-panel" title="Session Registry">
          <List
            dataSource={sessions}
            renderItem={(item) => (
              <List.Item
                className={`lab-list-item ${item.session_id === selectedSessionId ? 'lab-list-item-active' : ''}`}
                onClick={() => setSelectedSessionId(item.session_id)}
              >
                <div className="lab-list-body">
                  <div>
                    <Text strong>{item.goal}</Text>
                    <Paragraph className="lab-meta" ellipsis={{ rows: 2 }}>
                      {item.intent_declaration?.intent || 'Intent declaration pending'}
                    </Paragraph>
                  </div>
                  <div className="lab-tag-stack">
                    <StatusPill value={item.status} />
                    <Tag>{item.execution_mode}</Tag>
                  </div>
                </div>
              </List.Item>
            )}
          />
          {sessionDetail && (
            <>
              <Divider />
              <Row gutter={[16, 16]}>
                <Col xs={24} md={8}>
                  <Statistic title="Runs" value={sessionDetail.runs.length} />
                </Col>
                <Col xs={24} md={8}>
                  <Statistic title="Events" value={sessionDetail.events.length} />
                </Col>
                <Col xs={24} md={8}>
                  <Statistic title="Execution Mode" value={sessionDetail.data.execution_mode} />
                </Col>
              </Row>
              <Divider />
              <JsonCard title="Intent Declaration" value={sessionDetail.data.intent_declaration} />
              <JsonCard title="Active Policy" value={sessionDetail.active_policy || {}} />
              <JsonCard title="Workflow Template" value={sessionDetail.workflow_template || {}} />
              <JsonCard title="Task Graph" value={sessionDetail.data.task_graph} />
            </>
          )}
        </Card>
      </Col>
    </Row>
  )

  const renderConstraints = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={9}>
        <Card className="lab-panel sticky-panel" title="New Constraint Document">
          <Space direction="vertical" size={16} className="w-full">
            <Input value={newConstraintTitle} onChange={(event) => setNewConstraintTitle(event.target.value)} placeholder="Constraint title" />
            <TextArea rows={8} value={newConstraintBody} onChange={(event) => setNewConstraintBody(event.target.value)} />
            <Button type="primary" onClick={createConstraint}>
              Create Constraint
            </Button>
          </Space>
        </Card>
      </Col>
      <Col xs={24} xl={15}>
        <Card className="lab-panel" title="Constraint Registry">
          <List
            dataSource={constraints}
            renderItem={(item) => (
              <List.Item actions={[item.status !== 'published' ? <Button size="small" onClick={() => void api.publishConstraint(item.document_id).then(refreshSharedData)}>Publish</Button> : null].filter(Boolean)}>
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{item.title}</span>
                      <StatusPill value={item.status} />
                    </Space>
                  }
                  description={item.body}
                />
              </List.Item>
            )}
          />
        </Card>
      </Col>
    </Row>
  )

  const renderContext = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={16}>
        <Card className="lab-panel" title="Context Blocks">
          <List
            dataSource={contextPreview?.blocks || []}
            renderItem={(item) => (
              <List.Item>
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{item.title}</span>
                      <Tag>{item.layer}</Tag>
                      <Tag color={item.selected ? 'green' : 'default'}>{item.selected ? 'selected' : 'truncated'}</Tag>
                    </Space>
                  }
                  description={
                    <>
                      <Paragraph className="lab-meta">{item.source_ref}</Paragraph>
                      <Paragraph>{item.content}</Paragraph>
                    </>
                  }
                />
              </List.Item>
            )}
          />
        </Card>
      </Col>
      <Col xs={24} xl={8}>
        <Card className="lab-panel sticky-panel" title="Selection Summary">
          <JsonCard title="Context Assembly" value={contextPreview?.selection_summary || {}} />
        </Card>
      </Col>
    </Row>
  )

  const renderPrompts = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={8}>
        <Card className="lab-panel sticky-panel" title="Prompt Frame Meta">
          <Statistic title="Total Token Estimate" value={promptFrame?.total_token_estimate || 0} />
          <Divider />
          <JsonCard title="Prompt Frame" value={promptFrame} />
        </Card>
      </Col>
      <Col xs={24} xl={16}>
        <Card className="lab-panel" title="Prompt Sections">
          <List
            dataSource={promptFrame?.sections || []}
            renderItem={(sectionItem) => (
              <List.Item>
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{sectionItem.title}</span>
                      <Tag>{sectionItem.section_key}</Tag>
                      <Tag>{sectionItem.token_estimate} tok</Tag>
                    </Space>
                  }
                  description={<Paragraph className="lab-pre">{sectionItem.content}</Paragraph>}
                />
              </List.Item>
            )}
          />
        </Card>
      </Col>
    </Row>
  )

  const renderRuns = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={9}>
        <Card className="lab-panel sticky-panel" title="Runs">
          <Space direction="vertical" size={12} className="w-full">
            <Button type="primary" icon={<PlayCircleOutlined />} onClick={createRun}>
              Execute Selected Session
            </Button>
            <List
              dataSource={runs}
              renderItem={(item) => (
                <List.Item
                  className={`lab-list-item ${item.run_id === selectedRunId ? 'lab-list-item-active' : ''}`}
                  onClick={() => setSelectedRunId(item.run_id)}
                >
                  <div className="lab-list-body">
                    <div>
                      <Text strong>{item.run_id}</Text>
                      <Paragraph className="lab-meta" ellipsis={{ rows: 2 }}>
                        {String(item.result?.summary || 'Trace available')}
                      </Paragraph>
                    </div>
                    <StatusPill value={item.status} />
                  </div>
                </List.Item>
              )}
            />
          </Space>
        </Card>
      </Col>
      <Col xs={24} xl={15}>
        <Card className="lab-panel" title="Run Trace">
          {runDetail ? (
            <Space direction="vertical" size={18} className="w-full">
              <Alert
                type={runDetail.data.status === 'completed' ? 'success' : runDetail.data.status === 'failed' ? 'error' : 'warning'}
                message={String(runDetail.data.result?.summary || 'No summary')}
                showIcon
              />
              <JsonCard title="Assigned Worker" value={runDetail.worker || {}} />
              <JsonCard title="Mission" value={runDetail.mission || {}} />
              <JsonCard title="Mission Phase" value={runDetail.mission_phase || {}} />
              <JsonCard title="Run Status Summary" value={runDetail.status_summary || {}} />
              <JsonCard title="Coordination Snapshot" value={runDetail.coordination_snapshot || {}} />
              <JsonCard title="Timeline Summary" value={runDetail.timeline_summary || {}} />
              <JsonCard title="Role Timeline" value={runDetail.role_timeline || []} />
              <JsonCard title="Handoffs" value={runDetail.handoffs || []} />
              <JsonCard title="Review Decisions" value={runDetail.review_verdicts || []} />
              <JsonCard title="Sandbox Summary" value={runDetail.sandbox_summary || {}} />
              <JsonCard title="Task Attempts" value={runDetail.attempts || []} />
              <JsonCard title="Worker Leases" value={runDetail.leases || []} />
              <JsonCard title="Active Policy" value={runDetail.active_policy || {}} />
              <JsonCard title="Workflow Template" value={runDetail.workflow_template || {}} />
              <JsonCard title="Model Calls" value={runDetail.data.execution_trace?.model_calls || []} />
              <JsonCard title="Policy Verdicts" value={runDetail.data.execution_trace?.policy_verdicts || []} />
              <JsonCard title="Tool Calls" value={runDetail.data.execution_trace?.tool_calls || []} />
              <JsonCard title="Recovery Events" value={runDetail.data.execution_trace?.recovery_events || []} />
              <JsonCard title="Approvals" value={runDetail.approvals} />
              <JsonCard title="Events" value={runDetail.events} />
            </Space>
          ) : (
            <Alert type="info" message="Select a run to inspect its execution trace." showIcon />
          )}
        </Card>
      </Col>
    </Row>
  )

  const renderReplays = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={8}>
        <Card className="lab-panel sticky-panel" title="Approvals Inbox">
          <List
            dataSource={approvals}
            renderItem={(approval) => (
              <List.Item
                actions={[
                  <Button size="small" onClick={() => void resolveApproval(approval.approval_id, 'approve')}>
                    Approve
                  </Button>,
                  <Button size="small" onClick={() => void resolveApproval(approval.approval_id, 'approve_once')}>
                    Approve Once
                  </Button>,
                  <Button danger size="small" onClick={() => void resolveApproval(approval.approval_id, 'deny')}>
                    Deny
                  </Button>,
                ]}
              >
                <List.Item.Meta title={approval.subject} description={approval.summary} />
              </List.Item>
            )}
          />
        </Card>
      </Col>
      <Col xs={24} xl={16}>
        <Card className="lab-panel" title="Replay Envelope">
          <JsonCard title="Replay" value={replay} />
        </Card>
      </Col>
    </Row>
  )

  const renderPolicies = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={9}>
        <Card className="lab-panel sticky-panel" title="Control Plane Candidates">
          <Space direction="vertical" size={16} className="w-full">
            <Select
              mode="multiple"
              maxCount={2}
              value={policyCompareIds}
              onChange={setPolicyCompareIds}
              options={policies.map((policy) => ({ value: policy.policy_id, label: `${policy.name} (${policy.status})` }))}
            />
            <Button type="primary" onClick={comparePolicies}>
              Compare Policies
            </Button>
            <Button onClick={createPolicyCandidate}>Create Policy Candidate</Button>
            <Divider />
            <Select
              mode="multiple"
              maxCount={2}
              value={workflowCompareIds}
              onChange={setWorkflowCompareIds}
              options={workflows.map((workflow) => ({ value: workflow.workflow_id, label: `${workflow.name} (${workflow.status})` }))}
            />
            <Button type="primary" onClick={compareWorkflows}>
              Compare Workflows
            </Button>
            <Button onClick={createWorkflowCandidate}>Create Workflow Candidate</Button>
            <Divider />
            <Select
              value={selectedCandidateId || undefined}
              onChange={setSelectedCandidateId}
              options={candidates.map((candidate) => ({
                value: candidate.candidate_id,
                label: `${candidate.kind}:${candidate.target_version_id} (${candidate.publish_status})`,
              }))}
              placeholder="Select candidate"
            />
            <Button onClick={() => selectedCandidateId && void approveCandidate(selectedCandidateId)} disabled={!selectedCandidateId}>
              Approve Candidate
            </Button>
            <Button type="primary" onClick={() => selectedCandidateId && void publishCandidate(selectedCandidateId)} disabled={!selectedCandidateId}>
              Publish Candidate
            </Button>
            <Button danger onClick={() => selectedCandidateId && void rollbackCandidate(selectedCandidateId)} disabled={!selectedCandidateId}>
              Rollback Candidate
            </Button>
          </Space>
        </Card>
      </Col>
      <Col xs={24} xl={15}>
        <Card className="lab-panel" title="Policy, Workflow, and Candidate Registry">
          <List
            dataSource={policies}
            renderItem={(policy) => (
              <List.Item actions={policy.status !== 'published' ? [<Button size="small" onClick={() => void publishPolicy(policy.policy_id)}>Publish</Button>] : []}>
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{policy.name}</span>
                      <StatusPill value={policy.status} />
                    </Space>
                  }
                  description={`constraint=${policy.constraint_set_id} context=${policy.context_profile_id} prompt=${policy.prompt_template_id}`}
                />
              </List.Item>
            )}
          />
          <Divider />
          <List
            dataSource={workflows}
            renderItem={(workflow) => (
              <List.Item>
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{workflow.name}</span>
                      <StatusPill value={workflow.status} />
                    </Space>
                  }
                  description={`scope=${workflow.scope} gates=${workflow.gates.length}`}
                />
              </List.Item>
            )}
          />
          <Divider />
          <List
            dataSource={candidates}
            renderItem={(candidate) => (
              <List.Item
                actions={[
                  candidate.requires_human_approval && !candidate.approved ? (
                    <Button size="small" onClick={() => void approveCandidate(candidate.candidate_id)}>
                      Approve
                    </Button>
                  ) : null,
                  candidate.publish_status !== 'published' ? (
                    <Button size="small" onClick={() => void publishCandidate(candidate.candidate_id)}>
                      Publish
                    </Button>
                  ) : (
                    <Button size="small" danger onClick={() => void rollbackCandidate(candidate.candidate_id)}>
                      Rollback
                    </Button>
                  ),
                ].filter(Boolean)}
              >
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{candidate.kind}:{candidate.target_version_id}</span>
                      <StatusPill value={candidate.publish_status} />
                    </Space>
                  }
                  description={`${candidate.rationale} | proposal=${String(candidate.metrics?.proposal_summary || 'n/a')} | clusters=${String((candidate.metrics?.diagnosis as { cluster_count?: number } | undefined)?.cluster_count ?? 'n/a')}`}
                />
              </List.Item>
            )}
          />
          <Divider />
          <JsonCard title="Policy Diff" value={policyDiff || { note: 'Choose two policies to compare.' }} />
          <JsonCard title="Workflow Diff" value={workflowDiff || { note: 'Choose two workflows to compare.' }} />
          <JsonCard title="Selected Candidate" value={selectedCandidate || { note: 'Select a candidate to inspect.' }} />
          <JsonCard title="Candidate Trace Evidence" value={(selectedCandidate?.metrics as { trace_evidence?: unknown } | undefined)?.trace_evidence || []} />
          <JsonCard title="Publish Gate" value={candidateGate || { note: 'Select a candidate to inspect its gate.' }} />
          <JsonCard title="Candidate Evaluations" value={selectedCandidateEvaluations} />
        </Card>
      </Col>
    </Row>
  )

  const renderExperiments = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={10}>
        <Card className="lab-panel sticky-panel" title="Evaluation Actions">
          <Space direction="vertical" size={16} className="w-full">
            <Paragraph>
              Offline evaluations score candidates on success rate, safety score, recovery score, regression count, and trace-derived budget pressure.
            </Paragraph>
            <Button type="primary" onClick={() => void createEvaluation('replay')}>
              Run Replay Evaluation
            </Button>
            <Button onClick={() => void createEvaluation('benchmark')}>Run Benchmark Evaluation</Button>
            <Button onClick={createExperiment}>
              Record Golden Trace Experiment
            </Button>
            <Divider />
            <JsonCard title="Selected Candidate" value={selectedCandidate || {}} />
          </Space>
        </Card>
      </Col>
      <Col xs={24} xl={14}>
        <Card className="lab-panel" title="Evaluation Registry">
          <List
            dataSource={evaluations}
            renderItem={(evaluation) => (
              <List.Item
                className={`lab-list-item ${evaluation.evaluation_id === selectedEvaluationId ? 'lab-list-item-active' : ''}`}
                onClick={() => setSelectedEvaluationId(evaluation.evaluation_id)}
              >
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{evaluation.evaluation_id}</span>
                      <StatusPill value={evaluation.status} />
                      <Tag>{evaluation.suite}</Tag>
                    </Space>
                  }
                  description={`success=${evaluation.success_rate} safety=${evaluation.safety_score} recovery=${evaluation.recovery_score} regressions=${evaluation.regression_count}`}
                />
              </List.Item>
            )}
          />
          <Divider />
          <JsonCard title="Selected Evaluation" value={selectedEvaluation || { note: 'Select an evaluation to inspect.' }} />
          <JsonCard title="Evaluation Metrics" value={selectedEvaluation?.metrics || {}} />
          <JsonCard title="Suite Manifest" value={selectedEvaluation?.suite_manifest || {}} />
          <JsonCard title="Bucket Results" value={selectedEvaluation?.bucket_results || []} />
          <JsonCard title="Hard Failures" value={selectedEvaluation?.hard_failures || []} />
          <JsonCard title="Soft Regressions" value={selectedEvaluation?.soft_regressions || []} />
          <JsonCard title="Coverage Gaps" value={selectedEvaluation?.coverage_gaps || []} />
          <Divider />
          <List
            dataSource={failureClusters}
            renderItem={(cluster) => (
              <List.Item>
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{cluster.signature_type}</span>
                      <Tag>{cluster.roles.join(', ') || 'roles:n/a'}</Tag>
                      <Tag color="red">{cluster.frequency}</Tag>
                    </Space>
                  }
                  description={`handoffs=${cluster.handoff_pairs.join(', ') || 'n/a'} reviews=${cluster.review_decisions.join(', ') || 'n/a'} policies=${cluster.affected_policies.join(', ') || 'n/a'} workflows=${cluster.affected_workflows.join(', ') || 'n/a'}`}
                />
              </List.Item>
            )}
          />
          <Divider />
          <JsonCard
            title="Benchmark Dashboard"
            value={{
              experiments,
              failure_clusters: failureClusters,
              candidate_count: candidates.length,
              evaluation_count: evaluations.length,
            }}
          />
        </Card>
      </Col>
    </Row>
  )

  const renderSettings = () => (
    <Row gutter={[20, 20]}>
      <Col xs={24} xl={8}>
        <Card className="lab-panel sticky-panel" title="Health & Doctor">
          <Space direction="vertical" size={16} className="w-full">
            <JsonCard title="Runtime Health" value={health || {}} />
            <Button onClick={registerWorker}>Register Local Worker</Button>
          </Space>
        </Card>
      </Col>
      <Col xs={24} xl={16}>
        <Card className="lab-panel" title="Mission Control Catalog">
          <JsonCard
            title="Fleet Health"
            value={{
              active_workers: health?.active_workers || [],
              draining_workers: health?.draining_workers || [],
              offline_workers: health?.offline_workers || [],
              unhealthy_workers: health?.unhealthy_workers || [],
              stuck_runs: health?.stuck_runs || [],
            }}
          />
          <Divider />
          <JsonCard title="Fleet Status" value={fleetStatus || {}} />
          <Divider />
          <JsonCard title="Queue Shards" value={queueStatus} />
          <Divider />
          <List
            header="Worker Controls"
            dataSource={workers}
            renderItem={(worker) => (
              <List.Item
                actions={[
                  worker.drain_state === 'draining' ? (
                    <Button key="resume" size="small" onClick={() => void resumeWorker(worker.worker_id)}>
                      Resume
                    </Button>
                  ) : (
                    <Button key="drain" size="small" onClick={() => void drainWorker(worker.worker_id)}>
                      Drain
                    </Button>
                  ),
                ]}
              >
                <List.Item.Meta
                  title={
                    <Space>
                      <span>{worker.label}</span>
                      <StatusPill value={worker.state} />
                      {worker.drain_state === 'draining' ? <Tag color="orange">draining</Tag> : null}
                    </Space>
                  }
                  description={`role=${worker.role_profile || 'general'} labels=${(worker.labels || []).join(', ') || 'n/a'} class=${worker.worker_class || 'general'}`}
                />
              </List.Item>
            )}
          />
          <Divider />
          <JsonCard title="Execution Plane" value={catalog?.execution_plane || {}} />
          <Divider />
          <JsonCard title="Workers" value={workers} />
          <Divider />
          <JsonCard title="Leases" value={leases} />
          <Divider />
          <JsonCard title="Settings Catalog" value={catalog || {}} />
        </Card>
      </Col>
    </Row>
  )

  const renderSection = () => {
    switch (section) {
      case 'sessions':
        return renderSessions()
      case 'constraints':
        return renderConstraints()
      case 'context':
        return renderContext()
      case 'prompts':
        return renderPrompts()
      case 'runs':
        return renderRuns()
      case 'replays':
        return renderReplays()
      case 'policies':
        return renderPolicies()
      case 'experiments':
        return renderExperiments()
      case 'settings':
      default:
        return renderSettings()
    }
  }

  return (
    <Layout className="lab-shell">
      <LabSidebar
        items={NAV_ITEMS}
        selectedKey={section}
        onSelect={(key) => setSection(key as SectionKey)}
        counts={{
          Sessions: sessions.length,
          Constraints: constraints.length,
          Runs: runs.length,
          Candidates: candidates.length,
          Workers: workers.length,
          Experiments: experiments.length,
        }}
      />
      <Layout>
        <Header className="lab-header">
          <div>
            <Title level={2} className="lab-title">
              Harness Lab
            </Title>
            <Paragraph className="lab-subtitle">
              Mission control for execution, workers, approvals, offline evaluation, workflow promotion, and self-improving multi-agent policy loops.
            </Paragraph>
          </div>
          <Space size={16}>
            <StatusPill value={selectedSession?.status || 'idle'} />
            <Tag color="processing">{selectedRun?.status || 'no-run-selected'}</Tag>
          </Space>
        </Header>
        <Content className="lab-content">{renderSection()}</Content>
      </Layout>
    </Layout>
  )
}

export default App
