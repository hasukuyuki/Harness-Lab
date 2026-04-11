from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..boundary.gateway import ToolGateway
from ..constraints.engine import ConstraintEngine
from ..context.manager import ContextManager
from ..dispatch_queue import DispatchQueue, InMemoryDispatchQueue
from ..prompting.assembler import PromptAssembler
from ..storage import PlatformStore
from ..types import (
    ApprovalRequestModel,
    ActionPlan,
    ContextAssembleRequest,
    ContextProfile,
    EventEnvelope,
    ExecutionTrace,
    ExperimentRun,
    HarnessPolicy,
    HandoffPacket,
    IntentDeclaration,
    IntentRequest,
    ModelProfile,
    ModelProviderSettings,
    Mission,
    MissionPhaseSnapshot,
    PolicyVerdict,
    PromptFrame,
    PromptRenderRequest,
    PromptTemplate,
    RecoveryEvent,
    ResearchRun,
    ResearchSession,
    ReviewVerdict,
    RunRequest,
    SessionRequest,
    DispatchEnvelope,
    DispatchConstraint,
    LeaseCompletionRequest,
    LeaseFailureRequest,
    LeaseReleaseRequest,
    LeaseSweepReport,
    TaskNode,
    TaskAttempt,
    ToolCallRecord,
    ToolExecutionResult,
    WorkerEventBatch,
    WorkerLease,
    WorkerHeartbeatRequest,
    WorkerPollRequest,
    WorkerPollResponse,
    WorkflowTemplateVersion,
)
from ..utils import new_id, utc_now
from .models import ModelRegistry
from ..orchestrator.service import OrchestratorService
from .execution_plane import LocalWorkerAdapter, RunCoordinator
from ..fleet import create_protocol_adapters
from ..fleet.lease_manager import LeaseManager
from ..fleet.worker_registry import WorkerRegistry
from ..fleet.dispatcher import Dispatcher, InMemoryDispatcher


class RuntimeService:
    """Harness-first runtime that turns sessions into traces and replays."""

    def __init__(
        self,
        database: PlatformStore,
        dispatch_queue: DispatchQueue | InMemoryDispatchQueue,
        context_manager: ContextManager,
        constraint_engine: ConstraintEngine,
        tool_gateway: ToolGateway,
        model_registry: ModelRegistry,
        orchestrator: OrchestratorService,
        prompt_assembler: PromptAssembler,
    ) -> None:
        self.database = database
        self.dispatch_queue = dispatch_queue
        self.context_manager = context_manager
        self.constraint_engine = constraint_engine
        self.tool_gateway = tool_gateway
        self.model_registry = model_registry
        self.orchestrator = orchestrator
        self.prompt_assembler = prompt_assembler
        self.worker_registry = WorkerRegistry(database)
        self.lease_timeout_seconds = 30
        self.reclaimed_lease_count = 0
        self.last_lease_sweep_at: Optional[str] = None
        self.last_lease_sweep_report = LeaseSweepReport()
        self.late_callback_count = 0
        self.run_coordinator = RunCoordinator(self)
        self.local_worker_adapter = LocalWorkerAdapter(self)
        
        # Create Dispatcher first (owns dispatch queue and matching logic)
        # Always pass the same queue instance to ensure consistency
        if hasattr(dispatch_queue, '__class__') and 'InMemory' in dispatch_queue.__class__.__name__:
            # For InMemoryDispatchQueue, create dispatcher with the existing queue
            from ..fleet.dispatcher import InMemoryDispatcher
            self.dispatcher = InMemoryDispatcher(
                worker_registry=self.worker_registry,
                database=database,
                lease_timeout_seconds=self.lease_timeout_seconds,
                existing_queue=dispatch_queue,  # Pass the same queue instance
            )
        else:
            from ..fleet.dispatcher import Dispatcher
            self.dispatcher = Dispatcher(
                queue=dispatch_queue,
                worker_registry=self.worker_registry,
                database=database,
                lease_timeout_seconds=self.lease_timeout_seconds,
            )
        
        # Create protocol adapters for LeaseManager
        adapters = create_protocol_adapters(self)
        
        # Initialize LeaseManager with protocol dependencies and dispatcher
        self.lease_manager = LeaseManager(
            database=database,
            coordination=adapters["coordination"],
            constraints=adapters["constraints"],
            context=adapters["context"],
            execution=adapters["execution"],
            utilities=adapters["utilities"],
            worker_registry=self.worker_registry,
            dispatch_queue=dispatch_queue,
            orchestrator=orchestrator,
            dispatcher=self.dispatcher,
            lease_timeout_seconds=self.lease_timeout_seconds,
        )

    def list_sessions(self, limit: int = 50) -> List[ResearchSession]:
        rows = self.database.fetchall("SELECT payload_json FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,))
        return [ResearchSession(**json.loads(row["payload_json"])) for row in rows]

    def get_session(self, session_id: str) -> ResearchSession:
        row = self.database.fetchone("SELECT payload_json FROM sessions WHERE session_id = ?", (session_id,))
        if not row:
            raise ValueError("Session not found")
        return ResearchSession(**json.loads(row["payload_json"]))

    def create_session(self, request: SessionRequest) -> ResearchSession:
        refs = self._resolve_session_refs(
            request.constraint_set_id,
            request.context_profile_id,
            request.prompt_template_id,
            request.model_profile_id,
            request.workflow_template_id,
        )
        session = ResearchSession(
            session_id=new_id("session"),
            goal=request.goal,
            status="configured",
            active_policy_id=refs["policy"].policy_id,
            workflow_template_id=refs["workflow_template"].workflow_id,
            constraint_set_id=refs["constraint"].document_id,
            context_profile_id=refs["context_profile"].context_profile_id,
            prompt_template_id=refs["prompt_template"].prompt_template_id,
            model_profile_id=refs["model_profile"].model_profile_id,
            execution_mode=request.execution_mode,
            context=request.context,
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        session.intent_declaration, session.intent_model_call = self.model_registry.declare_intent_with_trace(
            session,
            refs["model_profile"],
        )
        session.task_graph = self.orchestrator.build_task_graph(
            session,
            session.intent_declaration,
            refs["workflow_template"],
        )
        self._persist_session(session)
        self.database.append_event(
            "session.created",
            {
                "goal": session.goal,
                "active_policy_id": session.active_policy_id,
                "workflow_template_id": session.workflow_template_id,
                "execution_mode": session.execution_mode,
            },
            session_id=session.session_id,
        )
        if session.intent_model_call:
            self.database.append_event(
                "model.intent_called",
                session.intent_model_call.model_dump(),
                session_id=session.session_id,
            )
        return session

    def declare_intent(self, request: IntentRequest) -> IntentDeclaration:
        if request.session_id:
            session = self.get_session(request.session_id)
            profile = self.get_model_profile(session.model_profile_id)
            intent, _ = self.model_registry.declare_intent_with_trace(session, profile)
            return intent
        profile = self.get_model_profile(request.model_profile_id or self._default_policy().model_profile_id)
        session = self._ephemeral_session(request.goal, request.context, profile.model_profile_id)
        intent, _ = self.model_registry.declare_intent_with_trace(session, profile)
        return intent

    def assemble_context(self, request: ContextAssembleRequest) -> Dict[str, Any]:
        if request.session_id:
            session = self.get_session(request.session_id)
            profile = self.get_context_profile(session.context_profile_id)
        else:
            profile_id = request.context_profile_id or self._default_policy().context_profile_id
            session = self._ephemeral_session(request.goal or "Ad-hoc context assembly", request.context, profile_id=profile_id)
            profile = self.get_context_profile(profile_id)
            session.intent_declaration, session.intent_model_call = self.model_registry.declare_intent_with_trace(
                session,
                self.get_model_profile(session.model_profile_id),
            )
        blocks, summary = self.context_manager.assemble(session, profile, session.intent_declaration)
        return {"blocks": [block.model_dump() for block in blocks], "selection_summary": summary}

    def render_prompt(self, request: PromptRenderRequest) -> PromptFrame:
        session = self.get_session(request.session_id)
        profile = self.get_context_profile(session.context_profile_id)
        blocks, summary = self.context_manager.assemble(session, profile, session.intent_declaration)
        template = self.get_prompt_template(session.prompt_template_id)
        document = self.constraint_engine.get_document(session.constraint_set_id)
        return self.prompt_assembler.render(
            session=session,
            template=template,
            constraint_document=document,
            intent=session.intent_declaration,
            blocks=blocks,
            truncated_blocks=summary["truncated_blocks"],
        )

    def list_runs(self, limit: int = 50) -> List[ResearchRun]:
        rows = self.database.fetchall("SELECT payload_json FROM runs ORDER BY created_at DESC LIMIT ?", (limit,))
        return [ResearchRun(**json.loads(row["payload_json"])) for row in rows]

    def get_run(self, run_id: str) -> ResearchRun:
        row = self.database.fetchone("SELECT payload_json FROM runs WHERE run_id = ?", (run_id,))
        if not row:
            raise ValueError("Run not found")
        return ResearchRun(**json.loads(row["payload_json"]))

    async def create_run(self, request: RunRequest) -> ResearchRun:
        self.reclaim_stale_leases()
        session = self.get_session(request.session_id) if request.session_id else self.create_session(
            SessionRequest(
                goal=request.goal or "Research the current workspace",
                context=request.context,
                constraint_set_id=request.constraint_set_id,
                context_profile_id=request.context_profile_id,
                prompt_template_id=request.prompt_template_id,
                model_profile_id=request.model_profile_id,
                workflow_template_id=request.workflow_template_id,
                execution_mode=request.execution_mode,
            )
        )
        session.status = "running"
        session.updated_at = utc_now()
        self._persist_session(session)
        self.database.append_event("run.planning_started", {"session_id": session.session_id}, session_id=session.session_id)

        run_id = new_id("run")
        context_profile = self.get_context_profile(session.context_profile_id)
        prompt_template = self.get_prompt_template(session.prompt_template_id)
        constraint_document = self.constraint_engine.get_document(session.constraint_set_id)
        blocks, summary = self.context_manager.assemble(session, context_profile, session.intent_declaration)
        prompt_frame = self.prompt_assembler.render(
            session=session,
            template=prompt_template,
            constraint_document=constraint_document,
            intent=session.intent_declaration,
            blocks=blocks,
            truncated_blocks=summary["truncated_blocks"],
        )
        preflight_result = self.tool_gateway.preflight(session.intent_declaration.suggested_action, session.constraint_set_id)
        verdicts = preflight_result["verdicts"]
        final_verdict = preflight_result["final_verdict"]
        constraint_explanation = preflight_result["explanation"]

        artifacts = [
            self.tool_gateway.create_snapshot_manifest(run_id),
            self.database.write_artifact_text(
                run_id,
                "context_bundle",
                "context_blocks.json",
                json.dumps([block.model_dump() for block in blocks], ensure_ascii=False, indent=2),
                {"selection_summary": summary},
            ),
            self.database.write_artifact_text(
                run_id,
                "prompt_frame",
                "prompt_frame.json",
                json.dumps(prompt_frame.model_dump(), ensure_ascii=False, indent=2),
                {"template_id": prompt_frame.template_id},
            ),
        ]
        knowledge_summary = summary.get("knowledge_search")
        if knowledge_summary:
            knowledge_artifact = self.database.write_artifact_text(
                run_id,
                "knowledge_search_results",
                "knowledge_search_results.json",
                json.dumps(knowledge_summary, ensure_ascii=False, indent=2),
                {
                    "query": knowledge_summary.get("query"),
                    "used_fallback": knowledge_summary.get("used_fallback"),
                    "source_coverage": knowledge_summary.get("source_coverage", {}),
                },
            )
            artifacts.append(knowledge_artifact)
        trace = ExecutionTrace(
            trace_id=new_id("trace"),
            session_id=session.session_id,
            prompt_frame_id=prompt_frame.prompt_frame_id,
            intent_declaration=session.intent_declaration,
            model_calls=[session.intent_model_call] if session.intent_model_call else [],
            context_blocks=blocks,
            policy_verdicts=verdicts,
            tool_calls=[],
            recovery_events=[],
            artifacts=artifacts,
            status="running",
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        run = ResearchRun(
            run_id=run_id,
            session_id=session.session_id,
            status="queued",
            mission_id=new_id("mission"),
            policy_id=session.active_policy_id,
            workflow_template_id=session.workflow_template_id,
            prompt_frame=prompt_frame,
            execution_trace=trace,
            result={
                "context_selection_summary": summary,
                "final_verdict": final_verdict.model_dump(),
            },
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        mission = Mission(
            mission_id=run.mission_id,
            session_id=session.session_id,
            run_id=run.run_id,
            status="queued",
            created_at=run.created_at,
            updated_at=run.updated_at,
        )
        self._persist_run(run)
        self.database.upsert_mission(mission)
        self.database.append_event("intent.declared", session.intent_declaration.model_dump(), session_id=session.session_id, run_id=run_id)
        if session.intent_model_call:
            self.database.append_event(
                "model.intent_called",
                session.intent_model_call.model_dump(),
                session_id=session.session_id,
                run_id=run_id,
            )
        self.database.append_event("context.assembled", summary, session_id=session.session_id, run_id=run_id)
        if knowledge_summary:
            self.database.append_event(
                "knowledge.search.selected",
                {
                    "query": knowledge_summary.get("query"),
                    "used_fallback": knowledge_summary.get("used_fallback"),
                    "source_coverage": knowledge_summary.get("source_coverage", {}),
                    "hit_count": len(knowledge_summary.get("hits", [])),
                },
                session_id=session.session_id,
                run_id=run_id,
            )
        self.database.append_event(
            "prompt.rendered",
            {"prompt_frame_id": prompt_frame.prompt_frame_id, "total_token_estimate": prompt_frame.total_token_estimate},
            session_id=session.session_id,
            run_id=run_id,
        )
        self.database.append_event(
            "policy.preflight",
            {
                "subject": final_verdict.subject,
                "decision": final_verdict.decision,
                "matched_rule": final_verdict.matched_rule,
                "rule_id": final_verdict.rule_id,
                "used_fallback": preflight_result["used_fallback"],
                "compiled_rule_count": preflight_result["compiled_rule_count"],
            },
            session_id=session.session_id,
            run_id=run_id,
        )
        
        # Store constraint explanation snapshot for replay/diagnosis
        constraint_snapshot = self.database.write_artifact_text(
            run_id=run_id,
            artifact_type="constraint_explanation",
            filename="constraint_explanation.json",
            content=json.dumps(constraint_explanation.model_dump(), ensure_ascii=False, indent=2),
            metadata={
                "subject": final_verdict.subject,
                "decision": final_verdict.decision,
                "used_fallback": preflight_result["used_fallback"],
                "matched_rule_count": len(preflight_result["matched_rules"]),
            },
        )
        artifacts.append(constraint_snapshot)
        self.database.append_event(
            "mission.created",
            {"mission_id": mission.mission_id, "run_id": run.run_id, "status": mission.status},
            session_id=session.session_id,
            run_id=run.run_id,
        )
        self.run_coordinator.mark_ready_nodes(session, run.run_id)
        self._persist_session(session)
        self._persist_run(run)
        if session.execution_mode == "single_worker":
            return await self.local_worker_adapter.drain_run(run.run_id)
        return self.get_run(run.run_id)

    async def resume_run(self, run_id: str) -> ResearchRun:
        self.reclaim_stale_leases()
        run = self.get_run(run_id)
        if run.status != "awaiting_approval":
            return run
        approvals = self.database.list_approvals(run_id=run_id)
        if not approvals:
            return run
        approval = approvals[0]
        session = self.get_session(run.session_id)
        if approval.status == "pending":
            return run
        if approval.decision == "deny":
            self.run_coordinator.mark_run_failed(run, session, "approval_denied", "Operator denied the requested high-risk action.")
            return self.get_run(run_id)
        if session.task_graph:
            for node in session.task_graph.nodes:
                if node.kind == "policy" and node.status == "blocked":
                    node.status = "completed"
                    node.metadata["approval_decision"] = approval.decision
        session.status = "running"
        session.updated_at = utc_now()
        run.status = "queued"
        if run.execution_trace:
            run.execution_trace.status = "queued"
            run.execution_trace.updated_at = utc_now()
        run.updated_at = utc_now()
        self._persist_session(session)
        self._persist_run(run)
        mission = self.database.get_mission_by_run(run.run_id)
        if mission:
            mission.status = "running"
            mission.updated_at = utc_now()
            self.database.upsert_mission(mission)
        self.run_coordinator.mark_ready_nodes(session, run.run_id)
        self._persist_session(session)
        if session.execution_mode == "single_worker":
            return await self.local_worker_adapter.drain_run(run.run_id)
        return self.get_run(run.run_id)

    async def resolve_approval(self, approval_id: str, decision: str) -> ApprovalRequestModel:
        approval = self.database.resolve_approval(approval_id, decision)
        run = self.get_run(approval.run_id)
        self.database.append_event(
            "approval.resolved",
            {"approval_id": approval_id, "decision": decision, "status": approval.status},
            session_id=run.session_id,
            run_id=run.run_id,
        )
        await self.resume_run(run.run_id)
        return self.database.get_approval(approval_id)

    def list_approvals(self, status: Optional[str] = None) -> List[ApprovalRequestModel]:
        return self.database.list_approvals(status=status)

    def get_replay(self, replay_id: str) -> Dict[str, Any]:
        replay = self.database.get_replay(replay_id)
        if not replay:
            raise ValueError("Replay not found")
        return replay

    def list_context_profiles(self) -> List[ContextProfile]:
        rows = self.database.fetchall("SELECT payload_json FROM context_profiles ORDER BY updated_at DESC")
        return [ContextProfile(**json.loads(row["payload_json"])) for row in rows]

    def get_context_profile(self, context_profile_id: str) -> ContextProfile:
        row = self.database.fetchone(
            "SELECT payload_json FROM context_profiles WHERE context_profile_id = ?",
            (context_profile_id,),
        )
        if not row:
            raise ValueError("Context profile not found")
        return ContextProfile(**json.loads(row["payload_json"]))

    def list_prompt_templates(self) -> List[PromptTemplate]:
        rows = self.database.fetchall("SELECT payload_json FROM prompt_templates ORDER BY updated_at DESC")
        return [PromptTemplate(**json.loads(row["payload_json"])) for row in rows]

    def get_prompt_template(self, prompt_template_id: str) -> PromptTemplate:
        row = self.database.fetchone(
            "SELECT payload_json FROM prompt_templates WHERE prompt_template_id = ?",
            (prompt_template_id,),
        )
        if not row:
            raise ValueError("Prompt template not found")
        return PromptTemplate(**json.loads(row["payload_json"]))

    def list_model_profiles(self) -> List[ModelProfile]:
        rows = self.database.fetchall("SELECT payload_json FROM model_profiles ORDER BY updated_at DESC")
        return [ModelProfile(**json.loads(row["payload_json"])) for row in rows]

    def get_model_profile(self, model_profile_id: str) -> ModelProfile:
        row = self.database.fetchone(
            "SELECT payload_json FROM model_profiles WHERE model_profile_id = ?",
            (model_profile_id,),
        )
        if not row:
            raise ValueError("Model profile not found")
        return ModelProfile(**json.loads(row["payload_json"]))

    def list_events(
        self,
        session_id: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: int = 500,
    ) -> List[EventEnvelope]:
        return self.database.list_events(session_id=session_id, run_id=run_id, limit=limit)

    def get_model_provider_settings(self, model_profile_id: Optional[str] = None) -> ModelProviderSettings:
        profile = self.get_model_profile(model_profile_id or self._default_policy().model_profile_id)
        return self.model_registry.get_provider_settings(profile)

    def get_mission(self, run_id: str) -> Optional[Mission]:
        return self.database.get_mission_by_run(run_id)

    def list_attempts(self, run_id: Optional[str] = None) -> List[TaskAttempt]:
        return self.database.list_attempts(run_id=run_id)

    def list_leases(
        self,
        run_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[WorkerLease]:
        return self.lease_manager.list_leases(run_id=run_id, worker_id=worker_id, status=status)

    def get_lease(self, lease_id: str) -> WorkerLease:
        return self.database.get_lease(lease_id)

    def rebuild_dispatch_state(self) -> None:
        self.lease_manager.rebuild_dispatch_state()

    def execution_plane_status(self) -> Dict[str, Any]:
        return self.lease_manager.execution_plane_status()

    def fleet_status(self):
        return self.lease_manager.fleet_status()

    def queue_status(self):
        return self.lease_manager.queue_status()

    def sandbox_status(self):
        return self.tool_gateway.sandbox_status()

    def run_coordination_snapshot(self, run_id: str):
        run = self.get_run(run_id)
        session = self.get_session(run.session_id)
        return self.run_coordinator.coordination_snapshot(run, session)

    def run_timeline_summary(self, run_id: str) -> Dict[str, Any]:
        return self.run_coordinator.timeline_summary(run_id)

    def run_status_summary(self, run_id: str) -> Dict[str, Any]:
        run = self.get_run(run_id)
        session = self.get_session(run.session_id)
        return self.lease_manager.run_status_summary(run, session)

    def mission_phase_snapshot(self, run_id: str) -> MissionPhaseSnapshot:
        run = self.get_run(run_id)
        session = self.get_session(run.session_id)
        task_graph = self._task_graph(session)
        active_nodes = [node for node in task_graph.nodes if node.status in {"planned", "ready", "leased", "running", "blocked"}]
        active_roles = list(dict.fromkeys(node.agent_role for node in active_nodes))
        if run.status in {"completed", "failed", "cancelled"}:
            phase = "mission_completion"
        elif any(node.kind in {"planning", "prompt"} and node.status in {"planned", "ready", "leased", "running"} for node in task_graph.nodes):
            phase = "mission_planning"
        elif any(node.kind == "context" and node.status in {"planned", "ready", "leased", "running"} for node in task_graph.nodes):
            phase = "task_decomposition"
        elif any(node.kind == "execution" and node.status in {"planned", "ready", "leased", "running"} for node in task_graph.nodes):
            phase = "wave_dispatch"
        elif any(node.kind in {"policy", "review"} and node.status in {"planned", "ready", "leased", "running", "blocked"} for node in task_graph.nodes):
            phase = "handoff_review"
        else:
            phase = "role_assignment"
        pending_handoffs = [
            packet["id"]
            for packet in self.run_handoffs(run_id)
            if self._task_node_by_id(task_graph, packet["task_node_id"]).status not in {"completed", "skipped"}
        ]
        return MissionPhaseSnapshot(
            run_id=run_id,
            phase=phase,
            active_roles=active_roles,
            pending_handoffs=pending_handoffs,
            updated_at=utc_now(),
        )

    def run_handoffs(self, run_id: str) -> List[Dict[str, Any]]:
        run = self.get_run(run_id)
        return list(run.result.get("handoffs", []))

    def run_review_verdicts(self, run_id: str) -> List[Dict[str, Any]]:
        run = self.get_run(run_id)
        return list(run.result.get("review_verdicts", []))

    def run_role_timeline(self, run_id: str) -> List[Dict[str, Any]]:
        run = self.get_run(run_id)
        session = self.get_session(run.session_id)
        nodes = {node.node_id: node for node in self._task_graph(session).nodes}
        timeline: List[Dict[str, Any]] = []
        for event in self.list_events(run_id=run_id, limit=500):
            if event.event_type not in {"task.started", "task.completed", "task.failed", "task.blocked"}:
                continue
            node = nodes.get(str(event.payload.get("node_id")))
            timeline.append(
                {
                    "event_type": event.event_type,
                    "node_id": event.payload.get("node_id"),
                    "label": event.payload.get("label"),
                    "agent_role": node.agent_role if node else event.payload.get("role"),
                    "worker_id": event.payload.get("worker_id"),
                    "created_at": event.created_at,
                }
            )
        return timeline

    def run_sandbox_summary(self, run_id: str) -> Dict[str, Any]:
        run = self.get_run(run_id)
        sandboxed_calls = []
        changed_paths: List[str] = []
        for call in run.execution_trace.tool_calls:
            trace = call.output.get("sandbox_trace") if call.output else None
            if not isinstance(trace, dict):
                continue
            sandboxed_calls.append(
                {
                    "tool_name": call.tool_name,
                    "ok": call.ok,
                    "changed_paths": call.output.get("changed_paths", []),
                    "sandbox_trace": trace,
                }
            )
            for path in call.output.get("changed_paths", []):
                if path not in changed_paths:
                    changed_paths.append(path)
        return {
            "sandboxed_call_count": len(sandboxed_calls),
            "sandbox_failure_count": len([item for item in sandboxed_calls if not item["ok"]]),
            "changed_paths": changed_paths,
            "latest_calls": sandboxed_calls[-5:],
        }

    def get_worker_health_summary(self, worker_id: str):
        return self.lease_manager.worker_health_summary(worker_id)

    def poll_worker(self, worker_id: str, request: Optional[WorkerPollRequest] = None) -> WorkerPollResponse:
        return self.lease_manager.poll_worker(worker_id, request)

    def heartbeat_lease(self, lease_id: str, request: WorkerHeartbeatRequest) -> WorkerLease:
        return self.lease_manager.heartbeat_lease(lease_id, request)

    def submit_worker_events(self, lease_id: str, batch: WorkerEventBatch) -> WorkerLease:
        return self.lease_manager.submit_worker_events(lease_id, batch)

    async def complete_lease(self, lease_id: str, request: LeaseCompletionRequest) -> ResearchRun:
        return await self.lease_manager.complete_lease(lease_id, request)

    async def fail_lease(self, lease_id: str, request: LeaseFailureRequest) -> ResearchRun:
        return await self.lease_manager.fail_lease(lease_id, request)

    async def release_lease(self, lease_id: str, request: LeaseReleaseRequest) -> ResearchRun:
        return await self.lease_manager.release_lease(lease_id, request)

    def _resolve_session_refs(
        self,
        constraint_set_id: Optional[str],
        context_profile_id: Optional[str],
        prompt_template_id: Optional[str],
        model_profile_id: Optional[str],
        workflow_template_id: Optional[str],
    ) -> Dict[str, Any]:
        policy = self._default_policy()
        constraint = self.constraint_engine.get_document(constraint_set_id or policy.constraint_set_id)
        context_profile = self.get_context_profile(context_profile_id or policy.context_profile_id)
        prompt_template = self.get_prompt_template(prompt_template_id or policy.prompt_template_id)
        model_profile = self.get_model_profile(model_profile_id or policy.model_profile_id)
        workflow_template = self.get_workflow_template(workflow_template_id or self._default_workflow().workflow_id)
        return {
            "policy": policy,
            "constraint": constraint,
            "context_profile": context_profile,
            "prompt_template": prompt_template,
            "model_profile": model_profile,
            "workflow_template": workflow_template,
        }

    def _default_policy(self) -> HarnessPolicy:
        row = self.database.fetchone(
            "SELECT payload_json FROM harness_policies WHERE status = 'published' ORDER BY updated_at DESC LIMIT 1"
        )
        if not row:
            raise ValueError("No published harness policy available")
        return HarnessPolicy(**json.loads(row["payload_json"]))

    def list_workflow_templates(self) -> List[WorkflowTemplateVersion]:
        rows = self.database.fetchall("SELECT payload_json FROM workflow_templates ORDER BY updated_at DESC")
        return [WorkflowTemplateVersion(**json.loads(row["payload_json"])) for row in rows]

    def get_workflow_template(self, workflow_id: str) -> WorkflowTemplateVersion:
        row = self.database.fetchone(
            "SELECT payload_json FROM workflow_templates WHERE workflow_id = ?",
            (workflow_id,),
        )
        if not row:
            raise ValueError("Workflow template not found")
        return WorkflowTemplateVersion(**json.loads(row["payload_json"]))

    def _default_workflow(self) -> WorkflowTemplateVersion:
        row = self.database.fetchone(
            "SELECT payload_json FROM workflow_templates WHERE status = 'published' ORDER BY updated_at DESC LIMIT 1"
        )
        if not row:
            raise ValueError("No published workflow template available")
        return WorkflowTemplateVersion(**json.loads(row["payload_json"]))

    def _ephemeral_session(
        self,
        goal: str,
        context: Dict[str, Any],
        model_profile_id: Optional[str] = None,
        profile_id: Optional[str] = None,
    ) -> ResearchSession:
        policy = self._default_policy()
        workflow = self._default_workflow()
        return ResearchSession(
            session_id=new_id("session_preview"),
            goal=goal,
            status="configured",
            active_policy_id=policy.policy_id,
            workflow_template_id=workflow.workflow_id,
            constraint_set_id=policy.constraint_set_id,
            context_profile_id=profile_id or policy.context_profile_id,
            prompt_template_id=policy.prompt_template_id,
            model_profile_id=model_profile_id or policy.model_profile_id,
            execution_mode="single_worker",
            context=context,
            created_at=utc_now(),
            updated_at=utc_now(),
        )

    def reclaim_stale_leases(self) -> LeaseSweepReport:
        return self.lease_manager.reclaim_stale_leases()

    async def _drain_run_with_local_workers(self, run_id: str) -> ResearchRun:
        return await self.local_worker_adapter.drain_run(run_id)

    async def execute_leased_task(self, lease_id: str) -> ResearchRun:
        return await self.local_worker_adapter.execute_leased_task(lease_id)

    def _next_dispatch_for_worker(self, worker) -> Optional[DispatchEnvelope]:
        return self.lease_manager.next_dispatch_for_worker(worker)

    def _create_dispatch(self, run: ResearchRun, session: ResearchSession, node: TaskNode, worker_id: str) -> DispatchEnvelope:
        return self.lease_manager.create_dispatch(run, session, node, worker_id)

    async def _after_lease_transition(self, run: ResearchRun, session: ResearchSession, node: TaskNode) -> ResearchRun:
        return await self.run_coordinator.after_lease_transition(run, session, node)

    def _apply_worker_batch(self, run: ResearchRun, batch: WorkerEventBatch, session: Optional[ResearchSession] = None) -> None:
        session = session or self.get_session(run.session_id)
        for event in batch.events:
            self.database.append_event(
                event.event_type,
                event.payload,
                session_id=session.session_id,
                run_id=run.run_id,
            )
        if run.execution_trace:
            run.execution_trace.model_calls.extend(batch.model_calls)
            run.execution_trace.tool_calls.extend(batch.tool_calls)
            run.execution_trace.recovery_events.extend(batch.recovery_events)
            run.execution_trace.artifacts.extend(batch.artifacts)
            run.execution_trace.updated_at = utc_now()
        for artifact in batch.artifacts:
            self.database.record_artifact_ref(artifact)

    def _mark_ready_nodes(self, session: ResearchSession, run_id: str) -> bool:
        return self.run_coordinator.mark_ready_nodes(session, run_id)

    def _worker_matches_node(self, worker, session: ResearchSession, node: TaskNode) -> bool:
        if getattr(worker, "drain_state", "active") == "draining":
            return False
        constraints = self._dispatch_constraint_for_node(session, node)
        if getattr(worker, "role_profile", None) and worker.role_profile != node.agent_role:
            return False
        if constraints.execution_mode and getattr(worker, "execution_mode", None) not in {constraints.execution_mode, "embedded"}:
            return False
        if constraints.requires_sandbox and not getattr(worker, "sandbox_ready", False):
            return False
        worker_labels = set(getattr(worker, "labels", []) or [])
        if any(label not in worker_labels for label in constraints.required_labels):
            return False
        if constraints.required_capabilities and any(capability not in (worker.capabilities or []) for capability in constraints.required_capabilities):
            return False
        return True

    def _dispatch_constraint_for_node(self, session: ResearchSession, node: TaskNode) -> DispatchConstraint:
        metadata = node.metadata or {}
        required_capabilities: List[str] = []
        if node.kind == "execution":
            required_capabilities = [session.intent_declaration.suggested_action.tool_name]
        tool_name = required_capabilities[0] if required_capabilities else None
        risk_level = self._tool_risk_level(tool_name) if tool_name else "low"
        required_labels = [str(item) for item in metadata.get("required_labels", [])]
        preferred_labels = [str(item) for item in metadata.get("preferred_labels", [])]
        if tool_name == "knowledge_search":
            preferred_labels = list(dict.fromkeys([*preferred_labels, "knowledge"]))
        if node.agent_role == "executor" and risk_level in {"medium", "high"}:
            preferred_labels = list(dict.fromkeys([*preferred_labels, "executor"]))
        if node.agent_role == "researcher":
            preferred_labels = list(dict.fromkeys([*preferred_labels, "research"]))
        requires_sandbox = self.tool_gateway.requires_sandbox(session.intent_declaration.suggested_action) if node.kind == "execution" else False
        queue_parts = [node.agent_role, risk_level]
        queue_parts.extend(required_labels or ["unlabeled"])
        return DispatchConstraint(
            agent_role=node.agent_role,
            required_capabilities=required_capabilities,
            required_labels=required_labels,
            preferred_labels=preferred_labels,
            execution_mode="remote_http" if session.execution_mode == "remote_worker" else None,
            requires_sandbox=requires_sandbox,
            risk_level=risk_level,
            queue_shard="/".join(queue_parts),
        )

    def _worker_sort_key(self, worker, session: ResearchSession, node: TaskNode) -> tuple[int, int, str]:
        constraints = self._dispatch_constraint_for_node(session, node)
        preferred_hits = sum(1 for label in constraints.preferred_labels if label in (worker.labels or []))
        current_load = int(getattr(worker, "lease_count", 0) or 0)
        return (-preferred_hits, current_load, worker.worker_id)

    def _dispatch_blockers_for_run(self, run: ResearchRun, session: ResearchSession) -> List[Dict[str, Any]]:
        task_graph = self._task_graph(session)
        ready_nodes = [node for node in task_graph.nodes if node.status == "ready"]
        if not ready_nodes:
            return []
        workers = self.worker_registry.list_workers()
        blockers: List[Dict[str, Any]] = []
        for node in ready_nodes:
            constraints = self._dispatch_constraint_for_node(session, node)
            role_workers = [worker for worker in workers if not worker.role_profile or worker.role_profile == node.agent_role]
            matching_workers = [worker for worker in workers if self._worker_matches_node(worker, session, node)]
            blocker = None
            if not role_workers:
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "no_role_worker",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif constraints.requires_sandbox and not any(worker.sandbox_ready for worker in role_workers):
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "awaiting_sandbox_ready_worker",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif constraints.required_labels and not any(
                all(label in (worker.labels or []) for label in constraints.required_labels)
                for worker in role_workers
            ):
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "awaiting_required_label",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif role_workers and not matching_workers and all(worker.drain_state == "draining" for worker in role_workers):
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "all_matching_workers_draining",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif role_workers and not matching_workers:
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "no_matching_worker",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            if blocker:
                blocker["queue_shard"] = constraints.queue_shard
                blockers.append(blocker)
        return blockers

    def _approval_token_for_run(self, run_id: str) -> Optional[str]:
        approvals = self.database.list_approvals(run_id=run_id)
        for approval in approvals:
            if approval.status == "approved":
                return f"approval:{approval.approval_id}:{approval.decision}"
        return None

    def _action_with_runtime_context(self, run: ResearchRun, action: ActionPlan) -> ActionPlan:
        payload = dict(action.payload)
        approval_token = self._approval_token_for_run(run.run_id)
        if approval_token:
            payload.setdefault("_approval_token", approval_token)
        sandbox_spec = self.tool_gateway.sandbox_spec_for(action, approval_token=approval_token)
        if sandbox_spec is not None:
            payload.setdefault("_sandbox_spec", sandbox_spec.model_dump())
        return action.model_copy(update={"payload": payload})

    @staticmethod
    def _result_list(run: ResearchRun, key: str) -> List[Dict[str, Any]]:
        value = run.result.setdefault(key, [])
        if isinstance(value, list):
            return value
        value = []
        run.result[key] = value
        return value

    @staticmethod
    def _merge_run_result(run: ResearchRun, payload: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(run.result or {})
        merged.update(payload)
        for preserved_key in ("handoffs", "review_verdicts"):
            if preserved_key in run.result and preserved_key not in payload:
                merged[preserved_key] = run.result[preserved_key]
        run.result = merged
        return merged

    def _record_handoffs_for_node(self, run: ResearchRun, session: ResearchSession, node: TaskNode) -> List[HandoffPacket]:
        if not session.task_graph:
            return []
        packets: List[HandoffPacket] = []
        existing = self._result_list(run, "handoffs")
        target_nodes = {item["task_node_id"] for item in existing if item.get("task_node_id")}
        artifacts = [artifact.artifact_id for artifact in run.execution_trace.artifacts[-3:]] if run.execution_trace else []
        context_refs = [ref for ref in [self._artifact_ref(run, "context_bundle"), self._artifact_ref(run, "prompt_frame")] if ref]
        for edge in session.task_graph.edges:
            if edge.source != node.node_id:
                continue
            target = self._task_node_by_id(session.task_graph, edge.target)
            if target.agent_role == node.agent_role or target.node_id in target_nodes:
                continue
            packet = HandoffPacket(
                id=new_id("handoff"),
                from_role=node.agent_role,
                to_role=target.agent_role,
                mission_id=run.mission_id,
                run_id=run.run_id,
                task_node_id=target.node_id,
                summary=f"{node.agent_role} handed off {node.label} to {target.agent_role} for {target.label}.",
                artifacts=artifacts,
                context_refs=context_refs,
                required_action=target.kind,
                open_questions=[run.result["reason"]] if run.result.get("reason") else [],
                created_at=utc_now(),
            )
            artifact = self.database.write_artifact_text(
                run.run_id,
                "handoff_packet",
                f"{packet.id}.json",
                json.dumps(packet.model_dump(), ensure_ascii=False, indent=2),
                {"from_role": packet.from_role, "to_role": packet.to_role, "task_node_id": packet.task_node_id},
            )
            if run.execution_trace:
                run.execution_trace.artifacts.append(artifact)
            node.metadata.setdefault("handoff_packet_ids", []).append(packet.id)
            existing.append(packet.model_dump())
            self.database.append_event(
                "handoff.created",
                {**packet.model_dump(), "artifact_id": artifact.artifact_id, "source_node_id": node.node_id},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            packets.append(packet)
        return packets

    def _review_decision(self, run: ResearchRun, node: TaskNode, final_verdict: Optional[PolicyVerdict] = None) -> tuple[str, str, bool]:
        if node.kind == "policy":
            verdict = final_verdict or self._stored_final_verdict(run)
            if verdict.decision in {"approval_required", "deny"}:
                return "escalate", verdict.reason, False
            return "accept", "Policy preflight accepted the mission for role handoff.", False
        if run.execution_trace and run.execution_trace.recovery_events:
            latest = run.execution_trace.recovery_events[-1]
            return "request_repair", latest.summary, True
        if self.run_handoffs(run.run_id):
            return "complete", "Reviewer accepted the current handoff chain and mission output.", False
        return "accept", "Reviewer accepted the current stage output.", False

    def _record_review_verdict(
        self,
        run: ResearchRun,
        session: ResearchSession,
        node: TaskNode,
        decision: str,
        summary: str,
        repair_requested: bool = False,
    ) -> ReviewVerdict:
        verdict = ReviewVerdict(
            id=new_id("review"),
            run_id=run.run_id,
            task_node_id=node.node_id,
            role=node.agent_role,
            decision=decision,
            summary=summary,
            repair_requested=repair_requested,
            created_at=utc_now(),
        )
        self._result_list(run, "review_verdicts").append(verdict.model_dump())
        self.database.append_event(
            "review.decided",
            verdict.model_dump(),
            session_id=session.session_id,
            run_id=run.run_id,
        )
        node.metadata["review_decision"] = decision
        return verdict

    def _lease_expiry(self) -> str:
        return (datetime.now(timezone.utc) + timedelta(seconds=self.lease_timeout_seconds)).isoformat()

    @staticmethod
    def _utc_datetime(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _release_worker_assignment(self, worker_id: str, error: Optional[str] = None) -> None:
        worker = self.worker_registry.get_worker(worker_id)
        worker.state = "draining" if worker.drain_state == "draining" and not error else ("unhealthy" if error else "idle")
        worker.current_run_id = None
        worker.current_task_node_id = None
        worker.current_lease_id = None
        worker.last_error = error
        worker.heartbeat_at = utc_now()
        worker.updated_at = worker.heartbeat_at
        self.worker_registry._persist(worker)

    @staticmethod
    def _task_node_by_id(task_graph, node_id: str) -> TaskNode:
        if not task_graph:
            raise ValueError("Task graph is missing")
        for node in task_graph.nodes:
            if node.node_id == node_id:
                return node
        raise ValueError(f"Task node not found: {node_id}")

    @staticmethod
    def _artifact_ref(run: ResearchRun, artifact_type: str) -> Optional[str]:
        if not run.execution_trace:
            return None
        for artifact in run.execution_trace.artifacts:
            if artifact.artifact_type == artifact_type:
                return artifact.storage_key or artifact.relative_path
        return None

    def _stored_final_verdict(self, run: ResearchRun) -> PolicyVerdict:
        stored = run.result.get("final_verdict")
        if isinstance(stored, dict) and stored:
            return PolicyVerdict(**stored)
        if run.execution_trace:
            return self.constraint_engine.final_verdict(run.execution_trace.policy_verdicts)
        raise ValueError("Run does not have a final policy verdict")

    def _tool_risk_level(self, tool_name: str) -> str:
        for tool in self.tool_gateway.list_tools():
            if tool.name == tool_name:
                return tool.risk_level
        return "unknown"

    @staticmethod
    def _new_recovery_event(kind: str, summary: str) -> RecoveryEvent:
        return RecoveryEvent(
            recovery_id=new_id("recovery"),
            kind=kind,
            summary=summary,
            created_at=utc_now(),
        )

    async def _execute_task_graph(
        self,
        run: ResearchRun,
        session: ResearchSession,
        context_summary: Optional[Dict[str, Any]] = None,
        final_verdict: Optional[PolicyVerdict] = None,
    ) -> ResearchRun:
        task_graph = self._task_graph(session)
        final_verdict = final_verdict or self.constraint_engine.final_verdict(run.execution_trace.policy_verdicts)
        run.status = "running"
        run.execution_trace.status = "running"
        run.updated_at = utc_now()
        session.status = "running"
        session.updated_at = utc_now()
        self._persist_run(run)
        self._persist_session(session)

        while True:
            skipped_nodes = self.orchestrator.skip_unreachable_nodes(task_graph)
            for skipped in skipped_nodes:
                self.database.append_event(
                    "task.skipped",
                    {"node_id": skipped.node_id, "label": skipped.label, "kind": skipped.kind, "reason": skipped.metadata.get("skip_reason")},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
            ready_nodes = self.orchestrator.next_wave(task_graph)
            if not ready_nodes:
                break
            idle_workers = [item for item in self.worker_registry.list_workers() if item.state in {"idle", "registering"}]
            if not idle_workers:
                idle_workers = [self.worker_registry.ensure_default_worker()]
            chunk_size = max(1, len(idle_workers))
            for offset in range(0, len(ready_nodes), chunk_size):
                chunk = ready_nodes[offset : offset + chunk_size]
                wave_id = new_id("wave")
                self.database.append_event(
                    "wave.started",
                    {"wave_id": wave_id, "node_ids": [node.node_id for node in chunk], "size": len(chunk)},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
                allocated = []
                for node in chunk:
                    worker = self.worker_registry.acquire_worker(run.run_id, node.node_id)
                    allocated.append((node, worker))
                    run.assigned_worker_id = worker.worker_id
                    self.orchestrator.mark_node_status(
                        task_graph,
                        node.node_id,
                        "running",
                        {"worker_id": worker.worker_id, "started_at": utc_now()},
                    )
                    self.database.append_event(
                        "worker.assigned",
                        {
                            "worker_id": worker.worker_id,
                            "state": "executing",
                            "run_id": run.run_id,
                            "task_node_id": node.node_id,
                            "agent_role": node.agent_role,
                        },
                        session_id=session.session_id,
                        run_id=run.run_id,
                    )
                    self.database.append_event(
                        "task.started",
                        {
                            "node_id": node.node_id,
                            "label": node.label,
                            "kind": node.kind,
                            "role": node.role,
                            "agent_role": node.agent_role,
                            "worker_id": worker.worker_id,
                        },
                        session_id=session.session_id,
                        run_id=run.run_id,
                    )

                for node, worker in allocated:
                    outcome = await self._execute_task_node(
                        node=node,
                        run=run,
                        session=session,
                        context_summary=context_summary,
                        final_verdict=final_verdict,
                    )
                    release_error = outcome.get("release_error")
                    released = self.worker_registry.release_worker(worker.worker_id, error=release_error)
                    self.database.append_event(
                        "worker.released",
                        {
                            "worker_id": worker.worker_id,
                            "state": released.state,
                            "task_node_id": node.node_id,
                            "reason": outcome.get("reason"),
                        },
                        session_id=session.session_id,
                        run_id=run.run_id,
                    )
                    if outcome["status"] == "awaiting_approval":
                        self._persist_run(run)
                        self._persist_session(session)
                        self._persist_replay(run)
                        return self.get_run(run.run_id)
                    if outcome["status"] == "failed" and not self.orchestrator.has_node_kind(task_graph, "recovery"):
                        self._persist_run(run)
                        self._persist_session(session)
                        self._persist_replay(run)
                        return self.get_run(run.run_id)

                self.database.append_event(
                    "wave.completed",
                    {"wave_id": wave_id, "node_ids": [node.node_id for node in chunk]},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
                run.updated_at = utc_now()
                session.updated_at = utc_now()
                self._persist_run(run)
                self._persist_session(session)

        if self.orchestrator.has_failed_nodes(task_graph):
            if run.status != "failed":
                self._mark_run_failed(run, session, "workflow_failed", "One or more task nodes failed.")
        elif run.status not in {"completed", "awaiting_approval"}:
            run.status = "completed"
            run.execution_trace.status = "completed"
            self._merge_run_result(
                run,
                {
                "summary": "Harness Lab run completed with a replayable trace.",
                "output": run.result.get("output", {}),
                "final_action": session.intent_declaration.suggested_action.model_dump(),
                "completed_nodes": [node.node_id for node in task_graph.nodes if node.status == "completed"],
                "context_selection_summary": run.result.get("context_selection_summary", context_summary or {}),
                "final_verdict": run.result.get("final_verdict", final_verdict.model_dump()),
                },
            )
            session.status = "completed"
            self.database.append_event(
                "run.completed",
                {
                    "summary": run.result["summary"],
                    "tool_name": session.intent_declaration.suggested_action.tool_name,
                    "completed_nodes": len([node for node in task_graph.nodes if node.status == "completed"]),
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
            run.execution_trace.updated_at = utc_now()
            run.updated_at = utc_now()
            session.updated_at = utc_now()
            self._persist_run(run)
            self._persist_session(session)
            self._persist_replay(run)
        return self.get_run(run.run_id)

    async def _execute_task_node(
        self,
        node: TaskNode,
        run: ResearchRun,
        session: ResearchSession,
        context_summary: Optional[Dict[str, Any]],
        final_verdict: PolicyVerdict,
    ) -> Dict[str, Any]:
        if node.kind == "planning":
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {
                    "completed_at": utc_now(),
                    "summary": session.intent_declaration.intent,
                    "action": session.intent_declaration.suggested_action.tool_name,
                },
            )
            self.database.append_event(
                "task.completed",
                {"node_id": node.node_id, "label": node.label, "summary": session.intent_declaration.intent},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self._record_handoffs_for_node(run, session, node)
            return {"status": "completed"}

        if node.kind == "context":
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {"completed_at": utc_now(), "selection_summary": context_summary or {}},
            )
            self.database.append_event(
                "task.completed",
                {"node_id": node.node_id, "label": node.label, "selection_summary": context_summary or {}},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self._record_handoffs_for_node(run, session, node)
            return {"status": "completed"}

        if node.kind == "prompt":
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {
                    "completed_at": utc_now(),
                    "prompt_frame_id": run.prompt_frame.prompt_frame_id if run.prompt_frame else None,
                    "total_token_estimate": run.prompt_frame.total_token_estimate if run.prompt_frame else 0,
                },
            )
            self.database.append_event(
                "task.completed",
                {
                    "node_id": node.node_id,
                    "label": node.label,
                    "prompt_frame_id": run.prompt_frame.prompt_frame_id if run.prompt_frame else None,
                    "total_token_estimate": run.prompt_frame.total_token_estimate if run.prompt_frame else 0,
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self._record_handoffs_for_node(run, session, node)
            return {"status": "completed"}

        if node.kind == "policy":
            if final_verdict.decision == "deny":
                self.orchestrator.mark_node_status(
                    session.task_graph,
                    node.node_id,
                    "failed",
                    {"completed_at": utc_now(), "reason": final_verdict.reason},
                )
                self.database.append_event(
                    "task.failed",
                    {"node_id": node.node_id, "label": node.label, "reason": final_verdict.reason},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
                self._mark_run_failed(run, session, "policy_denied", final_verdict.reason)
                return {"status": "failed", "reason": final_verdict.reason, "release_error": final_verdict.reason}
            if final_verdict.decision == "approval_required":
                approvals = self.database.list_approvals(run_id=run.run_id)
                approval = approvals[0] if approvals else self.database.create_approval(
                    run_id=run.run_id,
                    verdict_id=final_verdict.verdict_id,
                    subject=final_verdict.subject,
                    summary=final_verdict.reason,
                    payload=session.intent_declaration.suggested_action.payload,
                )
                self.orchestrator.mark_node_status(
                    session.task_graph,
                    node.node_id,
                    "blocked",
                    {"completed_at": utc_now(), "approval_id": approval.approval_id, "reason": approval.summary},
                )
                run.status = "awaiting_approval"
                self._merge_run_result(
                    run,
                    {
                    "summary": "Run is waiting for operator approval.",
                    "approval_id": approval.approval_id,
                    "final_verdict": final_verdict.model_dump(),
                    "context_selection_summary": run.result.get("context_selection_summary", context_summary or {}),
                    },
                )
                run.execution_trace.status = "awaiting_approval"
                run.updated_at = utc_now()
                session.status = "awaiting_approval"
                session.updated_at = utc_now()
                self.database.append_event(
                    "approval.requested",
                    {"approval_id": approval.approval_id, "subject": approval.subject, "summary": approval.summary},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
                self.database.append_event(
                    "task.blocked",
                    {"node_id": node.node_id, "label": node.label, "approval_id": approval.approval_id},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
                return {"status": "awaiting_approval", "reason": approval.summary}
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {"completed_at": utc_now(), "decision": final_verdict.decision, "matched_rule": final_verdict.matched_rule},
            )
            self.database.append_event(
                "task.completed",
                {"node_id": node.node_id, "label": node.label, "decision": final_verdict.decision},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            decision, summary, repair_requested = self._review_decision(run, node, final_verdict=final_verdict)
            self._record_review_verdict(run, session, node, decision, summary, repair_requested)
            self._record_handoffs_for_node(run, session, node)
            return {"status": "completed"}

        if node.kind == "execution":
            result = await self._execute_action(run, session)
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed" if result.ok else "failed",
                {
                    "completed_at": utc_now(),
                    "tool_name": session.intent_declaration.suggested_action.tool_name,
                    "ok": result.ok,
                    "error": result.error,
                },
            )
            if result.ok:
                self._merge_run_result(
                    run,
                    {
                    "summary": "Execution finished; awaiting review and learning stages.",
                    "output": result.output,
                    "final_action": session.intent_declaration.suggested_action.model_dump(),
                    "final_verdict": run.result.get("final_verdict", final_verdict.model_dump()),
                    "context_selection_summary": run.result.get("context_selection_summary", context_summary or {}),
                    },
                )
                self.database.append_event(
                    "task.completed",
                    {"node_id": node.node_id, "label": node.label, "tool_name": session.intent_declaration.suggested_action.tool_name},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
                self._record_handoffs_for_node(run, session, node)
                return {"status": "completed"}
            recovery = RecoveryEvent(
                recovery_id=new_id("recovery"),
                kind="tool_failure",
                summary=result.error or "Unknown tool failure",
                created_at=utc_now(),
            )
            run.execution_trace.recovery_events.append(recovery)
            run.execution_trace.status = "recovering" if self.orchestrator.has_node_kind(session.task_graph, "recovery") else "failed"
            run.status = "recovering" if self.orchestrator.has_node_kind(session.task_graph, "recovery") else "failed"
            self._merge_run_result(
                run,
                {
                "summary": "Run failed during execution.",
                "reason": recovery.summary,
                "final_verdict": run.result.get("final_verdict", final_verdict.model_dump()),
                "context_selection_summary": run.result.get("context_selection_summary", context_summary or {}),
                },
            )
            session.status = "running" if self.orchestrator.has_node_kind(session.task_graph, "recovery") else "failed"
            self.database.append_event(
                "task.failed",
                {"node_id": node.node_id, "label": node.label, "reason": recovery.summary},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            if not self.orchestrator.has_node_kind(session.task_graph, "recovery"):
                self.database.append_event(
                    "run.failed",
                    {"summary": run.result["summary"], "reason": recovery.summary},
                    session_id=session.session_id,
                    run_id=run.run_id,
                )
            return {"status": "failed", "reason": recovery.summary, "release_error": recovery.summary}

        if node.kind == "recovery":
            artifact = self.database.write_artifact_text(
                run.run_id,
                "recovery_packet",
                "recovery_packet.json",
                json.dumps(
                    {
                        "run_id": run.run_id,
                        "session_id": session.session_id,
                        "recovery_events": [item.model_dump() for item in run.execution_trace.recovery_events],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                {"node_id": node.node_id},
            )
            run.execution_trace.artifacts.append(artifact)
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {"completed_at": utc_now(), "artifact_id": artifact.artifact_id},
            )
            self.database.append_event(
                "task.completed",
                {"node_id": node.node_id, "label": node.label, "artifact_id": artifact.artifact_id},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self._record_handoffs_for_node(run, session, node)
            return {"status": "completed"}

        if node.kind == "review":
            review_summary = {
                "tool_calls": len(run.execution_trace.tool_calls),
                "recovery_events": len(run.execution_trace.recovery_events),
                "policy_verdicts": len(run.execution_trace.policy_verdicts),
            }
            decision, summary, repair_requested = self._review_decision(run, node)
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {"completed_at": utc_now(), "review": review_summary, "decision": decision},
            )
            self.database.append_event(
                "task.completed",
                {"node_id": node.node_id, "label": node.label, "review": review_summary, "decision": decision},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self._record_review_verdict(run, session, node, decision, summary, repair_requested)
            self._record_handoffs_for_node(run, session, node)
            return {"status": "completed"}

        if node.kind == "learning":
            artifact = self.database.write_artifact_text(
                run.run_id,
                "learning_summary",
                "run_summary.json",
                json.dumps(
                    {
                        "run_id": run.run_id,
                        "session_id": session.session_id,
                        "goal": session.goal,
                        "result": run.result,
                        "tool_calls": [item.model_dump() for item in run.execution_trace.tool_calls],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                {"node_id": node.node_id},
            )
            run.execution_trace.artifacts.append(artifact)
            self.orchestrator.mark_node_status(
                session.task_graph,
                node.node_id,
                "completed",
                {"completed_at": utc_now(), "artifact_id": artifact.artifact_id},
            )
            self.database.append_event(
                "task.completed",
                {"node_id": node.node_id, "label": node.label, "artifact_id": artifact.artifact_id},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            self._record_handoffs_for_node(run, session, node)
            return {"status": "completed"}

        self.orchestrator.mark_node_status(session.task_graph, node.node_id, "completed", {"completed_at": utc_now()})
        self.database.append_event(
            "task.completed",
            {"node_id": node.node_id, "label": node.label, "kind": node.kind},
            session_id=session.session_id,
            run_id=run.run_id,
        )
        self._record_handoffs_for_node(run, session, node)
        return {"status": "completed"}

    async def _execute_action(self, run: ResearchRun, session: ResearchSession):
        action = self._action_with_runtime_context(run, session.intent_declaration.suggested_action)
        if self._action_requires_approval_token(action) and not self._approval_token_for_run(run.run_id):
            result = ToolExecutionResult(ok=False, error="Mutating action is missing an approval token.")
            call = ToolCallRecord(
                tool_name=action.tool_name,
                payload=session.intent_declaration.suggested_action.payload,
                ok=result.ok,
                output=result.output,
                error=result.error,
                created_at=utc_now(),
            )
            run.execution_trace.tool_calls.append(call)
            self.database.append_event(
                "tool.executed",
                {"tool_name": call.tool_name, "ok": call.ok, "error": call.error},
                session_id=session.session_id,
                run_id=run.run_id,
            )
            return result
        if session.intent_declaration.suggested_action.tool_name == "model_reflection":
            model_profile = self.get_model_profile(session.model_profile_id)
            reflection, model_call = self.model_registry.reflect_with_trace(
                prompt=str(session.intent_declaration.suggested_action.payload.get("prompt", session.goal)),
                profile=model_profile,
                extra={"session_id": session.session_id, "run_id": run.run_id},
            )
            run.execution_trace.model_calls.append(model_call)
            self.database.append_event(
                "model.reflection_called",
                model_call.model_dump(),
                session_id=session.session_id,
                run_id=run.run_id,
            )
            result = self.tool_gateway.model_reflection_result(reflection)
        else:
            result = await self.tool_gateway.execute(run.run_id, action)
        call = ToolCallRecord(
            tool_name=action.tool_name,
            payload=session.intent_declaration.suggested_action.payload,
            ok=result.ok,
            output=result.output,
            error=result.error,
            created_at=utc_now(),
        )
        run.execution_trace.tool_calls.append(call)
        self.database.append_event(
            "tool.executed",
            {
                "tool_name": call.tool_name,
                "ok": call.ok,
                "error": call.error,
                "sandboxed": isinstance(call.output.get("sandbox_trace"), dict),
                "changed_paths": call.output.get("changed_paths", []),
            },
            session_id=session.session_id,
            run_id=run.run_id,
        )
        sandbox_trace = call.output.get("sandbox_trace")
        if isinstance(sandbox_trace, dict):
            self.database.append_event(
                "sandbox.executed" if call.ok else "sandbox.failed",
                {
                    "tool_name": call.tool_name,
                    "worker_id": run.assigned_worker_id,
                    "changed_paths": call.output.get("changed_paths", []),
                    "sandbox_trace": sandbox_trace,
                },
                session_id=session.session_id,
                run_id=run.run_id,
            )
        return result

    @staticmethod
    def _action_requires_approval_token(action) -> bool:
        if action.tool_name == "shell":
            return True
        if action.tool_name == "filesystem" and action.payload.get("action") == "write_file":
            return True
        return False

    @staticmethod
    def _task_graph(session: ResearchSession):
        if not session.task_graph:
            raise ValueError("Session has no task graph")
        return session.task_graph

    def _mark_run_failed(self, run: ResearchRun, session: ResearchSession, kind: str, reason: str) -> None:
        self.run_coordinator.mark_run_failed(run, session, kind, reason)

    def _mark_run_failed_impl(self, run: ResearchRun, session: ResearchSession, kind: str, reason: str) -> None:
        run.status = "failed"
        run.execution_trace.status = "failed"
        run.active_lease_id = None
        run.current_attempt_id = None
        run.execution_trace.recovery_events.append(
            RecoveryEvent(recovery_id=new_id("recovery"), kind=kind, summary=reason, created_at=utc_now())
        )
        self._merge_run_result(run, {"summary": "Run terminated before execution.", "reason": reason})
        run.execution_trace.updated_at = utc_now()
        run.updated_at = utc_now()
        session.status = "failed"
        session.updated_at = utc_now()
        if run.assigned_worker_id:
            self.worker_registry.release_worker(run.assigned_worker_id, error=reason)
            self.database.append_event(
                "worker.released",
                {"worker_id": run.assigned_worker_id, "state": "unhealthy", "reason": reason},
                session_id=session.session_id,
                run_id=run.run_id,
            )
        self._persist_run(run)
        self._persist_session(session)
        mission = self.database.get_mission_by_run(run.run_id)
        if mission:
            mission.status = "failed"
            mission.updated_at = utc_now()
            self.database.upsert_mission(mission)
        self.database.append_event("run.failed", {"summary": run.result["summary"], "reason": reason}, session_id=session.session_id, run_id=run.run_id)
        self._persist_replay_impl(run)

    def _persist_replay(self, run: ResearchRun) -> None:
        self.run_coordinator.persist_replay(run)

    def _persist_replay_impl(self, run: ResearchRun) -> None:
        mission = self.get_mission(run.run_id)
        replay_payload = {
            "run": run.model_dump(),
            "session": self.get_session(run.session_id).model_dump(),
            "mission": mission.model_dump() if mission else None,
            "mission_phase": self.mission_phase_snapshot(run.run_id).model_dump(),
            "handoffs": self.run_handoffs(run.run_id),
            "review_verdicts": self.run_review_verdicts(run.run_id),
            "role_timeline": self.run_role_timeline(run.run_id),
            "events": [event.model_dump() for event in self.database.list_events(run_id=run.run_id, limit=500)],
            "approvals": [approval.model_dump() for approval in self.database.list_approvals(run_id=run.run_id)],
            "artifacts": [artifact.model_dump() for artifact in self.database.list_artifacts(run_id=run.run_id)],
            "attempts": [attempt.model_dump() for attempt in self.database.list_attempts(run_id=run.run_id)],
            "leases": [lease.model_dump() for lease in self.database.list_leases(run_id=run.run_id)],
        }
        self.database.upsert_replay(run.run_id, run.run_id, replay_payload)

    def _persist_session(self, session: ResearchSession, conn: Any | None = None) -> None:
        self.database.upsert_row(
            "sessions",
            {
                "session_id": session.session_id,
                "goal": session.goal,
                "status": session.status,
                "active_policy_id": session.active_policy_id,
                "workflow_template_id": session.workflow_template_id,
                "constraint_set_id": session.constraint_set_id,
                "context_profile_id": session.context_profile_id,
                "prompt_template_id": session.prompt_template_id,
                "model_profile_id": session.model_profile_id,
                "execution_mode": session.execution_mode,
                "payload_json": json.dumps(session.model_dump(), ensure_ascii=False),
                "created_at": session.created_at,
                "updated_at": session.updated_at,
            },
            "session_id",
            conn=conn,
        )

    def _persist_run(self, run: ResearchRun, conn: Any | None = None) -> None:
        self.database.upsert_row(
            "runs",
            {
                "run_id": run.run_id,
                "session_id": run.session_id,
                "status": run.status,
                "payload_json": json.dumps(run.model_dump(), ensure_ascii=False),
                "prompt_frame_id": run.prompt_frame.prompt_frame_id if run.prompt_frame else None,
                "mission_id": run.mission_id,
                "current_attempt_id": run.current_attempt_id,
                "active_lease_id": run.active_lease_id,
                "created_at": run.created_at,
                "updated_at": run.updated_at,
            },
            "run_id",
            conn=conn,
        )
