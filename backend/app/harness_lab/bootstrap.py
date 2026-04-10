from __future__ import annotations

import json
from typing import Any, Optional

from .boundary.gateway import ToolGateway
from .boundary.sandbox import SandboxManager
from .constraints.engine import ConstraintEngine
from .context.manager import ContextManager
from .dispatch_queue import DispatchQueue, InMemoryDispatchQueue
from .improvement.service import ImprovementService
from .knowledge.service import KnowledgeIndexService
from .optimizer.service import OptimizerService
from .orchestrator.service import OrchestratorService
from .prompting.assembler import PromptAssembler
from .runtime.models import ModelRegistry
from .runtime.service import RuntimeService
from .settings import HarnessLabSettings
from .storage import PlatformStore, PostgresPlatformStore
from .types import ConstraintDocument, ContextProfile, HarnessPolicy, ModelProfile, PromptTemplate, WorkflowTemplateVersion
from .utils import utc_now



class HarnessLabServices:
    """Service container and default catalog bootstrap."""

    def __init__(
        self,
        settings: HarnessLabSettings,
        database: PlatformStore,
        dispatch_queue: DispatchQueue | InMemoryDispatchQueue,
    ) -> None:
        self.settings = settings
        self.database = database
        self.dispatch_queue = dispatch_queue
        self.constraint_engine = ConstraintEngine(self.database)
        self.sandbox = SandboxManager(self.settings, self.database)
        self.knowledge = KnowledgeIndexService(self.database)
        self.context_manager = ContextManager(self.database, knowledge_index=self.knowledge)
        self.tool_gateway = ToolGateway(
            self.database,
            self.constraint_engine,
            knowledge_index=self.knowledge,
            sandbox_manager=self.sandbox,
        )
        self.model_registry = ModelRegistry()
        self.orchestrator = OrchestratorService()
        self.prompt_assembler = PromptAssembler()
        self.runtime = RuntimeService(
            database=self.database,
            dispatch_queue=self.dispatch_queue,
            context_manager=self.context_manager,
            constraint_engine=self.constraint_engine,
            tool_gateway=self.tool_gateway,
            model_registry=self.model_registry,
            orchestrator=self.orchestrator,
            prompt_assembler=self.prompt_assembler,
        )
        # Keep a stable worker registry surface for control-plane routes and CLI.
        self.workers = self.runtime.worker_registry
        self.optimizer = OptimizerService(self.database)
        self.improvement = ImprovementService(self.database)
        self._seed_defaults()

    def _seed_defaults(self) -> None:
        now = utc_now()
        constraint = ConstraintDocument(
            document_id="constraint_lab_research_v1",
            title="Harness Lab Research Guardrails",
            body=(
                "Use layered context instead of prompt stuffing. Prefer read-first inspection. "
                "Read-only filesystem and git inspection are allowed. Filesystem writes require approval. "
                "Destructive shell operations such as rm, chmod, chown, git commit, git push, and sed -i are denied. "
                "Unknown shell commands escalate to review. Replays and policy verdicts must remain visible."
            ),
            scope="global",
            status="published",
            tags=["research", "deny-destructive"],
            priority=90,
            source="bootstrap",
            version="v1",
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "constraints_documents",
            {
                "document_id": constraint.document_id,
                "title": constraint.title,
                "scope": constraint.scope,
                "status": constraint.status,
                "version": constraint.version,
                "payload_json": json.dumps(constraint.model_dump(), ensure_ascii=False),
                "created_at": constraint.created_at,
                "updated_at": constraint.updated_at,
            },
            "document_id",
        )

        context_profile = ContextProfile(
            context_profile_id="context_profile_layered_v1",
            name="Layered Research Context",
            description="Four-layer context strategy with structure/task/history/index separation.",
            status="published",
            config={"max_tokens": 1400, "max_blocks": 8, "history_limit": 2, "index_limit": 6},
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "context_profiles",
            {
                "context_profile_id": context_profile.context_profile_id,
                "name": context_profile.name,
                "status": context_profile.status,
                "payload_json": json.dumps(context_profile.model_dump(), ensure_ascii=False),
                "created_at": context_profile.created_at,
                "updated_at": context_profile.updated_at,
            },
            "context_profile_id",
        )

        prompt_template = PromptTemplate(
            prompt_template_id="prompt_template_structured_v1",
            name="Structured Harness Prompt",
            description="Fixed section order: constraints, goal, reference, context, history.",
            status="published",
            sections=["CONSTRAINTS", "GOAL", "REFERENCE", "CONTEXT", "HISTORY"],
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "prompt_templates",
            {
                "prompt_template_id": prompt_template.prompt_template_id,
                "name": prompt_template.name,
                "status": prompt_template.status,
                "payload_json": json.dumps(prompt_template.model_dump(), ensure_ascii=False),
                "created_at": prompt_template.created_at,
                "updated_at": prompt_template.updated_at,
            },
            "prompt_template_id",
        )

        workflow_template = WorkflowTemplateVersion(
            workflow_id="workflow_template_mission_control_v1",
            parent_id=None,
            name="Mission Control Baseline",
            description="Wave-ready baseline with parallel context and policy preparation before execution.",
            scope="global",
            status="published",
            dag={
                "nodes": [
                    {"key": "plan", "label": "Plan Mission", "kind": "planning", "role": "planner"},
                    {"key": "context", "label": "Assemble Context", "kind": "context", "role": "researcher"},
                    {"key": "prompt", "label": "Render Prompt Frame", "kind": "prompt", "role": "planner"},
                    {"key": "policy", "label": "Run Policy Preflight", "kind": "policy", "role": "reviewer"},
                    {"key": "execute", "label": "Execute Action", "kind": "execution", "role": "executor"},
                    {"key": "review", "label": "Review Outcome", "kind": "review", "role": "reviewer"},
                    {"key": "learn", "label": "Persist Learnings", "kind": "learning", "role": "recovery"},
                ],
                "edges": [
                    {"source": "plan", "target": "context", "kind": "depends_on"},
                    {"source": "plan", "target": "policy", "kind": "depends_on"},
                    {"source": "context", "target": "prompt", "kind": "depends_on"},
                    {"source": "prompt", "target": "execute", "kind": "depends_on"},
                    {"source": "policy", "target": "execute", "kind": "depends_on"},
                    {"source": "execute", "target": "review", "kind": "handoff"},
                    {"source": "review", "target": "learn", "kind": "depends_on"},
                ],
            },
            role_map={
                "planner": "Creates bounded task packets and context bundles.",
                "researcher": "Builds context packets and repository-grounded recommendations before execution.",
                "executor": "Performs the selected tool action inside policy boundaries.",
                "reviewer": "Checks the result before the run is marked complete.",
                "recovery": "Captures learnings and prepares retry-ready recovery traces.",
            },
            gates=[
                {"kind": "policy_preflight", "owner": "planner"},
                {"kind": "review_gate", "owner": "reviewer", "when": "after_execution"},
            ],
            metrics={"success_rate": 0.0, "safety_score": 1.0},
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "workflow_templates",
            {
                "workflow_id": workflow_template.workflow_id,
                "name": workflow_template.name,
                "status": workflow_template.status,
                "payload_json": json.dumps(workflow_template.model_dump(), ensure_ascii=False),
                "created_at": workflow_template.created_at,
                "updated_at": workflow_template.updated_at,
            },
            "workflow_id",
        )

        workflow_candidate = WorkflowTemplateVersion(
            workflow_id="workflow_template_recovery_ring_v1",
            parent_id=workflow_template.workflow_id,
            name="Recovery Ring Candidate",
            description="Candidate workflow with explicit recovery and escalation guards.",
            scope="global",
            status="candidate",
            dag={
                "nodes": [
                    {"key": "plan", "label": "Plan Mission", "kind": "planning", "role": "planner"},
                    {"key": "context", "label": "Assemble Context", "kind": "context", "role": "researcher"},
                    {"key": "prompt", "label": "Render Prompt Frame", "kind": "prompt", "role": "planner"},
                    {"key": "policy", "label": "Run Policy Preflight", "kind": "policy", "role": "reviewer"},
                    {"key": "execute", "label": "Execute Action", "kind": "execution", "role": "executor"},
                    {"key": "recovery", "label": "Recovery Triage", "kind": "recovery", "role": "recovery"},
                    {"key": "review", "label": "Review Outcome", "kind": "review", "role": "reviewer"},
                    {"key": "learn", "label": "Persist Learnings", "kind": "learning", "role": "recovery"},
                ],
                "edges": [
                    {"source": "plan", "target": "context", "kind": "depends_on"},
                    {"source": "plan", "target": "policy", "kind": "depends_on"},
                    {"source": "context", "target": "prompt", "kind": "depends_on"},
                    {"source": "prompt", "target": "execute", "kind": "depends_on"},
                    {"source": "policy", "target": "execute", "kind": "depends_on"},
                    {"source": "execute", "target": "recovery", "kind": "on_failure"},
                    {"source": "execute", "target": "review", "kind": "handoff"},
                    {"source": "recovery", "target": "review", "kind": "handoff"},
                    {"source": "review", "target": "learn", "kind": "depends_on"},
                ],
            },
            role_map=workflow_template.role_map,
            gates=[
                {"kind": "policy_preflight", "owner": "planner"},
                {"kind": "retry_gate", "owner": "recovery", "max_attempts": 2},
                {"kind": "review_gate", "owner": "reviewer", "when": "after_execution"},
            ],
            metrics={"success_rate": 0.0, "safety_score": 1.0},
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "workflow_templates",
            {
                "workflow_id": workflow_candidate.workflow_id,
                "name": workflow_candidate.name,
                "status": workflow_candidate.status,
                "payload_json": json.dumps(workflow_candidate.model_dump(), ensure_ascii=False),
                "created_at": workflow_candidate.created_at,
                "updated_at": workflow_candidate.updated_at,
            },
            "workflow_id",
        )

        model_profile = ModelProfile(
            model_profile_id="model_profile_lab_balanced_v1",
            name="DeepSeek Research Balanced",
            provider="deepseek",
            profile="balanced",
            status="published",
            config={
                "mode": "chat",
                "model_name": "deepseek-chat",
                "notes": "Provider-backed research profile with heuristic fallback.",
            },
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "model_profiles",
            {
                "model_profile_id": model_profile.model_profile_id,
                "name": model_profile.name,
                "provider": model_profile.provider,
                "profile": model_profile.profile,
                "status": model_profile.status,
                "payload_json": json.dumps(model_profile.model_dump(), ensure_ascii=False),
                "created_at": model_profile.created_at,
                "updated_at": model_profile.updated_at,
            },
            "model_profile_id",
        )

        baseline_policy = HarnessPolicy(
            policy_id="policy_harness_lab_baseline_v1",
            name="Harness Lab Baseline",
            status="published",
            constraint_set_id=constraint.document_id,
            context_profile_id=context_profile.context_profile_id,
            prompt_template_id=prompt_template.prompt_template_id,
            model_profile_id=model_profile.model_profile_id,
            repair_policy={"on_denial": "safe_exit", "on_failure": "trace_and_stop"},
            budget_policy={"max_prompt_tokens": 1400, "max_context_blocks": 8},
            metrics={"success_rate": 0.0, "approval_rate": 0.0},
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "harness_policies",
            {
                "policy_id": baseline_policy.policy_id,
                "name": baseline_policy.name,
                "status": baseline_policy.status,
                "constraint_set_id": baseline_policy.constraint_set_id,
                "context_profile_id": baseline_policy.context_profile_id,
                "prompt_template_id": baseline_policy.prompt_template_id,
                "model_profile_id": baseline_policy.model_profile_id,
                "payload_json": json.dumps(baseline_policy.model_dump(), ensure_ascii=False),
                "created_at": baseline_policy.created_at,
                "updated_at": baseline_policy.updated_at,
            },
            "policy_id",
        )

        explorer_policy = HarnessPolicy(
            policy_id="policy_harness_lab_explorer_v1",
            name="Harness Lab Explorer",
            status="candidate",
            constraint_set_id=constraint.document_id,
            context_profile_id=context_profile.context_profile_id,
            prompt_template_id=prompt_template.prompt_template_id,
            model_profile_id=model_profile.model_profile_id,
            repair_policy={"on_denial": "fallback_to_reflection", "on_failure": "record_trace"},
            budget_policy={"max_prompt_tokens": 1800, "max_context_blocks": 10},
            metrics={"success_rate": 0.0, "approval_rate": 0.0},
            created_at=now,
            updated_at=now,
        )
        self.database.upsert_row(
            "harness_policies",
            {
                "policy_id": explorer_policy.policy_id,
                "name": explorer_policy.name,
                "status": explorer_policy.status,
                "constraint_set_id": explorer_policy.constraint_set_id,
                "context_profile_id": explorer_policy.context_profile_id,
                "prompt_template_id": explorer_policy.prompt_template_id,
                "model_profile_id": explorer_policy.model_profile_id,
                "payload_json": json.dumps(explorer_policy.model_dump(), ensure_ascii=False),
                "created_at": explorer_policy.created_at,
                "updated_at": explorer_policy.updated_at,
            },
            "policy_id",
        )

        self.workers.ensure_default_worker()
        self.runtime.rebuild_dispatch_state()
        self.runtime.reclaim_stale_leases()

    def doctor_report(self) -> dict:
        self.runtime.reclaim_stale_leases()
        provider = self.runtime.get_model_provider_settings().model_dump()
        knowledge = self.knowledge.status().model_dump()
        sandbox = self.sandbox.status().model_dump()
        workers = self.workers.list_workers()
        candidates = self.improvement.list_candidates()
        evaluations = self.improvement.list_evaluations()
        execution = self.runtime.execution_plane_status()
        warnings = []
        if not provider["model_ready"]:
            warnings.append("Model provider is not ready; runtime will use heuristic fallback.")
        if not workers:
            warnings.append("No workers are registered with the control plane.")
        if not execution["postgres_ready"]:
            warnings.append("Postgres truth source is not reachable.")
        if not execution["redis_ready"]:
            warnings.append("Redis dispatch queue is not reachable.")
        if execution["offline_workers"]:
            warnings.append(f"Offline workers detected: {', '.join(execution['offline_workers'])}.")
        if execution["unhealthy_workers"]:
            warnings.append(f"Unhealthy workers detected: {', '.join(execution['unhealthy_workers'])}.")
        if execution["draining_workers"]:
            warnings.append(f"Workers are draining: {', '.join(execution['draining_workers'])}.")
        if execution["stuck_runs"]:
            warnings.append(f"Stuck run candidates detected: {len(execution['stuck_runs'])}.")
        if not sandbox["docker_ready"]:
            warnings.append("Docker sandbox backend is not ready.")
        elif not sandbox["sandbox_image_ready"]:
            warnings.append(f"Sandbox image is missing: {sandbox['image']}.")
        if not knowledge["ready"]:
            warnings.append("Knowledge index has not been built yet; retrieval will use live fallback search.")
        elif knowledge["fallback_mode"]:
            warnings.append("Knowledge index is running in fallback mode; semantic retrieval is unavailable.")
        published_workflows = [item for item in self.improvement.list_workflows() if item.status == "published"]
        if not published_workflows:
            warnings.append("No published workflow template is available.")
        return {
            "control_plane": {
                "sessions": len(self.runtime.list_sessions(limit=500)),
                "runs": len(self.runtime.list_runs(limit=500)),
                "policies": len(self.optimizer.list_policies()),
                "workflows": len(self.improvement.list_workflows()),
            },
            "provider": provider,
            "knowledge": knowledge,
            "sandbox": sandbox,
            "workers": {
                "count": len(workers),
                "healthy": len([item for item in workers if item.state in {"idle", "leased", "executing"}]),
                "by_state": execution["worker_count_by_state"],
                "by_role": execution["workers_by_role"],
                "draining_workers": execution["draining_workers"],
                "unhealthy_workers": [item.worker_id for item in workers if item.state in {"offline", "unhealthy"}],
                "active_workers": execution["active_workers"],
            },
            "execution_plane": execution,
            "improvement_plane": {
                "candidates": len(candidates),
                "published_candidates": len([item for item in candidates if item.publish_status == "published"]),
                "evaluations": len(evaluations),
            },
            "warnings": warnings,
            "doctor_ready": (
                provider["model_ready"]
                and bool(workers)
                and bool(published_workflows)
                and execution["postgres_ready"]
                and execution["redis_ready"]
                and sandbox["docker_ready"]
                and sandbox["sandbox_image_ready"]
            ),
        }

    def close(self) -> None:
        self.dispatch_queue.close()
        self.database.close()

_services: Optional[HarnessLabServices] = None


def create_harness_lab_services(
    settings: HarnessLabSettings | None = None,
    database: PlatformStore | None = None,
    dispatch_queue: DispatchQueue | InMemoryDispatchQueue | None = None,
) -> HarnessLabServices:
    active_settings = settings or HarnessLabSettings.from_env()
    active_database = database or PostgresPlatformStore(
        db_url=active_settings.db_url,
        artifact_root=active_settings.resolved_artifact_root(),
    )
    active_queue = dispatch_queue or DispatchQueue(
        redis_url=active_settings.redis_url,
        namespace=active_settings.redis_namespace,
    )
    active_database.ping()
    active_queue.ping()
    return HarnessLabServices(active_settings, active_database, active_queue)


def initialize_harness_lab_services(
    settings: HarnessLabSettings | None = None,
    database: PlatformStore | None = None,
    dispatch_queue: DispatchQueue | InMemoryDispatchQueue | None = None,
    force: bool = False,
) -> HarnessLabServices:
    global _services
    if _services is not None and not force and settings is None and database is None and dispatch_queue is None:
        return _services
    if _services is not None and force:
        _services.close()
        _services = None
    if _services is None or force:
        _services = create_harness_lab_services(settings=settings, database=database, dispatch_queue=dispatch_queue)
    return _services


def get_harness_lab_services() -> HarnessLabServices:
    return initialize_harness_lab_services()


def shutdown_harness_lab_services() -> None:
    global _services
    if _services is None:
        return
    _services.close()
    _services = None


class _LazyHarnessLabServices:
    def __getattr__(self, item: str) -> Any:
        return getattr(get_harness_lab_services(), item)


harness_lab_services = _LazyHarnessLabServices()
