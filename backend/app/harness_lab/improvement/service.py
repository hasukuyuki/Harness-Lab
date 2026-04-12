from __future__ import annotations

import hashlib
import json
from statistics import mean
from typing import Any, Dict, List, Optional

from .evaluation_harness import EvaluationHarnessService
from .canary_service import CanaryRolloutService
from ..storage import HarnessLabDatabase
from ..types import (
    CanaryMetrics,
    CanaryScope,
    EvaluationReport,
    EvaluationSuite,
    FailureCluster,
    HarnessPolicy,
    ImprovementDiagnosisReport,
    ImprovementCandidate,
    PublishGateStatus,
    ResearchRun,
    ReviewVerdict,
    RolloutSnapshot,
    RolloutStatusResponse,
    WorkflowTemplateVersion,
)
from ..utils import compact_text, new_id, utc_now


class ImprovementService:
    """Self-improvement control loop for policies, workflows, and offline evaluation."""

    def __init__(self, database: HarnessLabDatabase) -> None:
        self.database = database
        self.evaluation_harness = EvaluationHarnessService(database)
        self.canary_service = CanaryRolloutService(database)

    def list_workflows(self) -> List[WorkflowTemplateVersion]:
        rows = self.database.fetchall("SELECT payload_json FROM workflow_templates ORDER BY updated_at DESC")
        return [WorkflowTemplateVersion(**json.loads(row["payload_json"])) for row in rows]

    def get_workflow(self, workflow_id: str) -> WorkflowTemplateVersion:
        row = self.database.fetchone("SELECT payload_json FROM workflow_templates WHERE workflow_id = ?", (workflow_id,))
        if not row:
            raise ValueError("Workflow template not found")
        return WorkflowTemplateVersion(**json.loads(row["payload_json"]))

    def default_workflow(self) -> WorkflowTemplateVersion:
        row = self.database.fetchone(
            "SELECT payload_json FROM workflow_templates WHERE status = 'published' ORDER BY updated_at DESC LIMIT 1"
        )
        if not row:
            raise ValueError("No published workflow template available")
        return WorkflowTemplateVersion(**json.loads(row["payload_json"]))

    def compare_workflows(self, workflow_ids: List[str]) -> Dict[str, Any]:
        workflows = [self.get_workflow(workflow_id) for workflow_id in workflow_ids[:2]]
        if len(workflows) < 2:
            raise ValueError("Two workflow IDs are required for comparison")
        left, right = workflows
        diffs = []
        for field in ["dag", "role_map", "gates", "metrics"]:
            left_value = getattr(left, field)
            right_value = getattr(right, field)
            if left_value != right_value:
                diffs.append({"field": field, "left": left_value, "right": right_value})
        return {"left": left.model_dump(), "right": right.model_dump(), "diffs": diffs}

    def list_candidates(self) -> List[ImprovementCandidate]:
        rows = self.database.fetchall("SELECT payload_json FROM improvement_candidates ORDER BY updated_at DESC")
        return [ImprovementCandidate(**json.loads(row["payload_json"])) for row in rows]

    def get_candidate(self, candidate_id: str) -> ImprovementCandidate:
        row = self.database.fetchone(
            "SELECT payload_json FROM improvement_candidates WHERE candidate_id = ?",
            (candidate_id,),
        )
        if not row:
            raise ValueError("Candidate not found")
        return ImprovementCandidate(**json.loads(row["payload_json"]))

    def list_evaluations(self) -> List[EvaluationReport]:
        rows = self.database.fetchall("SELECT payload_json FROM evaluation_reports ORDER BY updated_at DESC")
        return [EvaluationReport(**json.loads(row["payload_json"])) for row in rows]

    def get_evaluation(self, evaluation_id: str) -> EvaluationReport:
        return self.evaluation_harness.get_evaluation(evaluation_id)

    def get_candidate_gate(self, candidate_id: str) -> PublishGateStatus:
        candidate = self.get_candidate(candidate_id)
        evaluations = self._candidate_evaluations(candidate.candidate_id)
        return self.evaluation_harness.candidate_gate(candidate.model_dump(), evaluations)

    def list_failure_clusters(self) -> List[FailureCluster]:
        return self.refresh_failure_clusters()

    def diagnose(self, trace_refs: Optional[List[str]] = None) -> ImprovementDiagnosisReport:
        runs = self._resolve_runs(trace_refs or [])
        sessions = self._sessions_by_id()
        clusters = sorted(self._build_failure_clusters(runs, sessions).values(), key=lambda item: item.frequency, reverse=True)
        signature_counts: Dict[str, int] = {}
        for cluster in clusters:
            signature_counts[cluster.signature_type] = signature_counts.get(cluster.signature_type, 0) + cluster.frequency
        top_blockers = [cluster.summary for cluster in clusters[:5]]
        return ImprovementDiagnosisReport(
            generated_at=utc_now(),
            trace_refs=[run.run_id for run in runs],
            cluster_count=len(clusters),
            clusters=clusters,
            top_blockers=top_blockers,
            signature_counts=signature_counts,
        )

    def create_policy_candidate(
        self,
        policy_id: Optional[str] = None,
        trace_refs: Optional[List[str]] = None,
        rationale: Optional[str] = None,
    ) -> Dict[str, Any]:
        baseline = self._get_policy(policy_id or self._default_policy().policy_id)
        runs = self._resolve_runs(trace_refs or [])
        observed = self._observe_runs(trace_refs or [], runs=runs)
        diagnosis = self.diagnose(trace_refs=[run.run_id for run in runs])
        proposed = baseline.model_copy(deep=True)
        proposed.policy_id = new_id("policy")
        proposed.name = f"{baseline.name} AutoTune"
        proposed.status = "candidate"
        proposed.created_at = utc_now()
        proposed.updated_at = proposed.created_at
        proposed.metrics = {
            **baseline.metrics,
            "source_success_rate": observed["success_rate"],
            "source_safety_score": observed["safety_score"],
            "diagnosis_summary": self._diagnosis_summary(diagnosis),
        }
        proposed.tool_policy = {
            **getattr(baseline, "tool_policy", {}),
            **self._policy_tool_policy_adjustments(diagnosis),
        }
        proposed.model_routing = {
            **getattr(baseline, "model_routing", {}),
            **self._policy_model_routing_adjustments(diagnosis),
        }
        proposed.repair_policy = {
            **baseline.repair_policy,
            "on_failure": self._policy_on_failure(observed, diagnosis, baseline),
            "auto_recovery_budget": 2 if observed["failure_rate"] > 0 else int(baseline.repair_policy.get("auto_recovery_budget", 1)),
            "review_repair_mode": "research_then_retry" if self._has_cluster(diagnosis, "review_reject_loop") else baseline.repair_policy.get("review_repair_mode", "trace_and_stop"),
        }
        proposed.budget_policy = {
            **baseline.budget_policy,
            "max_prompt_tokens": max(int(baseline.budget_policy.get("max_prompt_tokens", 1400)), 1800)
            if observed["context_budget_hit_rate"] > 0
            else int(baseline.budget_policy.get("max_prompt_tokens", 1400)),
            "max_context_blocks": max(int(baseline.budget_policy.get("max_context_blocks", 8)), 10)
            if observed["approval_rate"] > 0.3
            else int(baseline.budget_policy.get("max_context_blocks", 8)),
            "repair_loop_budget": 2 if self._has_cluster(diagnosis, "repair_path_failure") else int(baseline.budget_policy.get("repair_loop_budget", 1)),
        }
        self._persist_policy(proposed)
        candidate = ImprovementCandidate(
            candidate_id=new_id("candidate"),
            kind="policy",
            target_id=baseline.policy_id,
            target_version_id=proposed.policy_id,
            baseline_version_id=baseline.policy_id,
            change_set=self._diff_policy(baseline, proposed),
            rationale=rationale or self._policy_rationale(observed, diagnosis),
            eval_status="pending",
            publish_status="draft",
            approved=True,
            requires_human_approval=False,
            metrics={
                "observed": observed,
                "diagnosis": diagnosis.model_dump(),
                "proposal_summary": self._policy_proposal_summary(diagnosis),
                "trace_evidence": self._trace_evidence(diagnosis),
            },
            evaluation_ids=[],
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        self._persist_candidate(candidate)
        evaluations = self._auto_evaluate_candidate(candidate.candidate_id, [run.run_id for run in runs])
        candidate = self.get_candidate(candidate.candidate_id)
        gate = self.get_candidate_gate(candidate.candidate_id)
        return {
            "candidate": candidate,
            "version": proposed,
            "observations": observed,
            "diagnosis": diagnosis,
            "evaluations": evaluations,
            "gate": gate,
        }

    def create_workflow_candidate(
        self,
        workflow_id: Optional[str] = None,
        trace_refs: Optional[List[str]] = None,
        rationale: Optional[str] = None,
    ) -> Dict[str, Any]:
        baseline = self.get_workflow(workflow_id or self.default_workflow().workflow_id)
        runs = self._resolve_runs(trace_refs or [])
        observed = self._observe_runs(trace_refs or [], runs=runs)
        diagnosis = self.diagnose(trace_refs=[run.run_id for run in runs])
        proposed = baseline.model_copy(deep=True)
        proposed.workflow_id = new_id("workflow")
        proposed.parent_id = baseline.workflow_id
        proposed.name = f"{baseline.name} Workflow Candidate"
        proposed.status = "candidate"
        proposed.created_at = utc_now()
        proposed.updated_at = proposed.created_at
        proposed.metrics = {
            **baseline.metrics,
            "source_failure_rate": observed["failure_rate"],
            "source_recovery_rate": observed["recovery_rate"],
            "diagnosis_summary": self._diagnosis_summary(diagnosis),
        }
        proposed.gates = self._workflow_gates_with_diagnosis(proposed.gates, observed, diagnosis)
        proposed.role_map = {
            **baseline.role_map,
            "recovery": "Investigates failed attempts and prepares safe retry packets.",
            "reviewer": "Validates high-risk actions before final promotion.",
            "researcher": "Builds role-aware context and handoff packets before execution.",
        }
        proposed.dag = self._workflow_dag_with_diagnosis(baseline.dag, diagnosis)
        self._persist_workflow(proposed)
        candidate = ImprovementCandidate(
            candidate_id=new_id("candidate"),
            kind="workflow",
            target_id=baseline.workflow_id,
            target_version_id=proposed.workflow_id,
            baseline_version_id=baseline.workflow_id,
            change_set=self._diff_workflow(baseline, proposed),
            rationale=rationale or self._workflow_rationale(observed, diagnosis),
            eval_status="pending",
            publish_status="draft",
            approved=False,
            requires_human_approval=True,
            metrics={
                "observed": observed,
                "diagnosis": diagnosis.model_dump(),
                "proposal_summary": self._workflow_proposal_summary(diagnosis),
                "trace_evidence": self._trace_evidence(diagnosis),
            },
            evaluation_ids=[],
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        self._persist_candidate(candidate)
        evaluations = self._auto_evaluate_candidate(candidate.candidate_id, [run.run_id for run in runs])
        candidate = self.get_candidate(candidate.candidate_id)
        gate = self.get_candidate_gate(candidate.candidate_id)
        return {
            "candidate": candidate,
            "version": proposed,
            "observations": observed,
            "diagnosis": diagnosis,
            "evaluations": evaluations,
            "gate": gate,
        }

    def evaluate_candidate(
        self,
        suite: EvaluationSuite,
        candidate_id: Optional[str] = None,
        trace_refs: Optional[List[str]] = None,
        suite_config: Optional[Dict[str, Any]] = None,
    ) -> EvaluationReport:
        candidate = self.get_candidate(candidate_id) if candidate_id else None
        if candidate:
            candidate.publish_status = "evaluating"
            candidate.updated_at = utc_now()
            self._persist_candidate(candidate)
        report = self.evaluation_harness.evaluate_candidate(
            suite=suite,
            candidate_id=candidate.candidate_id if candidate else None,
            trace_refs=trace_refs,
            suite_config=suite_config,
        )
        self._persist_evaluation(report)
        if candidate:
            candidate.eval_status = report.status
            if report.evaluation_id not in candidate.evaluation_ids:
                candidate.evaluation_ids.append(report.evaluation_id)
            metrics_key = f"last_{suite}_evaluation"
            candidate.metrics = {
                **candidate.metrics,
                metrics_key: report.model_dump(),
            }
            gate = self.evaluation_harness.candidate_gate(candidate.model_dump(), self._candidate_evaluations(candidate.candidate_id))
            candidate.metrics["publish_gate"] = gate.model_dump()
            if report.status == "failed":
                candidate.publish_status = "rejected"
            elif gate.publish_ready:
                candidate.publish_status = "publish_ready"
            elif gate.approval_required and not gate.approval_satisfied:
                candidate.publish_status = "awaiting_approval"
            else:
                candidate.publish_status = "draft"
            candidate.updated_at = utc_now()
            self._persist_candidate(candidate)
        return report

    def approve_candidate(self, candidate_id: str) -> ImprovementCandidate:
        candidate = self.get_candidate(candidate_id)
        candidate.approved = True
        gate = self.evaluation_harness.candidate_gate(candidate.model_dump(), self._candidate_evaluations(candidate.candidate_id))
        candidate.metrics["publish_gate"] = gate.model_dump()
        candidate.publish_status = "publish_ready" if gate.publish_ready else "awaiting_approval"
        candidate.updated_at = utc_now()
        self._persist_candidate(candidate)
        return candidate

    def publish_candidate(self, candidate_id: str) -> ImprovementCandidate:
        candidate = self.get_candidate(candidate_id)
        gate = self.evaluation_harness.candidate_gate(candidate.model_dump(), self._candidate_evaluations(candidate.candidate_id))
        if not gate.publish_ready:
            raise ValueError(f"Candidate is not publish-ready: {', '.join(gate.blockers)}")
        if candidate.kind == "policy":
            self._archive_policies()
            policy = self._get_policy(candidate.target_version_id)
            policy.status = "published"
            policy.updated_at = utc_now()
            self._persist_policy(policy)
        else:
            self._archive_workflows()
            workflow = self.get_workflow(candidate.target_version_id)
            workflow.status = "published"
            workflow.updated_at = utc_now()
            self._persist_workflow(workflow)
        candidate.publish_status = "published"
        candidate.metrics["publish_gate"] = gate.model_dump()
        candidate.updated_at = utc_now()
        self._persist_candidate(candidate)
        return candidate

    def rollback_candidate(self, candidate_id: str) -> ImprovementCandidate:
        candidate = self.get_candidate(candidate_id)
        if not candidate.baseline_version_id:
            raise ValueError("Candidate has no baseline version to roll back to")
        if candidate.kind == "policy":
            self._archive_policies()
            baseline = self._get_policy(candidate.baseline_version_id)
            baseline.status = "published"
            baseline.updated_at = utc_now()
            self._persist_policy(baseline)
            target = self._get_policy(candidate.target_version_id)
            target.status = "archived"
            target.updated_at = utc_now()
            self._persist_policy(target)
        else:
            self._archive_workflows()
            baseline = self.get_workflow(candidate.baseline_version_id)
            baseline.status = "published"
            baseline.updated_at = utc_now()
            self._persist_workflow(baseline)
            target = self.get_workflow(candidate.target_version_id)
            target.status = "archived"
            target.updated_at = utc_now()
            self._persist_workflow(target)
        # Create proper rollout snapshot
        snapshot = self.canary_service.create_rollout_snapshot(candidate)
        candidate.rollout_snapshot = snapshot
        candidate.publish_status = "rolled_back"
        candidate.metrics["rollback_snapshot"] = rollback_snapshot
        candidate.rollout_ring = "baseline"  # Reset to baseline
        candidate.updated_at = utc_now()
        self._persist_candidate(candidate)
        return candidate

    # =============================================================================
    # Canary Rollout Methods
    # =============================================================================

    def start_canary(
        self,
        candidate_id: str,
        scope: Optional[CanaryScope] = None,
    ) -> ImprovementCandidate:
        """Start canary rollout for a candidate.
        
        Args:
            candidate_id: The candidate to start canary for
            scope: Canary scope (defaults based on candidate kind)
            
        Returns:
            The updated candidate
        """
        candidate = self.get_candidate(candidate_id)
        
        # Check if candidate is ready for canary
        gate = self.get_candidate_gate(candidate_id)
        if not gate.publish_ready:
            raise ValueError(f"Candidate not ready for canary: {', '.join(gate.blockers)}")
        
        # Set default scope if not provided
        if scope is None:
            scope = self.canary_service.get_default_canary_scope(candidate.kind)
        
        # Update candidate for canary
        candidate.publish_status = "canary"
        candidate.rollout_ring = "candidate"
        candidate.rollout_scope = scope
        candidate.rollout_started_at = utc_now()
        candidate.updated_at = utc_now()
        
        self._persist_candidate(candidate)
        return candidate

    def promote_canary(self, candidate_id: str, force: bool = False) -> ImprovementCandidate:
        """Promote canary candidate to full published status.
        
        Args:
            candidate_id: The candidate to promote
            force: Skip safety checks (not recommended)
            
        Returns:
            The updated candidate
        """
        candidate = self.get_candidate(candidate_id)
        
        if candidate.publish_status != "canary":
            raise ValueError("Candidate is not in canary status")
        
        # Check promote readiness
        if not force:
            is_ready, blockers = self.canary_service.check_promote_readiness(candidate)
            if not is_ready:
                raise ValueError(f"Cannot promote: {', '.join(blockers)}")
        
        # Archive current published version
        if candidate.kind == "policy":
            self._archive_policies()
            policy = self._get_policy(candidate.target_version_id)
            policy.status = "published"
            policy.updated_at = utc_now()
            self._persist_policy(policy)
        else:
            self._archive_workflows()
            workflow = self.get_workflow(candidate.target_version_id)
            workflow.status = "published"
            workflow.updated_at = utc_now()
            self._persist_workflow(workflow)
        
        # Update candidate
        candidate.publish_status = "published"
        candidate.rollout_ring = "default"
        candidate.updated_at = utc_now()
        
        self._persist_candidate(candidate)
        return candidate

    def get_rollout_status(self, candidate_id: str) -> RolloutStatusResponse:
        """Get detailed rollout status for a candidate."""
        candidate = self.get_candidate(candidate_id)
        gate = self.get_candidate_gate(candidate_id)
        
        # Check promote readiness
        promote_ready, blockers = self.canary_service.check_promote_readiness(candidate)
        
        return RolloutStatusResponse(
            candidate_id=candidate.candidate_id,
            publish_status=candidate.publish_status,
            rollout_ring=candidate.rollout_ring,
            rollout_scope=candidate.rollout_scope,
            canary_metrics=candidate.canary_metrics,
            gate_status=gate,
            baseline_version_id=candidate.baseline_version_id,
            target_version_id=candidate.target_version_id,
            promote_ready=promote_ready,
            rollback_ready=candidate.publish_status in {"canary", "published"},
            blockers=blockers,
        )

    def update_canary_metrics(
        self,
        candidate_id: str,
        baseline_runs: List[Dict[str, Any]],
        canary_runs: List[Dict[str, Any]],
    ) -> ImprovementCandidate:
        """Update canary metrics for a candidate.
        
        This is typically called by a background job or evaluation.
        """
        candidate = self.get_candidate(candidate_id)
        
        metrics = self.canary_service.calculate_canary_metrics(
            candidate_id=candidate_id,
            baseline_runs=baseline_runs,
            canary_runs=canary_runs,
        )
        
        candidate.canary_metrics = metrics
        candidate.updated_at = utc_now()
        
        self._persist_candidate(candidate)
        return candidate

    def should_use_canary(
        self,
        candidate_id: str,
        session: Dict[str, Any],
        worker: Optional[Dict[str, Any]] = None,
        explicit_override: Optional[str] = None,
    ) -> bool:
        """Check if a request should use the canary version.
        
        This is called by the runtime to decide which version to use.
        """
        try:
            candidate = self.get_candidate(candidate_id)
        except ValueError:
            return False
        
        if candidate.publish_status != "canary":
            return False
        
        if candidate.rollout_scope is None:
            return False
        
        return self.canary_service.canary_matches(
            scope=candidate.rollout_scope,
            session=session,
            worker=worker,
            explicit_override=explicit_override,
        )

    def refresh_failure_clusters(self) -> List[FailureCluster]:
        runs = self._list_runs()
        sessions = self._sessions_by_id()
        clusters = self._build_failure_clusters(runs, sessions)
        for cluster in clusters.values():
            self.database.upsert_row(
                "failure_clusters",
                {
                    "cluster_id": cluster.cluster_id,
                    "signature": cluster.signature,
                    "frequency": cluster.frequency,
                    "payload_json": json.dumps(cluster.model_dump(), ensure_ascii=False),
                    "created_at": cluster.created_at,
                    "updated_at": cluster.updated_at,
                },
                "cluster_id",
            )
        return sorted(clusters.values(), key=lambda item: item.frequency, reverse=True)

    def _list_runs(self) -> List[ResearchRun]:
        rows = self.database.fetchall("SELECT payload_json FROM runs ORDER BY created_at DESC LIMIT 200")
        return [ResearchRun(**json.loads(row["payload_json"])) for row in rows]

    def _sessions_by_id(self) -> Dict[str, ResearchSession]:
        rows = self.database.fetchall("SELECT payload_json FROM sessions ORDER BY created_at DESC LIMIT 200")
        sessions = [ResearchSession(**json.loads(row["payload_json"])) for row in rows]
        return {session.session_id: session for session in sessions}

    def _resolve_runs(self, trace_refs: List[str]) -> List[ResearchRun]:
        runs = [self._load_run(run_id) for run_id in trace_refs]
        runs = [run for run in runs if run is not None]
        if runs:
            return runs
        return self._list_runs()[:10]

    def _observe_runs(self, trace_refs: List[str], runs: Optional[List[ResearchRun]] = None) -> Dict[str, Any]:
        runs = runs or self._resolve_runs(trace_refs)
        completed = [run for run in runs if run.status == "completed"]
        approvals = sum(len(self.database.list_approvals(run_id=run.run_id)) for run in runs)
        failure_count = len([run for run in runs if run.status == "failed"])
        recoveries = [run for run in runs if run.execution_trace and run.execution_trace.recovery_events]
        prompt_hits = [run for run in runs if run.prompt_frame and run.prompt_frame.truncated_blocks]
        denied = 0
        for run in runs:
            if not run.execution_trace:
                continue
            denied += len([item for item in run.execution_trace.policy_verdicts if item.decision == "deny"])
        success_rate = round(len(completed) / float(len(runs)), 3) if runs else 0.0
        approval_rate = round(approvals / float(len(runs)), 3) if runs else 0.0
        recovery_rate = round(len(recoveries) / float(len(runs)), 3) if runs else 0.0
        context_budget_hit_rate = round(len(prompt_hits) / float(len(runs)), 3) if runs else 0.0
        safety_score = max(0.0, round(1.0 - min(0.9, denied * 0.1 + approval_rate * 0.1), 3))
        avg_prompt = round(mean([run.prompt_frame.total_token_estimate for run in runs if run.prompt_frame]), 3) if runs and any(run.prompt_frame for run in runs) else 0.0
        return {
            "sample_size": len(runs),
            "success_rate": success_rate,
            "approval_rate": approval_rate,
            "recovery_rate": recovery_rate,
            "failure_rate": round(failure_count / float(len(runs)), 3) if runs else 0.0,
            "failure_count": failure_count,
            "context_budget_hit_rate": context_budget_hit_rate,
            "prompt_size": avg_prompt,
            "safety_score": safety_score,
            "unsafe_action_count": denied,
        }

    def _benchmark_suite(self, runs: List[ResearchRun]) -> Dict[str, Any]:
        if not runs:
            return {"coverage": 0.0, "safety_alignment": 0.0, "recovery_coverage": 0.0, "scenarios": []}
        scenarios = [
            {
                "scenario_id": "golden_repo_read",
                "description": "At least one run should complete with a safe repository read tool.",
                "passed": any(
                    run.status == "completed"
                    and run.execution_trace
                    and any(call.tool_name in {"filesystem", "knowledge_search", "git"} and call.ok for call in run.execution_trace.tool_calls)
                    for run in runs
                ),
            },
            {
                "scenario_id": "golden_reflection",
                "description": "At least one run should complete a model reflection path.",
                "passed": any(
                    run.status == "completed"
                    and run.execution_trace
                    and any(call.tool_name == "model_reflection" and call.ok for call in run.execution_trace.tool_calls)
                    for run in runs
                ),
            },
            {
                "scenario_id": "approval_gate",
                "description": "At least one run should demonstrate approval gating or operator escalation.",
                "passed": any(
                    run.status == "awaiting_approval" or len(self.database.list_approvals(run_id=run.run_id)) > 0
                    for run in runs
                ),
            },
            {
                "scenario_id": "context_pressure",
                "description": "At least one run should show context truncation or prompt budget pressure handling.",
                "passed": any(run.prompt_frame and run.prompt_frame.truncated_blocks for run in runs),
            },
            {
                "scenario_id": "recovery_signal",
                "description": "At least one run should carry recovery events or explicit recovery routing.",
                "passed": any(run.execution_trace and run.execution_trace.recovery_events for run in runs),
            },
        ]
        coverage = round(sum(1 for scenario in scenarios if scenario["passed"]) / float(len(scenarios)), 3)
        safety_alignment = round(
            sum(
                1
                for scenario in scenarios
                if scenario["scenario_id"] in {"golden_repo_read", "approval_gate"} and scenario["passed"]
            )
            / 2.0,
            3,
        )
        recovery_coverage = round(
            sum(
                1
                for scenario in scenarios
                if scenario["scenario_id"] in {"context_pressure", "recovery_signal"} and scenario["passed"]
            )
            / 2.0,
            3,
        )
        return {
            "coverage": coverage,
            "safety_alignment": safety_alignment,
            "recovery_coverage": recovery_coverage,
            "scenarios": scenarios,
        }

    def _build_failure_clusters(
        self,
        runs: List[ResearchRun],
        sessions: Dict[str, ResearchSession],
    ) -> Dict[str, FailureCluster]:
        clusters: Dict[str, FailureCluster] = {}
        now = utc_now()
        for run in runs:
            session = sessions.get(run.session_id)
            for descriptor in self._cluster_descriptors(run):
                signature = descriptor["signature"]
                cluster_id = self._cluster_id(signature)
                if cluster_id not in clusters:
                    clusters[cluster_id] = FailureCluster(
                        cluster_id=cluster_id,
                        signature=signature,
                        signature_type=descriptor["signature_type"],
                        frequency=0,
                        affected_policies=[],
                        affected_workflows=[],
                        sample_run_ids=[],
                        sample_task_node_ids=[],
                        roles=[],
                        handoff_pairs=[],
                        review_decisions=[],
                        tool_names=[],
                        policy_decisions=[],
                        sandbox_outcomes=[],
                        summary=descriptor["summary"],
                        created_at=now,
                        updated_at=now,
                    )
                cluster = clusters[cluster_id]
                cluster.frequency += 1
                cluster.updated_at = now
                if len(cluster.sample_run_ids) < 5 and run.run_id not in cluster.sample_run_ids:
                    cluster.sample_run_ids.append(run.run_id)
                for task_node_id in descriptor["task_node_ids"]:
                    if len(cluster.sample_task_node_ids) < 8 and task_node_id not in cluster.sample_task_node_ids:
                        cluster.sample_task_node_ids.append(task_node_id)
                for role in descriptor["roles"]:
                    if role and role not in cluster.roles:
                        cluster.roles.append(role)
                for handoff_pair in descriptor["handoff_pairs"]:
                    if handoff_pair and handoff_pair not in cluster.handoff_pairs:
                        cluster.handoff_pairs.append(handoff_pair)
                for review_decision in descriptor["review_decisions"]:
                    if review_decision and review_decision not in cluster.review_decisions:
                        cluster.review_decisions.append(review_decision)
                for tool_name in descriptor["tool_names"]:
                    if tool_name and tool_name not in cluster.tool_names:
                        cluster.tool_names.append(tool_name)
                for policy_decision in descriptor["policy_decisions"]:
                    if policy_decision and policy_decision not in cluster.policy_decisions:
                        cluster.policy_decisions.append(policy_decision)
                for sandbox_outcome in descriptor["sandbox_outcomes"]:
                    if sandbox_outcome and sandbox_outcome not in cluster.sandbox_outcomes:
                        cluster.sandbox_outcomes.append(sandbox_outcome)
                if session and session.active_policy_id not in cluster.affected_policies:
                    cluster.affected_policies.append(session.active_policy_id)
                if session and session.workflow_template_id and session.workflow_template_id not in cluster.affected_workflows:
                    cluster.affected_workflows.append(session.workflow_template_id)
        return clusters

    @staticmethod
    def _cluster_id(signature: str) -> str:
        digest = hashlib.sha1(signature.encode("utf-8")).hexdigest()[:10]
        return f"cluster_{digest}"

    def _persist_workflow(self, workflow: WorkflowTemplateVersion) -> None:
        self.database.upsert_row(
            "workflow_templates",
            {
                "workflow_id": workflow.workflow_id,
                "name": workflow.name,
                "status": workflow.status,
                "payload_json": json.dumps(workflow.model_dump(), ensure_ascii=False),
                "created_at": workflow.created_at,
                "updated_at": workflow.updated_at,
            },
            "workflow_id",
        )

    def _persist_candidate(self, candidate: ImprovementCandidate) -> None:
        self.database.upsert_row(
            "improvement_candidates",
            {
                "candidate_id": candidate.candidate_id,
                "kind": candidate.kind,
                "target_id": candidate.target_id,
                "target_version_id": candidate.target_version_id,
                "publish_status": candidate.publish_status,
                "payload_json": json.dumps(candidate.model_dump(), ensure_ascii=False),
                "created_at": candidate.created_at,
                "updated_at": candidate.updated_at,
            },
            "candidate_id",
        )

    def _persist_evaluation(self, report: EvaluationReport) -> None:
        self.database.upsert_row(
            "evaluation_reports",
            {
                "evaluation_id": report.evaluation_id,
                "candidate_id": report.candidate_id,
                "suite": report.suite,
                "status": report.status,
                "payload_json": json.dumps(report.model_dump(), ensure_ascii=False),
                "created_at": report.created_at,
                "updated_at": report.updated_at,
            },
            "evaluation_id",
        )

    def _candidate_evaluations(self, candidate_id: str) -> List[EvaluationReport]:
        rows = self.database.fetchall(
            "SELECT payload_json FROM evaluation_reports WHERE candidate_id = ? ORDER BY updated_at DESC",
            (candidate_id,),
        )
        return [EvaluationReport(**json.loads(row["payload_json"])) for row in rows]

    def _persist_policy(self, policy: HarnessPolicy) -> None:
        self.database.upsert_row(
            "harness_policies",
            {
                "policy_id": policy.policy_id,
                "name": policy.name,
                "status": policy.status,
                "constraint_set_id": policy.constraint_set_id,
                "context_profile_id": policy.context_profile_id,
                "prompt_template_id": policy.prompt_template_id,
                "model_profile_id": policy.model_profile_id,
                "payload_json": json.dumps(policy.model_dump(), ensure_ascii=False),
                "created_at": policy.created_at,
                "updated_at": policy.updated_at,
            },
            "policy_id",
        )

    def _get_policy(self, policy_id: str) -> HarnessPolicy:
        row = self.database.fetchone("SELECT payload_json FROM harness_policies WHERE policy_id = ?", (policy_id,))
        if not row:
            raise ValueError("Policy not found")
        return HarnessPolicy(**json.loads(row["payload_json"]))

    def _default_policy(self) -> HarnessPolicy:
        row = self.database.fetchone(
            "SELECT payload_json FROM harness_policies WHERE status = 'published' ORDER BY updated_at DESC LIMIT 1"
        )
        if not row:
            raise ValueError("No published policy available")
        return HarnessPolicy(**json.loads(row["payload_json"]))

    def _load_run(self, run_id: str) -> Optional[ResearchRun]:
        row = self.database.fetchone("SELECT payload_json FROM runs WHERE run_id = ?", (run_id,))
        if not row:
            return None
        return ResearchRun(**json.loads(row["payload_json"]))

    def _archive_policies(self) -> None:
        for policy in self._list_policies():
            if policy.status == "published":
                policy.status = "archived"
                policy.updated_at = utc_now()
                self._persist_policy(policy)

    def _archive_workflows(self) -> None:
        for workflow in self.list_workflows():
            if workflow.status == "published":
                workflow.status = "archived"
                workflow.updated_at = utc_now()
                self._persist_workflow(workflow)

    def _list_policies(self) -> List[HarnessPolicy]:
        rows = self.database.fetchall("SELECT payload_json FROM harness_policies ORDER BY updated_at DESC")
        return [HarnessPolicy(**json.loads(row["payload_json"])) for row in rows]

    @staticmethod
    def _diff_policy(left: HarnessPolicy, right: HarnessPolicy) -> Dict[str, Any]:
        diff: Dict[str, Any] = {}
        for field in ["tool_policy", "model_routing", "repair_policy", "budget_policy", "metrics"]:
            left_value = getattr(left, field)
            right_value = getattr(right, field)
            if left_value != right_value:
                diff[field] = {"before": left_value, "after": right_value}
        return diff

    @staticmethod
    def _diff_workflow(left: WorkflowTemplateVersion, right: WorkflowTemplateVersion) -> Dict[str, Any]:
        diff: Dict[str, Any] = {}
        for field in ["dag", "role_map", "gates", "metrics"]:
            left_value = getattr(left, field)
            right_value = getattr(right, field)
            if left_value != right_value:
                diff[field] = {"before": left_value, "after": right_value}
        return diff

    @staticmethod
    def _policy_rationale(observed: Dict[str, Any], diagnosis: ImprovementDiagnosisReport) -> str:
        return (
            "Observed multi-agent traces suggest policy tuning can improve success rate, review quality, and safety. "
            f"success_rate={observed['success_rate']} approval_rate={observed['approval_rate']} "
            f"context_budget_hit_rate={observed['context_budget_hit_rate']} blockers={', '.join(diagnosis.top_blockers[:2]) or 'none'}."
        )

    @staticmethod
    def _workflow_rationale(observed: Dict[str, Any], diagnosis: ImprovementDiagnosisReport) -> str:
        return (
            "Observed multi-agent traces suggest workflow gating, handoff routing, and repair flow need adjustment. "
            f"failure_rate={observed['failure_rate']} recovery_rate={observed['recovery_rate']} "
            f"approval_rate={observed['approval_rate']} blockers={', '.join(diagnosis.top_blockers[:2]) or 'none'}."
        )

    @staticmethod
    def _workflow_dag_with_recovery(dag: Dict[str, Any]) -> Dict[str, Any]:
        nodes = list(dag.get("nodes", []))
        edges = list(dag.get("edges", []))
        node_keys = {node.get("key") for node in nodes}
        if "recovery" not in node_keys:
            nodes.append(
                {
                    "key": "recovery",
                    "label": "Recovery Triage",
                    "kind": "recovery",
                    "role": "recovery",
                }
            )
        if not any(edge.get("source") == "execute" and edge.get("target") == "recovery" for edge in edges):
            edges.append({"source": "execute", "target": "recovery", "kind": "on_failure"})
        if not any(edge.get("source") == "recovery" and edge.get("target") == "review" for edge in edges):
            edges.append({"source": "recovery", "target": "review", "kind": "handoff"})
        return {"nodes": nodes, "edges": edges}

    def _cluster_descriptors(self, run: ResearchRun) -> List[Dict[str, Any]]:
        mission_phase = str(run.result.get("mission_phase", {}).get("phase") or "unknown")
        handoffs = run.result.get("handoffs", [])
        review_verdicts = run.result.get("review_verdicts", [])
        latest_handoff = handoffs[-1] if handoffs else {}
        latest_review = review_verdicts[-1] if review_verdicts else {}
        tool_name = ""
        if run.execution_trace and run.execution_trace.tool_calls:
            tool_name = run.execution_trace.tool_calls[-1].tool_name
        policy_decision = ""
        if run.execution_trace and run.execution_trace.policy_verdicts:
            policy_decision = run.execution_trace.policy_verdicts[-1].decision
        sandbox_outcomes = self._sandbox_outcomes(run)
        descriptors: List[Dict[str, Any]] = []

        if handoffs and run.status in {"failed", "queued", "running", "recovering"}:
            descriptors.append(
                self._descriptor(
                    signature_type="handoff_breakdown",
                    mission_phase=mission_phase,
                    roles=[latest_handoff.get("from_role"), latest_handoff.get("to_role")],
                    handoff_pairs=[self._handoff_pair(latest_handoff)],
                    review_decisions=[latest_review.get("decision")],
                    tool_names=[tool_name],
                    policy_decisions=[policy_decision],
                    sandbox_outcomes=sandbox_outcomes,
                    task_node_ids=[latest_handoff.get("task_node_id")],
                    summary=f"Handoff chain stalled or failed during {mission_phase}.",
                )
            )
        if any(verdict.get("decision") == "request_repair" for verdict in review_verdicts):
            repair_verdicts = [verdict for verdict in review_verdicts if verdict.get("decision") == "request_repair"]
            descriptors.append(
                self._descriptor(
                    signature_type="review_reject_loop",
                    mission_phase=mission_phase,
                    roles=[verdict.get("role") for verdict in repair_verdicts],
                    handoff_pairs=[self._handoff_pair(packet) for packet in handoffs[-2:]],
                    review_decisions=[verdict.get("decision") for verdict in repair_verdicts],
                    tool_names=[tool_name],
                    policy_decisions=[policy_decision],
                    sandbox_outcomes=sandbox_outcomes,
                    task_node_ids=[verdict.get("task_node_id") for verdict in repair_verdicts],
                    summary="Reviewer requested repair, indicating the mission loop is rejecting output.",
                )
            )
        if run.execution_trace and run.execution_trace.recovery_events and run.status in {"failed", "recovering", "queued"}:
            descriptors.append(
                self._descriptor(
                    signature_type="repair_path_failure",
                    mission_phase=mission_phase,
                    roles=[latest_handoff.get("to_role"), "recovery"],
                    handoff_pairs=[self._handoff_pair(latest_handoff)],
                    review_decisions=[latest_review.get("decision")],
                    tool_names=[tool_name],
                    policy_decisions=[policy_decision],
                    sandbox_outcomes=sandbox_outcomes,
                    task_node_ids=[latest_handoff.get("task_node_id")],
                    summary=compact_text(run.execution_trace.recovery_events[-1].summary, 180),
                )
            )
        approvals = self.database.list_approvals(run_id=run.run_id)
        if approvals and (sandbox_outcomes or run.status == "awaiting_approval"):
            descriptors.append(
                self._descriptor(
                    signature_type="approval_sandbox_friction",
                    mission_phase=mission_phase,
                    roles=[latest_handoff.get("to_role"), latest_review.get("role")],
                    handoff_pairs=[self._handoff_pair(latest_handoff)],
                    review_decisions=[latest_review.get("decision")],
                    tool_names=[tool_name],
                    policy_decisions=[policy_decision],
                    sandbox_outcomes=sandbox_outcomes or [approvals[-1].status],
                    task_node_ids=[latest_handoff.get("task_node_id")],
                    summary="Approval or sandbox friction is slowing down or blocking high-risk execution.",
                )
            )
        if run.status in {"queued", "running"} and handoffs and not self._has_active_execution_evidence(run):
            descriptors.append(
                self._descriptor(
                    signature_type="role_mismatch_or_starvation",
                    mission_phase=mission_phase,
                    roles=[packet.get("to_role") for packet in handoffs[-2:]],
                    handoff_pairs=[self._handoff_pair(packet) for packet in handoffs[-2:]],
                    review_decisions=[latest_review.get("decision")],
                    tool_names=[tool_name],
                    policy_decisions=[policy_decision],
                    sandbox_outcomes=sandbox_outcomes,
                    task_node_ids=[packet.get("task_node_id") for packet in handoffs[-2:]],
                    summary="A role handoff is ready but no compatible worker or follow-up execution arrived.",
                )
            )
        return descriptors

    def _descriptor(
        self,
        *,
        signature_type: str,
        mission_phase: str,
        roles: List[Any],
        handoff_pairs: List[Any],
        review_decisions: List[Any],
        tool_names: List[Any],
        policy_decisions: List[Any],
        sandbox_outcomes: List[Any],
        task_node_ids: List[Any],
        summary: str,
    ) -> Dict[str, Any]:
        role_values = [value for value in roles if value]
        handoff_values = [value for value in handoff_pairs if value]
        review_values = [value for value in review_decisions if value]
        tool_values = [value for value in tool_names if value]
        policy_values = [value for value in policy_decisions if value]
        sandbox_values = [value for value in sandbox_outcomes if value]
        task_values = [value for value in task_node_ids if value]
        signature = (
            f"{signature_type}|phase={mission_phase}|roles={','.join(role_values) or 'none'}|"
            f"handoff={','.join(handoff_values) or 'none'}|review={','.join(review_values) or 'none'}|"
            f"tool={','.join(tool_values) or 'none'}|policy={','.join(policy_values) or 'none'}|"
            f"sandbox={','.join(sandbox_values) or 'none'}"
        )
        return {
            "signature": signature,
            "signature_type": signature_type,
            "roles": role_values,
            "handoff_pairs": handoff_values,
            "review_decisions": review_values,
            "tool_names": tool_values,
            "policy_decisions": policy_values,
            "sandbox_outcomes": sandbox_values,
            "task_node_ids": task_values,
            "summary": compact_text(summary, 180),
        }

    def _auto_evaluate_candidate(self, candidate_id: str, trace_refs: List[str]) -> List[EvaluationReport]:
        reports = [
            self.evaluate_candidate("replay", candidate_id=candidate_id, trace_refs=trace_refs),
            self.evaluate_candidate("benchmark", candidate_id=candidate_id, trace_refs=trace_refs),
        ]
        return reports

    @staticmethod
    def _handoff_pair(packet: Dict[str, Any]) -> Optional[str]:
        from_role = packet.get("from_role")
        to_role = packet.get("to_role")
        if not from_role or not to_role:
            return None
        return f"{from_role}->{to_role}"

    def _sandbox_outcomes(self, run: ResearchRun) -> List[str]:
        outcomes: List[str] = []
        for event in self.database.list_events(run_id=run.run_id, limit=500):
            if event.event_type == "sandbox.failed":
                outcomes.append("failed")
            elif event.event_type == "sandbox.executed":
                outcomes.append("executed")
        return outcomes

    def _has_active_execution_evidence(self, run: ResearchRun) -> bool:
        events = self.database.list_events(run_id=run.run_id, limit=500)
        return any(event.event_type in {"lease.created", "lease.completed", "lease.failed"} for event in events)

    @staticmethod
    def _diagnosis_summary(diagnosis: ImprovementDiagnosisReport) -> Dict[str, Any]:
        return {
            "cluster_count": diagnosis.cluster_count,
            "top_blockers": diagnosis.top_blockers[:3],
            "signature_counts": diagnosis.signature_counts,
        }

    @staticmethod
    def _trace_evidence(diagnosis: ImprovementDiagnosisReport) -> List[Dict[str, Any]]:
        return [
            {
                "cluster_id": cluster.cluster_id,
                "signature_type": cluster.signature_type,
                "sample_run_ids": cluster.sample_run_ids,
                "sample_task_node_ids": cluster.sample_task_node_ids,
            }
            for cluster in diagnosis.clusters[:5]
        ]

    @staticmethod
    def _has_cluster(diagnosis: ImprovementDiagnosisReport, signature_type: str) -> bool:
        return any(cluster.signature_type == signature_type for cluster in diagnosis.clusters)

    @staticmethod
    def _policy_proposal_summary(diagnosis: ImprovementDiagnosisReport) -> str:
        if not diagnosis.clusters:
            return "No strong multi-agent blockers were detected; keeping policy changes conservative."
        dominant = diagnosis.clusters[0]
        return f"Policy proposal targets {dominant.signature_type} using trace-backed safety and repair tuning."

    @staticmethod
    def _workflow_proposal_summary(diagnosis: ImprovementDiagnosisReport) -> str:
        if not diagnosis.clusters:
            return "No strong workflow blocker was detected; keeping DAG changes conservative."
        dominant = diagnosis.clusters[0]
        return f"Workflow proposal targets {dominant.signature_type} by tightening handoff and review routing."

    @staticmethod
    def _policy_tool_policy_adjustments(diagnosis: ImprovementDiagnosisReport) -> Dict[str, Any]:
        if any(cluster.signature_type == "approval_sandbox_friction" for cluster in diagnosis.clusters):
            return {"high_risk_requires_review": True, "prefer_sandboxed_execution": True}
        return {}

    @staticmethod
    def _policy_model_routing_adjustments(diagnosis: ImprovementDiagnosisReport) -> Dict[str, Any]:
        if any(cluster.signature_type in {"handoff_breakdown", "review_reject_loop"} for cluster in diagnosis.clusters):
            return {"reviewer_mode": "reflection_first", "researcher_mode": "context_heavy"}
        return {}

    def _policy_on_failure(
        self,
        observed: Dict[str, Any],
        diagnosis: ImprovementDiagnosisReport,
        baseline: HarnessPolicy,
    ) -> str:
        if self._has_cluster(diagnosis, "review_reject_loop"):
            return "retry_with_research_handoff"
        if self._has_cluster(diagnosis, "repair_path_failure") or observed["failure_rate"] > 0:
            return "retry_with_reflection"
        return str(baseline.repair_policy.get("on_failure", "trace_and_stop"))

    def _workflow_gates_with_diagnosis(
        self,
        gates: List[Dict[str, Any]],
        observed: Dict[str, Any],
        diagnosis: ImprovementDiagnosisReport,
    ) -> List[Dict[str, Any]]:
        updated = list(gates)
        if observed["failure_rate"] > 0 or self._has_cluster(diagnosis, "repair_path_failure"):
            updated.append({"kind": "retry_gate", "max_attempts": 2, "owner": "recovery"})
        if observed["approval_rate"] > 0.3 or self._has_cluster(diagnosis, "approval_sandbox_friction"):
            updated.append({"kind": "review_gate", "owner": "reviewer", "when": "high_risk"})
        if observed["context_budget_hit_rate"] > 0:
            updated.append({"kind": "context_guard", "owner": "planner", "action": "compress_and_retry"})
        if self._has_cluster(diagnosis, "handoff_breakdown"):
            updated.append({"kind": "handoff_guard", "owner": "reviewer", "action": "require_handoff_packet"})
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for gate in updated:
            gate_key = json.dumps(gate, sort_keys=True, ensure_ascii=False)
            if gate_key in seen:
                continue
            seen.add(gate_key)
            deduped.append(gate)
        return deduped

    def _workflow_dag_with_diagnosis(
        self,
        dag: Dict[str, Any],
        diagnosis: ImprovementDiagnosisReport,
    ) -> Dict[str, Any]:
        updated = self._workflow_dag_with_recovery(dag)
        edges = list(updated.get("edges", []))
        if self._has_cluster(diagnosis, "handoff_breakdown"):
            for edge in edges:
                if edge.get("source") == "execute" and edge.get("target") == "review":
                    edge["kind"] = "handoff"
                if edge.get("source") == "recovery" and edge.get("target") == "review":
                    edge["kind"] = "handoff"
        if self._has_cluster(diagnosis, "review_reject_loop") and not any(
            edge.get("source") == "review" and edge.get("target") == "recovery" for edge in edges
        ):
            edges.append({"source": "review", "target": "recovery", "kind": "handoff"})
        updated["edges"] = edges
        return updated
