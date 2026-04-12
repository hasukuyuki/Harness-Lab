"""Canary Rollout Service for safe promotion governance."""

from __future__ import annotations

import random
import re
from typing import Any, Dict, List, Optional, Tuple

from ..types import (
    CanaryMetrics,
    CanaryScope,
    ImprovementCandidate,
    PublishGateStatus,
    RolloutSnapshot,
)
from ..utils import utc_now


class CanaryRolloutService:
    """Service for managing canary rollouts of policies and workflows.
    
    This service provides:
    - Canary scope matching (session tag, worker label, goal pattern, percentage)
    - Canary metrics collection and comparison
    - Promote/rollback decision support
    - Rollout cohort tracking
    """

    # Minimum sample size before promotion is considered
    MIN_CANARY_SAMPLE_SIZE = 10
    
    # Safety thresholds
    MAX_SUCCESS_REGRESSION = -0.1  # 10% worse success rate
    MAX_SAFETY_REGRESSION = -0.05  # 5% worse safety score

    def __init__(self, database) -> None:
        self.database = database

    def canary_matches(
        self,
        scope: CanaryScope,
        session: Dict[str, Any],
        worker: Optional[Dict[str, Any]] = None,
        explicit_override: Optional[str] = None,
    ) -> bool:
        """Check if a session/worker matches the canary scope.
        
        Args:
            scope: The canary scope definition
            session: Session data including tags, goal, etc.
            worker: Optional worker data including labels
            explicit_override: Explicit canary override from request
            
        Returns:
            True if this request should use the canary version
        """
        scope_type = scope.scope_type
        scope_value = scope.scope_value

        if scope_type == "explicit_override":
            # Check if explicit override matches candidate ID
            return explicit_override == scope_value

        if scope_type == "percentage":
            # Use session_id for deterministic percentage rollout
            session_id = session.get("session_id", "")
            # Hash the session_id to get a number between 0-99
            hash_val = hash(session_id) % 100
            percentage = int(scope_value)
            return hash_val < percentage

        if scope_type == "session_tag":
            # Check if session has the specified tag
            tags = session.get("tags", [])
            if isinstance(tags, list):
                return scope_value in tags
            return scope_value == tags

        if scope_type == "worker_label":
            # Check if worker has the specified label
            if worker is None:
                return False
            labels = worker.get("labels", [])
            if isinstance(labels, list):
                return scope_value in labels
            return scope_value == labels

        if scope_type == "goal_pattern":
            # Check if session goal matches the pattern
            goal = session.get("goal", "")
            try:
                return bool(re.search(scope_value, goal, re.IGNORECASE))
            except re.error:
                # If invalid regex, do simple substring match
                return scope_value.lower() in goal.lower()

        return False

    def calculate_canary_metrics(
        self,
        candidate_id: str,
        baseline_runs: List[Dict[str, Any]],
        canary_runs: List[Dict[str, Any]],
    ) -> CanaryMetrics:
        """Calculate canary metrics comparing baseline vs canary runs.
        
        Args:
            candidate_id: The candidate being evaluated
            baseline_runs: Runs using the baseline version
            canary_runs: Runs using the canary version
            
        Returns:
            CanaryMetrics with deltas and readiness assessment
        """
        baseline_metrics = self._aggregate_run_metrics(baseline_runs)
        canary_metrics = self._aggregate_run_metrics(canary_runs)

        success_delta = canary_metrics["success_rate"] - baseline_metrics["success_rate"]
        safety_delta = canary_metrics["safety_score"] - baseline_metrics["safety_score"]
        recovery_delta = canary_metrics["recovery_rate"] - baseline_metrics["recovery_rate"]

        # Check for regressions
        regression_detected = (
            success_delta < self.MAX_SUCCESS_REGRESSION or
            safety_delta < self.MAX_SAFETY_REGRESSION
        )

        # Check for sufficient sample size
        sufficient_sample = len(canary_runs) >= self.MIN_CANARY_SAMPLE_SIZE

        return CanaryMetrics(
            baseline_sample_size=len(baseline_runs),
            canary_sample_size=len(canary_runs),
            baseline_success_rate=baseline_metrics["success_rate"],
            canary_success_rate=canary_metrics["success_rate"],
            baseline_safety_score=baseline_metrics["safety_score"],
            canary_safety_score=canary_metrics["safety_score"],
            baseline_recovery_rate=baseline_metrics["recovery_rate"],
            canary_recovery_rate=canary_metrics["recovery_rate"],
            success_delta=round(success_delta, 3),
            safety_delta=round(safety_delta, 3),
            recovery_delta=round(recovery_delta, 3),
            regression_detected=regression_detected,
            sufficient_sample=sufficient_sample,
        )

    def check_promote_readiness(
        self,
        candidate: ImprovementCandidate,
    ) -> Tuple[bool, List[str]]:
        """Check if a canary candidate is ready to be promoted.
        
        Args:
            candidate: The candidate to check
            
        Returns:
            Tuple of (is_ready, list_of_blockers)
        """
        blockers: List[str] = []

        if candidate.publish_status != "canary":
            blockers.append("Candidate is not in canary status")
            return False, blockers

        if candidate.canary_metrics is None:
            blockers.append("No canary metrics available")
            return False, blockers

        metrics = candidate.canary_metrics

        # Check sample size
        if not metrics.sufficient_sample:
            blockers.append(
                f"Insufficient canary sample size: {metrics.canary_sample_size} "
                f"(minimum {self.MIN_CANARY_SAMPLE_SIZE})"
            )

        # Check for regressions
        if metrics.regression_detected:
            if metrics.success_delta < self.MAX_SUCCESS_REGRESSION:
                blockers.append(
                    f"Success rate regression detected: {metrics.success_delta:+.1%}"
                )
            if metrics.safety_delta < self.MAX_SAFETY_REGRESSION:
                blockers.append(
                    f"Safety score regression detected: {metrics.safety_delta:+.1%}"
                )

        # Workflow candidates require explicit approval
        if candidate.kind == "workflow" and not candidate.approved:
            blockers.append("Workflow candidate requires human approval")

        return len(blockers) == 0, blockers

    def create_rollout_snapshot(
        self,
        candidate: ImprovementCandidate,
    ) -> RolloutSnapshot:
        """Create a snapshot of the current rollout state.
        
        This is stored when rolling back to understand the state at rollback time.
        """
        return RolloutSnapshot(
            ring=candidate.rollout_ring or "unknown",
            scope=candidate.rollout_scope,
            baseline_version_id=candidate.baseline_version_id,
            canary_metrics=candidate.canary_metrics,
            started_at=candidate.rollout_started_at,
            ended_at=utc_now(),
        )

    def get_default_canary_scope(self, candidate_kind: str) -> CanaryScope:
        """Get the default canary scope for a candidate type.
        
        Policy candidates: 10% rollout by default
        Workflow candidates: explicit override only (safer)
        """
        if candidate_kind == "policy":
            return CanaryScope(
                scope_type="percentage",
                scope_value="10",
                description="10% percentage rollout",
            )
        else:  # workflow
            return CanaryScope(
                scope_type="explicit_override",
                scope_value="manual",
                description="Explicit manual override only",
            )

    def _aggregate_run_metrics(self, runs: List[Dict[str, Any]]) -> Dict[str, float]:
        """Aggregate metrics from a list of runs."""
        if not runs:
            return {
                "success_rate": 0.0,
                "safety_score": 0.0,
                "recovery_rate": 0.0,
            }

        completed = sum(1 for r in runs if r.get("status") == "completed")
        success_rate = completed / len(runs)

        # Calculate safety score from policy verdicts
        total_denied = 0
        total_approval_required = 0
        for run in runs:
            trace = run.get("execution_trace", {})
            verdicts = trace.get("policy_verdicts", [])
            for verdict in verdicts:
                if verdict.get("decision") == "deny":
                    total_denied += 1
                elif verdict.get("decision") == "approval_required":
                    total_approval_required += 1

        # Safety score: 1.0 means no denials/approvals needed
        total_verdicts = total_denied + total_approval_required
        safety_score = max(0.0, 1.0 - (total_verdicts * 0.1))

        # Recovery rate from recovery events
        recoveries = sum(
            1 for r in runs
            if r.get("execution_trace", {}).get("recovery_events")
        )
        recovery_rate = recoveries / len(runs)

        return {
            "success_rate": round(success_rate, 3),
            "safety_score": round(safety_score, 3),
            "recovery_rate": round(recovery_rate, 3),
        }

    def filter_runs_by_cohort(
        self,
        runs: List[Dict[str, Any]],
        cohort: str,  # "baseline" or "canary"
    ) -> List[Dict[str, Any]]:
        """Filter runs by rollout cohort.
        
        This allows failure cluster and evaluation to distinguish between
        baseline and canary runs.
        """
        return [
            run for run in runs
            if run.get("rollout_cohort") == cohort or
            (cohort == "baseline" and not run.get("rollout_cohort"))
        ]
