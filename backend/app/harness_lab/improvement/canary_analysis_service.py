"""Online Canary Analysis Service for continuous rollout observation.

This service provides:
- Long-term canary metrics collection from historical runs
- Automated cohort analysis (baseline vs canary)
- Promotion/hold/rollback recommendation generation
- Key bucket coverage analysis (handoff, review, approval_sandbox, role_dispatch)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ..types import (
    AnalyzeRolloutResponse,
    BucketMetrics,
    CanaryMetrics,
    CohortSummary,
    ImprovementCandidate,
    RecommendationType,
    RolloutRecommendation,
)
from ..utils import utc_now


class CanaryAnalysisService:
    """Service for continuous canary rollout analysis and recommendation.
    
    This service operates independently of manual start/promote operations,
    continuously analyzing real production runs to provide:
    - Up-to-date canary metrics
    - Promotion recommendations with structured reasoning
    - Cohort-specific failure analysis
    """

    # Minimum sample sizes
    MIN_CANARY_SAMPLE_SIZE = 10
    MIN_BASELINE_SAMPLE_SIZE = 5
    
    # Regression thresholds
    SUCCESS_REGRESSION_THRESHOLD = -0.10  # 10% worse
    SAFETY_REGRESSION_THRESHOLD = -0.05   # 5% worse
    RECOVERY_REGRESSION_THRESHOLD = -0.10  # 10% worse
    APPROVAL_REGRESSION_THRESHOLD = -0.10  # 10% more approvals needed
    REPAIR_REGRESSION_THRESHOLD = -0.10    # 10% more repairs needed
    
    # Key buckets that must have coverage for promotion
    KEY_BUCKETS = ["handoff", "review", "approval_sandbox", "role_dispatch"]
    
    # Minimum runs per key bucket for sufficient coverage
    MIN_BUCKET_COVERAGE = 3

    def __init__(self, database) -> None:
        self.database = database

    async def analyze_rollout(
        self,
        candidate: ImprovementCandidate,
        analysis_window_hours: Optional[int] = None,
    ) -> AnalyzeRolloutResponse:
        """Analyze canary rollout and generate recommendation.
        
        This is the main entry point for online canary analysis. It:
        1. Queries historical runs for baseline and canary cohorts
        2. Calculates comprehensive metrics
        3. Generates promotion recommendation with reasoning
        
        Args:
            candidate: The candidate in canary status
            analysis_window_hours: Optional time window for analysis
            
        Returns:
            AnalyzeRolloutResponse with metrics, recommendation, and cohort summary
        """
        analyzed_at = utc_now()
        
        # Fetch runs for both cohorts
        baseline_runs = await self._fetch_cohort_runs(
            candidate.target_id,
            candidate.baseline_version_id,
            "baseline",
            analysis_window_hours,
        )
        canary_runs = await self._fetch_cohort_runs(
            candidate.target_id,
            candidate.target_version_id,
            "canary",
            analysis_window_hours,
        )
        
        # Calculate comprehensive metrics
        canary_metrics = self._calculate_canary_metrics(
            baseline_runs=baseline_runs,
            canary_runs=canary_runs,
        )
        canary_metrics.calculated_at = analyzed_at
        
        # Generate cohort summaries
        cohort_summary = self._generate_cohort_summaries(
            baseline_runs=baseline_runs,
            canary_runs=canary_runs,
        )
        
        # Generate recommendation
        recommendation = self._generate_recommendation(
            candidate=candidate,
            metrics=canary_metrics,
            baseline_runs=baseline_runs,
            canary_runs=canary_runs,
        )
        
        # Get recent failing runs for context
        recent_failing_runs = self._get_recent_failing_runs(canary_runs, limit=5)
        
        return AnalyzeRolloutResponse(
            candidate_id=candidate.candidate_id,
            analyzed_at=analyzed_at,
            canary_metrics=canary_metrics,
            recommendation=recommendation,
            cohort_summary=cohort_summary,
            recent_failing_runs=recent_failing_runs,
        )

    def _generate_recommendation(
        self,
        candidate: ImprovementCandidate,
        metrics: CanaryMetrics,
        baseline_runs: List[Dict[str, Any]],
        canary_runs: List[Dict[str, Any]],
    ) -> RolloutRecommendation:
        """Generate promotion recommendation based on metrics.
        
        Rules:
        - ROLLBACK: Safety regression, approval_sandbox regression, or role_dispatch regression
        - HOLD: Insufficient sample, key bucket gaps, or unstable metrics
        - PROMOTE: Sufficient samples, key buckets covered, no significant regression
        """
        generated_at = utc_now()
        blockers: List[str] = []
        failing_cohorts: List[str] = []
        bucket_coverage_gaps: List[str] = []
        metric_deltas: Dict[str, float] = {}
        
        # Check sample sufficiency
        if not metrics.sufficient_sample:
            blockers.append(
                f"Insufficient canary sample size: {metrics.canary_sample_size} "
                f"(minimum {self.MIN_CANARY_SAMPLE_SIZE})"
            )
        
        if metrics.baseline_sample_size < self.MIN_BASELINE_SAMPLE_SIZE:
            blockers.append(
                f"Insufficient baseline sample size: {metrics.baseline_sample_size} "
                f"(minimum {self.MIN_BASELINE_SAMPLE_SIZE})"
            )
        
        # Collect metric deltas
        metric_deltas = {
            "success": round(metrics.success_delta, 4),
            "safety": round(metrics.safety_delta, 4),
            "recovery": round(metrics.recovery_delta, 4),
            "approval": round(metrics.approval_delta, 4),
            "repair": round(metrics.repair_delta, 4),
        }
        
        # Check for critical regressions -> ROLLBACK
        if metrics.safety_delta < self.SAFETY_REGRESSION_THRESHOLD:
            blockers.append(
                f"Safety regression detected: {metrics.safety_delta:+.1%} "
                f"(threshold: {self.SAFETY_REGRESSION_THRESHOLD:+.1%})"
            )
            failing_cohorts.append("safety")
        
        # Check bucket-level regressions
        for bucket_metric in metrics.bucket_metrics:
            if bucket_metric.regression_detected:
                blockers.append(
                    f"{bucket_metric.bucket_name} regression: "
                    f"{bucket_metric.pass_rate_delta:+.1%} pass rate change"
                )
                failing_cohorts.append(bucket_metric.bucket_name)
        
        # Check key bucket coverage
        covered_buckets = {bm.bucket_name for bm in metrics.bucket_metrics}
        for key_bucket in self.KEY_BUCKETS:
            bucket_metric = next(
                (bm for bm in metrics.bucket_metrics if bm.bucket_name == key_bucket),
                None
            )
            if bucket_metric is None:
                bucket_coverage_gaps.append(f"{key_bucket}: no data")
            elif bucket_metric.canary_count < self.MIN_BUCKET_COVERAGE:
                bucket_coverage_gaps.append(
                    f"{key_bucket}: only {bucket_metric.canary_count} runs "
                    f"(minimum {self.MIN_BUCKET_COVERAGE})"
                )
        
        # Determine recommendation
        requires_operator_review = False
        
        # Critical regressions -> ROLLBACK
        if failing_cohorts and any(
            fc in ["safety", "approval_sandbox", "role_dispatch"]
            for fc in failing_cohorts
        ):
            recommendation = RecommendationType.ROLLBACK
            reason_summary = (
                f"Critical regression detected in: {', '.join(failing_cohorts)}. "
                f"Safety delta: {metrics.safety_delta:+.1%}. "
                f"Recommend immediate rollback to baseline."
            )
            requires_operator_review = True
            
        # Insufficient data or coverage gaps -> HOLD
        elif not metrics.sufficient_sample or bucket_coverage_gaps:
            recommendation = RecommendationType.HOLD
            gaps_str = "; ".join(bucket_coverage_gaps) if bucket_coverage_gaps else "None"
            reason_summary = (
                f"Insufficient data for promotion decision. "
                f"Canary samples: {metrics.canary_sample_size}, "
                f"coverage gaps: {gaps_str}. "
                f"Continue monitoring."
            )
            
        # Success rate regression but not critical -> HOLD
        elif metrics.success_delta < self.SUCCESS_REGRESSION_THRESHOLD:
            recommendation = RecommendationType.HOLD
            blockers.append(
                f"Success rate regression: {metrics.success_delta:+.1%} "
                f"(threshold: {self.SUCCESS_REGRESSION_THRESHOLD:+.1%})"
            )
            reason_summary = (
                f"Success rate regression detected ({metrics.success_delta:+.1%}). "
                f"Not critical enough for rollback, but requires investigation."
            )
            requires_operator_review = True
            
        # All checks pass -> PROMOTE
        else:
            recommendation = RecommendationType.PROMOTE
            reason_summary = (
                f"Canary rollout successful. "
                f"Samples: {metrics.canary_sample_size} canary / {metrics.baseline_sample_size} baseline. "
                f"Success delta: {metrics.success_delta:+.1%}, "
                f"Safety delta: {metrics.safety_delta:+.1%}. "
                f"Ready for promotion."
            )
            requires_operator_review = candidate.kind == "workflow" and not candidate.approved
        
        # Get representative failing runs/clusters
        representative_runs = self._get_representative_failing_runs(canary_runs)
        representative_clusters = self._get_representative_clusters(canary_runs)
        
        return RolloutRecommendation(
            recommendation=recommendation,
            confidence=self._calculate_confidence(metrics, blockers),
            reason_summary=reason_summary,
            blockers=blockers,
            metric_deltas=metric_deltas,
            failing_cohorts=failing_cohorts,
            representative_runs=representative_runs,
            representative_clusters=representative_clusters,
            bucket_coverage_gaps=[gap.split(":")[0] for gap in bucket_coverage_gaps],
            requires_operator_review=requires_operator_review,
            generated_at=generated_at,
        )

    def _calculate_canary_metrics(
        self,
        baseline_runs: List[Dict[str, Any]],
        canary_runs: List[Dict[str, Any]],
    ) -> CanaryMetrics:
        """Calculate comprehensive canary metrics from runs."""
        # Aggregate baseline metrics
        baseline_agg = self._aggregate_run_metrics(baseline_runs)
        canary_agg = self._aggregate_run_metrics(canary_runs)
        
        # Calculate deltas
        success_delta = canary_agg["success_rate"] - baseline_agg["success_rate"]
        safety_delta = canary_agg["safety_score"] - baseline_agg["safety_score"]
        recovery_delta = canary_agg["recovery_rate"] - baseline_agg["recovery_rate"]
        approval_delta = canary_agg["approval_rate"] - baseline_agg["approval_rate"]
        repair_delta = canary_agg["repair_rate"] - baseline_agg["repair_rate"]
        
        # Check for regression
        regression_detected = (
            success_delta < self.SUCCESS_REGRESSION_THRESHOLD or
            safety_delta < self.SAFETY_REGRESSION_THRESHOLD or
            recovery_delta < self.RECOVERY_REGRESSION_THRESHOLD
        )
        
        # Collect regression buckets
        regression_buckets = []
        if safety_delta < self.SAFETY_REGRESSION_THRESHOLD:
            regression_buckets.append("safety")
        if success_delta < self.SUCCESS_REGRESSION_THRESHOLD:
            regression_buckets.append("success")
        if recovery_delta < self.RECOVERY_REGRESSION_THRESHOLD:
            regression_buckets.append("recovery")
        
        # Calculate bucket metrics
        bucket_metrics = self._calculate_bucket_metrics(baseline_runs, canary_runs)
        for bm in bucket_metrics:
            if bm.regression_detected and bm.bucket_name not in regression_buckets:
                regression_buckets.append(bm.bucket_name)
        
        # Check sufficiency
        sufficient_sample = (
            len(canary_runs) >= self.MIN_CANARY_SAMPLE_SIZE and
            len(baseline_runs) >= self.MIN_BASELINE_SAMPLE_SIZE
        )
        
        # Top blockers from canary runs
        top_blockers = self._extract_top_blockers(canary_runs)
        
        return CanaryMetrics(
            baseline_sample_size=len(baseline_runs),
            canary_sample_size=len(canary_runs),
            baseline_success_rate=round(baseline_agg["success_rate"], 4),
            canary_success_rate=round(canary_agg["success_rate"], 4),
            baseline_safety_score=round(baseline_agg["safety_score"], 4),
            canary_safety_score=round(canary_agg["safety_score"], 4),
            baseline_recovery_rate=round(baseline_agg["recovery_rate"], 4),
            canary_recovery_rate=round(canary_agg["recovery_rate"], 4),
            baseline_approval_rate=round(baseline_agg["approval_rate"], 4),
            canary_approval_rate=round(canary_agg["approval_rate"], 4),
            baseline_repair_rate=round(baseline_agg["repair_rate"], 4),
            canary_repair_rate=round(canary_agg["repair_rate"], 4),
            success_delta=round(success_delta, 4),
            safety_delta=round(safety_delta, 4),
            recovery_delta=round(recovery_delta, 4),
            approval_delta=round(approval_delta, 4),
            repair_delta=round(repair_delta, 4),
            bucket_metrics=bucket_metrics,
            regression_detected=regression_detected,
            regression_buckets=regression_buckets,
            sufficient_sample=sufficient_sample,
            minimum_sample_reached=len(canary_runs) >= self.MIN_CANARY_SAMPLE_SIZE,
            top_blockers=top_blockers,
            failing_cohorts=regression_buckets,
        )

    def _aggregate_run_metrics(self, runs: List[Dict[str, Any]]) -> Dict[str, float]:
        """Aggregate metrics from a list of runs."""
        if not runs:
            return {
                "success_rate": 0.0,
                "safety_score": 0.0,
                "recovery_rate": 0.0,
                "approval_rate": 0.0,
                "repair_rate": 0.0,
            }
        
        total = len(runs)
        completed = sum(1 for r in runs if r.get("status") == "completed")
        success_rate = completed / total
        
        # Safety score from policy verdicts
        total_denied = 0
        total_approval_required = 0
        total_verdicts = 0
        
        for run in runs:
            trace = run.get("execution_trace", {})
            verdicts = trace.get("policy_verdicts", [])
            for verdict in verdicts:
                total_verdicts += 1
                if verdict.get("decision") == "deny":
                    total_denied += 1
                elif verdict.get("decision") == "approval_required":
                    total_approval_required += 1
        
        # Safety score: fewer denials/approvals = higher safety
        if total_verdicts > 0:
            safety_score = max(0.0, 1.0 - ((total_denied + total_approval_required * 0.5) / total_verdicts))
        else:
            safety_score = 1.0
        
        # Recovery rate
        recoveries = sum(
            1 for r in runs
            if r.get("execution_trace", {}).get("recovery_events")
        )
        recovery_rate = recoveries / total
        
        # Approval rate (how often approval was needed)
        needs_approval = sum(
            1 for r in runs
            if any(
                v.get("decision") == "approval_required"
                for v in r.get("execution_trace", {}).get("policy_verdicts", [])
            )
        )
        approval_rate = needs_approval / total
        
        # Repair rate (from review decisions)
        needs_repair = sum(
            1 for r in runs
            if any(
                rd.get("decision") == "request_repair"
                for rd in r.get("execution_trace", {}).get("review_decisions", [])
            )
        )
        repair_rate = needs_repair / total
        
        return {
            "success_rate": round(success_rate, 4),
            "safety_score": round(safety_score, 4),
            "recovery_rate": round(recovery_rate, 4),
            "approval_rate": round(approval_rate, 4),
            "repair_rate": round(repair_rate, 4),
        }

    def _calculate_bucket_metrics(
        self,
        baseline_runs: List[Dict[str, Any]],
        canary_runs: List[Dict[str, Any]],
    ) -> List[BucketMetrics]:
        """Calculate per-bucket metrics for key buckets."""
        bucket_metrics = []
        
        for bucket_name in self.KEY_BUCKETS:
            baseline_bucket = self._filter_runs_by_bucket(baseline_runs, bucket_name)
            canary_bucket = self._filter_runs_by_bucket(canary_runs, bucket_name)
            
            baseline_count = len(baseline_bucket)
            canary_count = len(canary_bucket)
            
            baseline_passed = sum(1 for r in baseline_bucket if r.get("status") == "completed")
            canary_passed = sum(1 for r in canary_bucket if r.get("status") == "completed")
            
            baseline_failed = baseline_count - baseline_passed
            canary_failed = canary_count - canary_passed
            
            # Calculate pass rate delta
            baseline_pass_rate = baseline_passed / baseline_count if baseline_count > 0 else 0.0
            canary_pass_rate = canary_passed / canary_count if canary_count > 0 else 0.0
            pass_rate_delta = canary_pass_rate - baseline_pass_rate
            
            # Detect regression (10% worse pass rate)
            regression_detected = pass_rate_delta < -0.10 and canary_count >= 3
            
            bucket_metrics.append(BucketMetrics(
                bucket_name=bucket_name,
                baseline_count=baseline_count,
                canary_count=canary_count,
                baseline_passed=baseline_passed,
                canary_passed=canary_passed,
                baseline_failed=baseline_failed,
                canary_failed=canary_failed,
                pass_rate_delta=round(pass_rate_delta, 4),
                regression_detected=regression_detected,
            ))
        
        return bucket_metrics

    def _filter_runs_by_bucket(
        self,
        runs: List[Dict[str, Any]],
        bucket_name: str,
    ) -> List[Dict[str, Any]]:
        """Filter runs that involve a specific bucket/behavior."""
        filtered = []
        
        for run in runs:
            trace = run.get("execution_trace", {})
            
            # Check various signals for bucket involvement
            if bucket_name == "handoff":
                # Has handoff packets
                if trace.get("handoff_packets"):
                    filtered.append(run)
                    
            elif bucket_name == "review":
                # Has review decisions
                if trace.get("review_decisions"):
                    filtered.append(run)
                    
            elif bucket_name == "approval_sandbox":
                # Has approval-required verdicts or sandbox executions
                verdicts = trace.get("policy_verdicts", [])
                if any(v.get("decision") == "approval_required" for v in verdicts):
                    filtered.append(run)
                elif trace.get("sandbox_traces"):
                    filtered.append(run)
                    
            elif bucket_name == "role_dispatch":
                # Has multiple role transitions
                task_graph = run.get("task_graph", {})
                nodes = task_graph.get("nodes", [])
                roles = {n.get("agent_role") for n in nodes if n.get("agent_role")}
                if len(roles) > 1:
                    filtered.append(run)
        
        return filtered

    def _generate_cohort_summaries(
        self,
        baseline_runs: List[Dict[str, Any]],
        canary_runs: List[Dict[str, Any]],
    ) -> List[CohortSummary]:
        """Generate summary for each cohort."""
        summaries = []
        
        for cohort, runs in [("baseline", baseline_runs), ("canary", canary_runs)]:
            if not runs:
                continue
                
            success_count = sum(1 for r in runs if r.get("status") == "completed")
            failure_count = len(runs) - success_count
            success_rate = success_count / len(runs) if runs else 0.0
            
            # Recent runs (last 10)
            recent_runs = sorted(
                runs,
                key=lambda r: r.get("created_at", ""),
                reverse=True
            )[:10]
            
            # Top blockers
            top_blockers = self._extract_top_blockers(runs)
            
            summaries.append(CohortSummary(
                cohort=cohort,
                sample_size=len(runs),
                success_count=success_count,
                failure_count=failure_count,
                success_rate=round(success_rate, 4),
                recent_runs=[r.get("run_id") for r in recent_runs],
                top_blockers=top_blockers,
            ))
        
        return summaries

    def _extract_top_blockers(self, runs: List[Dict[str, Any]], limit: int = 5) -> List[str]:
        """Extract top failure reasons from runs."""
        blocker_counts: Dict[str, int] = {}
        
        for run in runs:
            if run.get("status") != "completed":
                # Extract failure reason from result or trace
                result = run.get("result", {})
                error = result.get("error") or result.get("failure_reason")
                if error:
                    blocker_counts[error] = blocker_counts.get(error, 0) + 1
                else:
                    trace = run.get("execution_trace", {})
                    failures = trace.get("failures", [])
                    for f in failures:
                        reason = f.get("reason") or f.get("message") or "Unknown failure"
                        blocker_counts[reason] = blocker_counts.get(reason, 0) + 1
        
        # Sort by count and return top N
        sorted_blockers = sorted(blocker_counts.items(), key=lambda x: -x[1])
        return [reason for reason, _ in sorted_blockers[:limit]]

    def _get_representative_failing_runs(
        self,
        canary_runs: List[Dict[str, Any]],
        limit: int = 3,
    ) -> List[str]:
        """Get representative failing run IDs for investigation."""
        failing_runs = [
            r for r in canary_runs
            if r.get("status") != "completed"
        ]
        
        # Sort by recency and diversity of failure reasons
        sorted_runs = sorted(
            failing_runs,
            key=lambda r: r.get("created_at", ""),
            reverse=True
        )
        
        return [r.get("run_id") for r in sorted_runs[:limit] if r.get("run_id")]

    def _get_representative_clusters(
        self,
        canary_runs: List[Dict[str, Any]],
        limit: int = 3,
    ) -> List[str]:
        """Get representative failure cluster IDs."""
        # Collect cluster references from failing runs
        cluster_refs: Dict[str, int] = {}
        
        for run in canary_runs:
            if run.get("status") != "completed":
                trace = run.get("execution_trace", {})
                clusters = trace.get("failure_clusters", [])
                for cluster in clusters:
                    cluster_id = cluster.get("cluster_id") if isinstance(cluster, dict) else cluster
                    if cluster_id:
                        cluster_refs[cluster_id] = cluster_refs.get(cluster_id, 0) + 1
        
        # Sort by frequency
        sorted_clusters = sorted(cluster_refs.items(), key=lambda x: -x[1])
        return [cid for cid, _ in sorted_clusters[:limit]]

    def _get_recent_failing_runs(
        self,
        canary_runs: List[Dict[str, Any]],
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Get recent failing runs with key details."""
        failing_runs = [
            {
                "run_id": r.get("run_id"),
                "status": r.get("status"),
                "created_at": r.get("created_at"),
                "error": r.get("result", {}).get("error"),
                "failure_reason": r.get("result", {}).get("failure_reason"),
            }
            for r in canary_runs
            if r.get("status") != "completed"
        ]
        
        # Sort by recency
        sorted_runs = sorted(
            failing_runs,
            key=lambda r: r.get("created_at", ""),
            reverse=True
        )
        
        return sorted_runs[:limit]

    def _calculate_confidence(
        self,
        metrics: CanaryMetrics,
        blockers: List[str],
    ) -> float:
        """Calculate confidence level for the recommendation."""
        base_confidence = 0.5
        
        # Increase confidence with sample size
        if metrics.canary_sample_size >= 50:
            base_confidence += 0.2
        elif metrics.canary_sample_size >= 20:
            base_confidence += 0.1
        elif metrics.canary_sample_size >= 10:
            base_confidence += 0.05
        
        # Decrease confidence with more blockers
        base_confidence -= len(blockers) * 0.1
        
        # Decrease confidence with regressions
        if metrics.regression_detected:
            base_confidence -= 0.15
        
        # Clamp to [0, 1]
        return max(0.0, min(1.0, base_confidence))

    async def _fetch_cohort_runs(
        self,
        target_id: str,
        version_id: Optional[str],
        cohort: str,
        analysis_window_hours: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch runs for a specific cohort from database.
        
        This queries runs with rollout_info matching the cohort.
        """
        # This is a placeholder - actual implementation depends on database schema
        # The database should support querying by rollout_info.cohort and version
        filters = {
            "target_id": target_id,
            "rollout_info.cohort": cohort,
        }
        if version_id:
            filters["rollout_info.target_version_id"] = version_id
        
        # Call database method (to be implemented based on actual schema)
        return await self.database.query_runs_with_rollout_info(
            filters=filters,
            time_window_hours=analysis_window_hours,
        )
