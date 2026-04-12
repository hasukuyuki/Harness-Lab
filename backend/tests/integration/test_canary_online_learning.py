"""Integration tests for Online Canary Learning & Controlled Promotion.

These tests verify the complete canary rollout flow:
- candidate -> canary -> run accumulation -> analyze-rollout -> promote/rollback
"""

import json
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from backend.app.harness_lab.improvement.canary_analysis_service import CanaryAnalysisService
from backend.app.harness_lab.types import (
    AnalyzeRolloutResponse,
    CanaryMetrics,
    CanaryScope,
    CohortRunsResponse,
    ImprovementCandidate,
    RecommendationType,
    ResearchRun,
    RolloutInfo,
    RolloutRecommendation,
)


class TestCanaryOnlineLearningFlow:
    """Integration tests for the complete canary online learning flow."""

    @pytest.fixture
    def mock_database(self):
        """Create a mock database with run storage."""
        db = MagicMock()
        db.runs = []  # In-memory run storage
        
        def mock_fetchall(query, params=None, conn=None):
            # Simple query parser for test
            if "FROM runs" in query:
                return [
                    {"payload_json": json.dumps(run)}
                    for run in db.runs
                ]
            return []
        
        def mock_query_runs_with_rollout_info(filters, time_window_hours=None):
            """Mock implementation of cohort run querying."""
            cohort = filters.get("rollout_info.cohort")
            version_id = filters.get("rollout_info.target_version_id")
            
            matching = []
            for run in db.runs:
                run_data = run if isinstance(run, dict) else json.loads(run)
                rollout_info = run_data.get("rollout_info", {})
                
                if cohort and rollout_info.get("cohort") != cohort:
                    continue
                if version_id and rollout_info.get("target_version_id") != version_id:
                    continue
                    
                matching.append(run_data)
            
            return matching
        
        db.fetchall.side_effect = mock_fetchall
        db.query_runs_with_rollout_info = AsyncMock(side_effect=mock_query_runs_with_rollout_info)
        
        return db

    @pytest.fixture
    def analysis_service(self, mock_database):
        """Create a canary analysis service with mock database."""
        return CanaryAnalysisService(mock_database)

    @pytest.fixture
    def base_candidate(self):
        """Create a base candidate in canary status."""
        return ImprovementCandidate(
            candidate_id="candidate_123",
            kind="policy",
            target_id="policy_abc",
            target_version_id="policy_v2",
            baseline_version_id="policy_v1",
            change_set={"tool_policy": {"updated": True}},
            rationale="Test candidate for canary",
            eval_status="passed",
            publish_status="canary",
            rollout_ring="candidate",
            rollout_scope=CanaryScope(
                scope_type="percentage",
                scope_value="50",
                description="50% rollout",
            ),
            approved=True,
            requires_human_approval=False,
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )

    def create_run(self, run_id, status, cohort, version_id, bucket_type="handoff", **kwargs):
        """Helper to create a run dict with rollout info."""
        # Add bucket-specific trace data for key bucket coverage
        execution_trace = kwargs.get("execution_trace", {})
        task_graph = None
        
        if bucket_type == "handoff":
            execution_trace["handoff_packets"] = [{}]
        elif bucket_type == "review":
            execution_trace["review_decisions"] = [{}]
        elif bucket_type == "approval_sandbox":
            execution_trace["policy_verdicts"] = [{"decision": "approval_required"}]
        elif bucket_type == "role_dispatch":
            # task_graph needs to be at run level, not in execution_trace
            task_graph = {
                "nodes": [
                    {"agent_role": "planner"},
                    {"agent_role": "executor"},
                ]
            }
        
        run_data = {
            "run_id": run_id,
            "session_id": f"session_{run_id}",
            "status": status,
            "policy_id": version_id,
            "rollout_info": {
                "candidate_id": "candidate_123",
                "target_version_id": version_id,
                "rollout_ring": "candidate",
                "cohort": cohort,
                "matched_scope": {"type": "percentage", "value": "50"},
                "rollout_reason": "percentage_match",
                "recorded_at": "2024-01-01T00:00:00Z",
            },
            "execution_trace": execution_trace,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }
        
        if task_graph:
            run_data["task_graph"] = task_graph
            
        return run_data

    @pytest.mark.asyncio
    async def test_analyze_rollout_with_sufficient_samples_recommends_promote(
        self,
        mock_database,
        analysis_service,
        base_candidate,
    ):
        """Test that sufficient samples with good metrics results in PROMOTE."""
        import random
        random.seed(42)
        
        # Create baseline runs (using baseline version) - distributed across buckets
        baseline_runs = []
        buckets = ["handoff", "review", "approval_sandbox", "role_dispatch"]
        for i in range(10):
            bucket = buckets[i % 4]
            baseline_runs.append(
                self.create_run(f"baseline_{i}", "completed", "baseline", "policy_v1", bucket_type=bucket)
            )
        # Add some failures to baseline
        for i in range(2):
            bucket = buckets[i % 4]
            baseline_runs.append(
                self.create_run(f"baseline_fail_{i}", "failed", "baseline", "policy_v1", bucket_type=bucket)
            )
        
        # Create canary runs (using canary version) - slightly better success rate, distributed across buckets
        canary_runs = []
        for i in range(12):
            bucket = buckets[i % 4]
            canary_runs.append(
                self.create_run(f"canary_{i}", "completed", "canary", "policy_v2", bucket_type=bucket)
            )
        for i in range(1):
            bucket = buckets[i % 4]
            canary_runs.append(
                self.create_run(f"canary_fail_{i}", "failed", "canary", "policy_v2", bucket_type=bucket)
            )
        
        mock_database.runs = baseline_runs + canary_runs
        
        response = await analysis_service.analyze_rollout(base_candidate)
        
        assert isinstance(response, AnalyzeRolloutResponse)
        assert response.candidate_id == base_candidate.candidate_id
        assert response.canary_metrics.canary_sample_size == 13
        assert response.canary_metrics.baseline_sample_size == 12
        
        # Should recommend promote since canary is performing better and has bucket coverage
        assert response.recommendation.recommendation == RecommendationType.PROMOTE
        assert response.recommendation.confidence > 0.5

    @pytest.mark.asyncio
    async def test_analyze_rollout_with_regression_recommends_rollback(
        self,
        mock_database,
        analysis_service,
        base_candidate,
    ):
        """Test that significant regression results in ROLLBACK."""
        # Baseline: mostly successful
        baseline_runs = [
            self.create_run(f"baseline_{i}", "completed", "baseline", "policy_v1")
            for i in range(10)
        ]
        
        # Canary: many failures (high regression)
        canary_runs = [
            self.create_run(f"canary_{i}", "completed", "canary", "policy_v2")
            for i in range(3)
        ]
        canary_runs.extend([
            self.create_run(
                f"canary_fail_{i}",
                "failed",
                "canary",
                "policy_v2",
                execution_trace={
                    "policy_verdicts": [{"decision": "deny"}],
                }
            )
            for i in range(10)
        ])
        
        mock_database.runs = baseline_runs + canary_runs
        
        response = await analysis_service.analyze_rollout(base_candidate)
        
        assert response.recommendation.recommendation == RecommendationType.ROLLBACK
        assert response.recommendation.requires_operator_review
        assert any("safety" in b.lower() for b in response.recommendation.failing_cohorts)

    @pytest.mark.asyncio
    async def test_analyze_rollout_with_insufficient_samples_recommends_hold(
        self,
        mock_database,
        analysis_service,
        base_candidate,
    ):
        """Test that insufficient samples results in HOLD."""
        # Only a few canary runs
        baseline_runs = [
            self.create_run(f"baseline_{i}", "completed", "baseline", "policy_v1")
            for i in range(10)
        ]
        canary_runs = [
            self.create_run(f"canary_{i}", "completed", "canary", "policy_v2")
            for i in range(3)  # Less than MIN_CANARY_SAMPLE_SIZE (10)
        ]
        
        mock_database.runs = baseline_runs + canary_runs
        
        response = await analysis_service.analyze_rollout(base_candidate)
        
        assert response.recommendation.recommendation == RecommendationType.HOLD
        assert any("insufficient" in b.lower() for b in response.recommendation.blockers)

    @pytest.mark.asyncio
    async def test_cohort_summary_generation(self, mock_database, analysis_service, base_candidate):
        """Test that cohort summaries are correctly generated."""
        baseline_runs = [
            self.create_run(f"baseline_{i}", "completed", "baseline", "policy_v1")
            for i in range(8)
        ]
        baseline_runs.extend([
            self.create_run(f"baseline_fail_{i}", "failed", "baseline", "policy_v1")
            for i in range(2)
        ])
        
        canary_runs = [
            self.create_run(f"canary_{i}", "completed", "canary", "policy_v2")
            for i in range(12)
        ]
        canary_runs.extend([
            self.create_run(f"canary_fail_{i}", "failed", "canary", "policy_v2")
            for i in range(3)
        ])
        
        mock_database.runs = baseline_runs + canary_runs
        
        response = await analysis_service.analyze_rollout(base_candidate)
        
        assert len(response.cohort_summary) == 2
        
        baseline_summary = next(s for s in response.cohort_summary if s.cohort == "baseline")
        assert baseline_summary.sample_size == 10
        assert baseline_summary.success_count == 8
        assert baseline_summary.failure_count == 2
        assert baseline_summary.success_rate == 0.8
        
        canary_summary = next(s for s in response.cohort_summary if s.cohort == "canary")
        assert canary_summary.sample_size == 15
        assert canary_summary.success_count == 12
        assert canary_summary.failure_count == 3
        assert canary_summary.success_rate == 0.8

    @pytest.mark.asyncio
    async def test_recent_failing_runs_in_response(
        self,
        mock_database,
        analysis_service,
        base_candidate,
    ):
        """Test that recent failing runs are included in the response."""
        canary_runs = [
            self.create_run(
                f"canary_fail_{i}",
                "failed",
                "canary",
                "policy_v2",
            )
            for i in range(5)
        ]
        
        # Add some successful runs too
        canary_runs.extend([
            self.create_run(f"canary_ok_{i}", "completed", "canary", "policy_v2")
            for i in range(10)
        ])
        
        mock_database.runs = canary_runs
        
        response = await analysis_service.analyze_rollout(base_candidate)
        
        # Should include recent failing runs
        assert len(response.recent_failing_runs) > 0
        # All entries should be failed status
        assert all(r["status"] != "completed" for r in response.recent_failing_runs)

    def test_rollout_info_structure(self):
        """Test that rollout info has the correct structure."""
        rollout_info = RolloutInfo(
            candidate_id="candidate_123",
            target_version_id="policy_v2",
            rollout_ring="candidate",
            cohort="canary",
            matched_scope={
                "type": "percentage",
                "value": "50",
                "description": "50% rollout",
            },
            rollout_reason="percentage_match",
            recorded_at="2024-01-01T00:00:00Z",
        )
        
        assert rollout_info.candidate_id == "candidate_123"
        assert rollout_info.cohort == "canary"
        assert rollout_info.rollout_reason == "percentage_match"
        assert rollout_info.matched_scope["type"] == "percentage"

    def test_recommendation_structure(self):
        """Test that recommendation has the correct structure with all fields."""
        from backend.app.harness_lab.utils import utc_now
        
        recommendation = RolloutRecommendation(
            recommendation=RecommendationType.HOLD,
            confidence=0.75,
            reason_summary="Insufficient data for promotion decision.",
            blockers=["Sample size too small", "Missing bucket coverage"],
            metric_deltas={
                "success": -0.05,
                "safety": 0.02,
            },
            failing_cohorts=["approval_sandbox"],
            representative_runs=["run_1", "run_2"],
            representative_clusters=["cluster_abc"],
            bucket_coverage_gaps=["role_dispatch"],
            requires_operator_review=True,
            generated_at=utc_now(),
        )
        
        assert recommendation.recommendation == RecommendationType.HOLD
        assert recommendation.confidence == 0.75
        assert len(recommendation.blockers) == 2
        assert "approval_sandbox" in recommendation.failing_cohorts
        assert recommendation.requires_operator_review


class TestCanaryCohortFiltering:
    """Tests for cohort filtering in run details/replay/failure clusters."""

    def test_run_cohort_determination_from_rollout_info(self):
        """Test that run cohort is correctly determined from rollout_info."""
        run_with_canary = ResearchRun(
            run_id="run_1",
            session_id="session_1",
            status="completed",
            rollout_info=RolloutInfo(
                candidate_id="candidate_123",
                cohort="canary",
            ),
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        
        assert run_with_canary.rollout_info.cohort == "canary"
        
        run_with_baseline = ResearchRun(
            run_id="run_2",
            session_id="session_2",
            status="completed",
            rollout_info=RolloutInfo(
                candidate_id="candidate_123",
                cohort="baseline",
            ),
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        
        assert run_with_baseline.rollout_info.cohort == "baseline"

    def test_legacy_cohort_field_backward_compatibility(self):
        """Test that legacy cohort field still works."""
        run = ResearchRun(
            run_id="run_1",
            session_id="session_1",
            status="completed",
            cohort="canary",  # Legacy field
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        
        # Should be able to read legacy cohort
        assert run.cohort == "canary"

    def test_rollout_reason_tracking(self):
        """Test that rollout reason is tracked for observability."""
        reasons = [
            "percentage_match",
            "session_tag_match",
            "worker_label_match",
            "goal_pattern_match",
            "explicit_override",
        ]
        
        for reason in reasons:
            rollout_info = RolloutInfo(
                candidate_id="candidate_123",
                cohort="canary",
                rollout_reason=reason,
            )
            assert rollout_info.rollout_reason == reason


class TestControlledPromotionGovernance:
    """Tests for controlled promotion governance (operator in the loop)."""

    def test_promote_recommendation_does_not_auto_execute(self):
        """Test that promote recommendation requires manual action."""
        # The recommendation system only produces recommendations,
        # actual promotion must be triggered by operator
        recommendation = RolloutRecommendation(
            recommendation=RecommendationType.PROMOTE,
            confidence=0.9,
            reason_summary="Canary successful, ready for promotion",
            generated_at="2024-01-01T00:00:00Z",
        )
        
        # Recommendation exists but system doesn't auto-promote
        assert recommendation.recommendation == RecommendationType.PROMOTE
        # In real system, operator would call promote API

    def test_rollback_recommendation_alerts_operator(self):
        """Test that rollback recommendation alerts operator."""
        recommendation = RolloutRecommendation(
            recommendation=RecommendationType.ROLLBACK,
            confidence=0.85,
            reason_summary="Safety regression detected",
            blockers=["Safety regression: -8%"],
            failing_cohorts=["safety"],
            requires_operator_review=True,
            generated_at="2024-01-01T00:00:00Z",
        )
        
        assert recommendation.recommendation == RecommendationType.ROLLBACK
        assert recommendation.requires_operator_review
        assert len(recommendation.blockers) > 0

    def test_hold_recommendation_with_blockers(self):
        """Test that hold recommendation includes actionable blockers."""
        recommendation = RolloutRecommendation(
            recommendation=RecommendationType.HOLD,
            confidence=0.6,
            reason_summary="Insufficient data",
            blockers=[
                "Insufficient canary sample size: 5 (minimum 10)",
                "Missing coverage: approval_sandbox",
            ],
            bucket_coverage_gaps=["approval_sandbox", "role_dispatch"],
            generated_at="2024-01-01T00:00:00Z",
        )
        
        assert recommendation.recommendation == RecommendationType.HOLD
        assert len(recommendation.blockers) == 2
        assert "approval_sandbox" in recommendation.bucket_coverage_gaps
