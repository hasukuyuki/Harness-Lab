"""Unit tests for canary rollout service."""

import pytest
from datetime import datetime

from backend.app.harness_lab.improvement.canary_service import CanaryRolloutService
from backend.app.harness_lab.types import (
    CanaryScope,
    CanaryMetrics,
    ImprovementCandidate,
)


class MockDatabase:
    """Mock database for testing."""
    pass


class TestCanaryScopeMatching:
    """Test canary scope matching logic."""

    @pytest.fixture
    def canary_service(self):
        return CanaryRolloutService(MockDatabase())

    def test_percentage_scope(self, canary_service):
        """Test percentage-based rollout matching."""
        scope = CanaryScope(scope_type="percentage", scope_value="50")
        
        # Same session should consistently match or not
        session = {"session_id": "test_session_123"}
        result1 = canary_service.canary_matches(scope, session)
        result2 = canary_service.canary_matches(scope, session)
        assert result1 == result2  # Consistent
        
        # With 50%, roughly half should match (deterministic based on hash)
        matches = sum(
            canary_service.canary_matches(scope, {"session_id": f"session_{i}"})
            for i in range(100)
        )
        assert 40 <= matches <= 60  # Should be around 50

    def test_explicit_override_scope(self, canary_service):
        """Test explicit override matching."""
        scope = CanaryScope(scope_type="explicit_override", scope_value="candidate_123")
        
        session = {"session_id": "any"}
        
        # Should match when explicit override matches
        assert canary_service.canary_matches(scope, session, explicit_override="candidate_123")
        
        # Should not match when different
        assert not canary_service.canary_matches(scope, session, explicit_override="candidate_456")
        assert not canary_service.canary_matches(scope, session, explicit_override=None)

    def test_session_tag_scope(self, canary_service):
        """Test session tag matching."""
        scope = CanaryScope(scope_type="session_tag", scope_value="experimental")
        
        # Should match when tag present
        session = {"session_id": "s1", "tags": ["experimental", "test"]}
        assert canary_service.canary_matches(scope, session)
        
        # Should not match when tag absent
        session = {"session_id": "s2", "tags": ["test"]}
        assert not canary_service.canary_matches(scope, session)
        
        # Should handle string tags
        session = {"session_id": "s3", "tags": "experimental"}
        assert canary_service.canary_matches(scope, session)

    def test_worker_label_scope(self, canary_service):
        """Test worker label matching."""
        scope = CanaryScope(scope_type="worker_label", scope_value="canary-worker")
        
        session = {"session_id": "s1"}
        worker = {"labels": ["canary-worker", "gpu"]}
        
        # Should match when worker has label
        assert canary_service.canary_matches(scope, session, worker)
        
        # Should not match when worker lacks label
        worker_no_label = {"labels": ["gpu"]}
        assert not canary_service.canary_matches(scope, session, worker_no_label)
        
        # Should not match when no worker
        assert not canary_service.canary_matches(scope, session, None)

    def test_goal_pattern_scope(self, canary_service):
        """Test goal pattern matching."""
        scope = CanaryScope(scope_type="goal_pattern", scope_value="refactor.*code")
        
        # Should match when pattern matches
        session = {"session_id": "s1", "goal": "refactor the codebase"}
        assert canary_service.canary_matches(scope, session)
        
        # Should be case insensitive
        session = {"session_id": "s2", "goal": "REFACTOR my CODE"}
        assert canary_service.canary_matches(scope, session)
        
        # Should not match when pattern doesn't match
        session = {"session_id": "s3", "goal": "add new feature"}
        assert not canary_service.canary_matches(scope, session)

    def test_invalid_regex_falls_back_to_substring(self, canary_service):
        """Test that invalid regex falls back to substring match."""
        scope = CanaryScope(scope_type="goal_pattern", scope_value="[invalid")
        
        session = {"session_id": "s1", "goal": "test [invalid pattern"}
        # Should not crash, falls back to substring
        result = canary_service.canary_matches(scope, session)
        assert isinstance(result, bool)


class TestCanaryMetricsCalculation:
    """Test canary metrics calculation."""

    @pytest.fixture
    def canary_service(self):
        return CanaryRolloutService(MockDatabase())

    def test_basic_metrics_calculation(self, canary_service):
        """Test basic metrics calculation."""
        baseline_runs = [
            {"status": "completed", "execution_trace": {}},
            {"status": "completed", "execution_trace": {}},
            {"status": "failed", "execution_trace": {}},
        ]
        canary_runs = [
            {"status": "completed", "execution_trace": {}},
            {"status": "completed", "execution_trace": {}},
        ]
        
        metrics = canary_service.calculate_canary_metrics(
            "candidate_123",
            baseline_runs,
            canary_runs,
        )
        
        assert metrics.baseline_sample_size == 3
        assert metrics.canary_sample_size == 2
        assert abs(metrics.baseline_success_rate - 2/3) < 0.01
        assert metrics.canary_success_rate == 1.0
        assert abs(metrics.success_delta - (1.0 - 2/3)) < 0.01

    def test_empty_runs(self, canary_service):
        """Test metrics with empty run lists."""
        metrics = canary_service.calculate_canary_metrics(
            "candidate_123",
            [],
            [],
        )
        
        assert metrics.baseline_sample_size == 0
        assert metrics.canary_sample_size == 0
        assert metrics.baseline_success_rate == 0.0
        assert metrics.canary_success_rate == 0.0

    def test_regression_detection(self, canary_service):
        """Test regression detection."""
        # Baseline: 100% success
        baseline_runs = [
            {"status": "completed", "execution_trace": {}},
            {"status": "completed", "execution_trace": {}},
        ]
        # Canary: 50% success (regression)
        canary_runs = [
            {"status": "completed", "execution_trace": {}},
            {"status": "failed", "execution_trace": {}},
        ]
        
        metrics = canary_service.calculate_canary_metrics(
            "candidate_123",
            baseline_runs,
            canary_runs,
        )
        
        assert metrics.regression_detected is True
        assert metrics.success_delta < -0.1  # More than 10% regression

    def test_sufficient_sample_check(self, canary_service):
        """Test sufficient sample size check."""
        # Less than minimum sample size
        canary_runs = [{"status": "completed", "execution_trace": {}}] * 5
        
        metrics = canary_service.calculate_canary_metrics(
            "candidate_123",
            [],
            canary_runs,
        )
        
        assert metrics.sufficient_sample is False
        
        # At minimum sample size
        canary_runs = [{"status": "completed", "execution_trace": {}}] * 10
        
        metrics = canary_service.calculate_canary_metrics(
            "candidate_123",
            [],
            canary_runs,
        )
        
        assert metrics.sufficient_sample is True


class TestPromoteReadiness:
    """Test promote readiness checks."""

    @pytest.fixture
    def canary_service(self):
        return CanaryRolloutService(MockDatabase())

    def test_not_in_canary_status(self, canary_service):
        """Test that non-canary candidates cannot be promoted."""
        candidate = ImprovementCandidate(
            candidate_id="c1",
            kind="policy",
            target_id="p1",
            target_version_id="p1_v2",
            rationale="Test candidate",
            publish_status="draft",  # Not canary
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        
        is_ready, blockers = canary_service.check_promote_readiness(candidate)
        
        assert is_ready is False
        assert any("not in canary" in b.lower() for b in blockers)

    def test_no_metrics(self, canary_service):
        """Test that candidates without metrics cannot be promoted."""
        candidate = ImprovementCandidate(
            candidate_id="c1",
            kind="policy",
            target_id="p1",
            target_version_id="p1_v2",
            rationale="Test candidate",
            publish_status="canary",
            rollout_ring="candidate",
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        
        is_ready, blockers = canary_service.check_promote_readiness(candidate)
        
        assert is_ready is False
        assert any("metrics" in b.lower() for b in blockers)

    def test_insufficient_sample(self, canary_service):
        """Test insufficient sample size blocker."""
        candidate = ImprovementCandidate(
            candidate_id="c1",
            kind="policy",
            target_id="p1",
            target_version_id="p1_v2",
            rationale="Test candidate",
            publish_status="canary",
            rollout_ring="candidate",
            canary_metrics=CanaryMetrics(
                canary_sample_size=5,  # Less than minimum
                sufficient_sample=False,
            ),
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        
        is_ready, blockers = canary_service.check_promote_readiness(candidate)
        
        assert is_ready is False
        assert any("sample" in b.lower() for b in blockers)

    def test_regression_blocker(self, canary_service):
        """Test regression detection blocker."""
        candidate = ImprovementCandidate(
            candidate_id="c1",
            kind="policy",
            target_id="p1",
            target_version_id="p1_v2",
            rationale="Test candidate",
            publish_status="canary",
            rollout_ring="candidate",
            canary_metrics=CanaryMetrics(
                canary_sample_size=20,
                sufficient_sample=True,
                regression_detected=True,
                success_delta=-0.15,  # Big regression
            ),
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        
        is_ready, blockers = canary_service.check_promote_readiness(candidate)
        
        assert is_ready is False
        assert any("regression" in b.lower() for b in blockers)

    def test_workflow_requires_approval(self, canary_service):
        """Test that workflow candidates require approval."""
        candidate = ImprovementCandidate(
            candidate_id="c1",
            kind="workflow",  # Workflow
            target_id="w1",
            target_version_id="w1_v2",
            rationale="Test candidate",
            publish_status="canary",
            rollout_ring="candidate",
            approved=False,  # Not approved
            canary_metrics=CanaryMetrics(
                canary_sample_size=20,
                sufficient_sample=True,
                regression_detected=False,
                success_delta=0.05,
            ),
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        
        is_ready, blockers = canary_service.check_promote_readiness(candidate)
        
        assert is_ready is False
        assert any("approval" in b.lower() for b in blockers)

    def test_ready_to_promote(self, canary_service):
        """Test ready candidate."""
        candidate = ImprovementCandidate(
            candidate_id="c1",
            kind="policy",
            target_id="p1",
            target_version_id="p1_v2",
            rationale="Test candidate",
            publish_status="canary",
            rollout_ring="candidate",
            canary_metrics=CanaryMetrics(
                canary_sample_size=20,
                sufficient_sample=True,
                regression_detected=False,
                success_delta=0.05,
            ),
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
        )
        
        is_ready, blockers = canary_service.check_promote_readiness(candidate)
        
        assert is_ready is True
        assert len(blockers) == 0


class TestDefaultCanaryScope:
    """Test default canary scope selection."""

    @pytest.fixture
    def canary_service(self):
        return CanaryRolloutService(MockDatabase())

    def test_policy_default_scope(self, canary_service):
        """Test policy candidates get percentage scope by default."""
        scope = canary_service.get_default_canary_scope("policy")
        
        assert scope.scope_type == "percentage"
        assert scope.scope_value == "10"

    def test_workflow_default_scope(self, canary_service):
        """Test workflow candidates get explicit override scope by default."""
        scope = canary_service.get_default_canary_scope("workflow")
        
        assert scope.scope_type == "explicit_override"
        assert scope.scope_value == "manual"


class TestCohortFiltering:
    """Test cohort filtering for runs."""

    @pytest.fixture
    def canary_service(self):
        return CanaryRolloutService(MockDatabase())

    def test_filter_baseline_runs(self, canary_service):
        """Test filtering baseline cohort runs."""
        runs = [
            {"run_id": "r1", "rollout_cohort": "baseline"},
            {"run_id": "r2", "rollout_cohort": "canary"},
            {"run_id": "r3"},  # No cohort = baseline
            {"run_id": "r4", "rollout_cohort": "baseline"},
        ]
        
        baseline = canary_service.filter_runs_by_cohort(runs, "baseline")
        
        assert len(baseline) == 3
        assert all(r["run_id"] in {"r1", "r3", "r4"} for r in baseline)

    def test_filter_canary_runs(self, canary_service):
        """Test filtering canary cohort runs."""
        runs = [
            {"run_id": "r1", "rollout_cohort": "baseline"},
            {"run_id": "r2", "rollout_cohort": "canary"},
            {"run_id": "r3", "rollout_cohort": "canary"},
        ]
        
        canary = canary_service.filter_runs_by_cohort(runs, "canary")
        
        assert len(canary) == 2
        assert all(r["run_id"] in {"r2", "r3"} for r in canary)
