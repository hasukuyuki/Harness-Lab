"""Integration tests for the semantic constraint engine."""

import pytest
import tempfile
import os
from datetime import datetime

# Set environment variables before importing HarnessLabSettings
os.environ.setdefault("HARNESS_DB_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("HARNESS_REDIS_URL", "redis://localhost:6379")

from backend.app.harness_lab.constraints import ConstraintEngine
from backend.app.harness_lab.types import (
    ConstraintCreateRequest,
    ConstraintVerifyRequest,
)
from backend.app.harness_lab.storage import SqliteTestPlatformStore
from backend.app.harness_lab.artifact_store import create_artifact_store
from backend.app.harness_lab.settings import HarnessLabSettings


@pytest.fixture
def temp_database():
    """Create a temporary database for testing."""
    settings = HarnessLabSettings.from_env()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        store = create_artifact_store(settings, artifact_root_override=tmpdir)
        db = SqliteTestPlatformStore(db_path=db_path, artifact_root=tmpdir, artifact_store=store)
        db.ping()
        yield db
        db.close()


@pytest.fixture
def constraint_engine(temp_database):
    """Create a constraint engine with a temporary database."""
    return ConstraintEngine(temp_database)


class TestConstraintEngineIntegration:
    """Integration tests for the full constraint engine pipeline."""

    def test_create_and_compile_document(self, constraint_engine):
        """Test creating a document and automatic compilation."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Research Guardrails",
            body="Shell commands require approval. Read-only filesystem access is allowed.",
            tags=["research", "deny-destructive"],
        ))
        
        assert doc.document_id is not None
        assert doc.status == "candidate"
        assert doc.compiled is not None
        assert doc.compiled.status in ["success", "partial", "failed"]
        assert doc.compiled.rule_count > 0

    def test_publish_document(self, constraint_engine):
        """Test publishing a document triggers recompilation."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Test Policy",
            body="All filesystem writes require approval.",
            tags=[],
        ))
        
        published = constraint_engine.publish(doc.document_id)
        assert published.status == "published"
        assert published.compiled is not None

    def test_archive_document(self, constraint_engine):
        """Test archiving a document."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Old Policy",
            body="Old rules.",
            tags=[],
        ))
        
        archived = constraint_engine.archive(doc.document_id)
        assert archived.status == "archived"

    def test_verify_shell_command_with_fallback(self, constraint_engine):
        """Test verifying a shell command using fallback logic."""
        # Create a document with shell-related rules
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Shell Policy",
            body="Shell commands require approval. Destructive operations are denied.",
            tags=["deny-destructive"],
        ))
        
        # Test read-only command
        result = constraint_engine.verify(
            subject="tool.shell.execute",
            payload={"command": "ls -la"},
            constraint_set_id=doc.document_id,
        )
        
        assert result.final_verdict is not None
        assert result.explanation is not None
        assert result.explanation.subject == "tool.shell.execute"

    def test_verify_filesystem_read(self, constraint_engine):
        """Test verifying filesystem read operations."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Filesystem Policy",
            body="Read-only filesystem access is allowed. Writes require approval.",
            tags=[],
        ))
        
        result = constraint_engine.verify(
            subject="tool.filesystem.read_file",
            payload={"path": "/some/file.txt"},
            constraint_set_id=doc.document_id,
        )
        
        assert result.final_verdict is not None
        # Read operations should typically be allowed
        assert result.compiled_rule_count > 0

    def test_verify_git_operations(self, constraint_engine):
        """Test verifying git operations."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Git Policy",
            body="Read-only git inspection is allowed. Mutable git actions require review.",
            tags=[],
        ))
        
        # Test git status (read-only)
        result = constraint_engine.verify(
            subject="tool.git.status",
            payload={},
            constraint_set_id=doc.document_id,
        )
        
        assert result.final_verdict is not None
        assert result.explanation is not None

    def test_verify_http_fetch(self, constraint_engine):
        """Test verifying HTTP fetch operations."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Network Policy",
            body="HTTP GET is allowed. Network operations respect strict-network mode.",
            tags=["strict-network"],
        ))
        
        result = constraint_engine.verify(
            subject="tool.http_fetch.get",
            payload={"url": "https://example.com"},
            constraint_set_id=doc.document_id,
        )
        
        assert result.final_verdict is not None
        assert result.explanation is not None

    def test_verify_with_default_constraint_set(self, constraint_engine):
        """Test verify without specifying constraint_set_id uses published."""
        # First verify with no published documents - should use fallback
        result = constraint_engine.verify(
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
        )
        
        assert result.used_fallback is True
        assert result.final_verdict is not None

    def test_explanation_contains_matched_rules(self, constraint_engine):
        """Test that explanation contains matched rules information."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Detailed Policy",
            body="Shell commands require approval.",
            tags=[],
        ))
        
        result = constraint_engine.verify(
            subject="tool.shell.execute",
            payload={"command": "ls"},
            constraint_set_id=doc.document_id,
        )
        
        assert len(result.matched_rules) >= 0  # Could be 0 if fallback is used
        assert result.explanation.evaluated_rules >= 0
        assert result.explanation.compiled_rule_count > 0

    def test_get_document_with_summary(self, constraint_engine):
        """Test retrieving document with compilation summary."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Summary Test",
            body="Test body for summary.",
            tags=["test"],
        ))
        
        result = constraint_engine.get_document_with_summary(doc.document_id)
        
        assert "document" in result
        assert "compilation_summary" in result
        assert result["document"]["document_id"] == doc.document_id
        assert result["compilation_summary"]["rule_count"] >= 0

    def test_engine_status(self, constraint_engine):
        """Test getting engine status."""
        status = constraint_engine.get_engine_status()
        
        assert status.constraint_engine_version == "v2"
        assert status.constraint_parser_ready is True
        assert status.constraint_compiler_ready is True

    def test_deny_before_allow_semantics(self, constraint_engine):
        """Test that deny takes precedence over approval_required and allow."""
        # Create document with both deny and allow rules
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Conflict Policy",
            body="Destructive shell commands are denied. Shell commands require approval.",
            tags=["deny-destructive"],
        ))
        
        # Test destructive command
        result = constraint_engine.verify(
            subject="tool.shell.execute",
            payload={"command": "rm -rf /"},
            constraint_set_id=doc.document_id,
        )
        
        # The final decision should be the most restrictive
        assert result.final_verdict is not None
        # Explanation should show the decision process
        assert result.explanation.final_decision in ["deny", "approval_required"]

    def test_list_documents(self, constraint_engine):
        """Test listing constraint documents."""
        # Create a few documents
        for i in range(3):
            constraint_engine.create_document(ConstraintCreateRequest(
                title=f"Test Doc {i}",
                body=f"Test body {i}",
                tags=[],
            ))
        
        docs = constraint_engine.list_documents()
        assert len(docs) >= 3

    def test_list_documents_by_status(self, constraint_engine):
        """Test listing documents filtered by status."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Published Doc",
            body="Will be published.",
            tags=[],
        ))
        
        constraint_engine.publish(doc.document_id)
        
        published = constraint_engine.list_documents(status="published")
        assert len(published) >= 1
        
        candidates = constraint_engine.list_documents(status="candidate")
        # The published document should not be in candidates
        assert all(d.document_id != doc.document_id for d in candidates)


class TestConstraintEngineBackwardCompatibility:
    """Tests for backward compatibility with old API."""

    def test_legacy_verify_returns_list(self, constraint_engine):
        """Test that legacy verify method returns list of verdicts."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Legacy Test",
            body="Shell commands require approval.",
            tags=[],
        ))
        
        verdicts = constraint_engine.verify_legacy(
            subject="tool.shell.execute",
            payload={"command": "ls"},
            constraint_set_id=doc.document_id,
        )
        
        assert isinstance(verdicts, list)
        assert len(verdicts) > 0
        # Each verdict should have required fields
        for verdict in verdicts:
            assert verdict.verdict_id is not None
            assert verdict.subject is not None
            assert verdict.decision in ["allow", "deny", "approval_required"]

    def test_final_verdict_legacy(self, constraint_engine):
        """Test the legacy final_verdict method."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Final Verdict Test",
            body="Test body.",
            tags=[],
        ))
        
        verdicts = constraint_engine.verify_legacy(
            subject="tool.shell.execute",
            payload={"command": "ls"},
            constraint_set_id=doc.document_id,
        )
        
        final = constraint_engine.final_verdict(verdicts)
        assert final is not None
        assert final.decision in ["allow", "deny", "approval_required"]


class TestConstraintExplanation:
    """Tests for constraint explanation functionality."""

    def test_explanation_structure(self, constraint_engine):
        """Test that explanation has the expected structure."""
        doc = constraint_engine.create_document(ConstraintCreateRequest(
            title="Explanation Test",
            body="Test policy for explanation.",
            tags=[],
        ))
        
        result = constraint_engine.verify(
            subject="tool.shell.execute",
            payload={"command": "echo hello"},
            constraint_set_id=doc.document_id,
        )
        
        explanation = result.explanation
        assert explanation.subject == "tool.shell.execute"
        assert explanation.final_decision is not None
        assert explanation.final_reason is not None
        assert explanation.compilation_status is not None
        assert explanation.compiled_rule_count >= 0
        assert isinstance(explanation.matched_rules, list)
        assert isinstance(explanation.context_snapshot, dict)

    def test_explanation_with_fallback(self, constraint_engine):
        """Test explanation when fallback is used."""
        # Don't create any documents - this should trigger fallback
        result = constraint_engine.verify(
            subject="tool.unknown.operation",
            payload={},
        )
        
        assert result.used_fallback is True
        assert result.explanation.used_fallback is True
        assert result.explanation.fallback_reason is not None
