"""Unit tests for the semantic constraint engine."""

import pytest
from datetime import datetime

from backend.app.harness_lab.constraints import (
    ConstraintParser,
    ConstraintCompiler,
    ConstraintVerifier,
)
from backend.app.harness_lab.constraints.parser import ParsedRule, ParsedCondition
from backend.app.harness_lab.types import (
    ConstraintRule,
    RuleCondition,
    CompiledConstraintSet,
    ConstraintExplanation,
)


class TestConstraintParser:
    """Test the constraint parser."""

    def test_parse_simple_shell_rule(self):
        parser = ConstraintParser()
        rules = parser.parse("Shell commands require approval.", [])
        
        assert len(rules) >= 1
        shell_rules = [r for r in rules if "shell" in r.subject_pattern]
        assert len(shell_rules) >= 1
        assert shell_rules[0].decision == "approval_required"

    def test_parse_read_only_filesystem(self):
        parser = ConstraintParser()
        rules = parser.parse("Read-only filesystem access is allowed.", [])
        
        fs_rules = [r for r in rules if "filesystem" in r.subject_pattern]
        assert len(fs_rules) >= 1
        # Should have at least one allow rule for read operations
        allow_rules = [r for r in fs_rules if r.decision == "allow"]
        assert len(allow_rules) >= 1

    def test_parse_tags_read_only(self):
        parser = ConstraintParser()
        rules = parser.parse("Some body text.", ["read-only"])
        
        # Should have a deny rule for filesystem writes
        deny_rules = [r for r in rules if r.decision == "deny" and "filesystem" in r.subject_pattern]
        assert len(deny_rules) >= 1

    def test_parse_tags_strict_network(self):
        parser = ConstraintParser()
        rules = parser.parse("Some body text.", ["strict-network"])
        
        # Should have approval_required for http_fetch
        http_rules = [r for r in rules if "http_fetch" in r.subject_pattern and r.decision == "approval_required"]
        assert len(http_rules) >= 1

    def test_classify_shell_command(self):
        parser = ConstraintParser()
        
        # Read-only commands
        assert parser.classify_shell_command("ls -la")["read_only"] is True
        assert parser.classify_shell_command("cat file.txt")["read_only"] is True
        assert parser.classify_shell_command("git status")["read_only"] is True
        
        # Destructive commands
        assert parser.classify_shell_command("rm -rf /")["destructive"] is True
        assert parser.classify_shell_command("chmod 777 file")["destructive"] is True
        
        # Mutating commands
        assert parser.classify_shell_command("echo hello > file")["mutating"] is True
        assert parser.classify_shell_command("mv a b")["mutating"] is True


class TestConstraintCompiler:
    """Test the constraint compiler."""

    def test_compile_simple_rule(self):
        compiler = ConstraintCompiler()
        result = compiler.compile_document(
            document_id="test_doc",
            body="Shell commands require approval.",
            tags=[],
        )
        
        assert result.document_id == "test_doc"
        assert result.status in ["success", "partial", "failed"]
        assert result.rules_compiled >= 0

    def test_compile_to_set(self):
        compiler = ConstraintCompiler()
        compiled = compiler.compile_to_set(
            document_id="test_doc",
            body="Read-only access is allowed.",
            tags=["research"],
        )
        
        assert compiled.document_id == "test_doc"
        assert compiled.compilation_status in ["success", "partial", "failed"]
        assert len(compiled.rules) >= 0

    def test_fallback_rules_on_failure(self):
        compiler = ConstraintCompiler()
        # Empty body should trigger fallback
        result = compiler.compile_document(
            document_id="test_doc",
            body="",
            tags=[],
        )
        
        # If compilation fails completely, should use fallback
        if result.status == "failed":
            assert result.used_fallback is True


class TestConstraintVerifier:
    """Test the constraint verifier."""

    def test_verify_shell_command(self):
        verifier = ConstraintVerifier()
        
        # Create a simple compiled set
        rule = ConstraintRule(
            rule_id="test_shell",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="approval_required",
            priority=50,
            reason_template="Shell commands require approval",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        compiled = CompiledConstraintSet(
            compiled_at=datetime.now().isoformat(),
            document_id="doc1",
            document_version="v1",
            rules=[rule],
            compilation_status="success",
        )
        
        verdicts, explanation = verifier.verify(
            compiled_set=compiled,
            subject="tool.shell.execute",
            payload={"command": "ls -la"},
        )
        
        assert len(verdicts) >= 1
        assert explanation.final_decision == "approval_required"

    def test_verify_allow_filesystem_read(self):
        verifier = ConstraintVerifier()
        
        rule = ConstraintRule(
            rule_id="test_fs_read",
            source_document_id="doc1",
            subject_pattern="tool.filesystem.read_file",
            conditions=[],
            decision="allow",
            priority=50,
            reason_template="Read-only access allowed",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        compiled = CompiledConstraintSet(
            compiled_at=datetime.now().isoformat(),
            document_id="doc1",
            document_version="v1",
            rules=[rule],
            compilation_status="success",
        )
        
        verdicts, explanation = verifier.verify(
            compiled_set=compiled,
            subject="tool.filesystem.read_file",
            payload={"path": "/some/path"},
        )
        
        assert explanation.final_decision == "allow"

    def test_deny_before_allow(self):
        """Test that deny takes precedence over allow."""
        verifier = ConstraintVerifier()
        
        deny_rule = ConstraintRule(
            rule_id="deny_rule",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="deny",
            priority=10,  # Higher priority (lower number)
            reason_template="Denied",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        allow_rule = ConstraintRule(
            rule_id="allow_rule",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="allow",
            priority=50,
            reason_template="Allowed",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        compiled = CompiledConstraintSet(
            compiled_at=datetime.now().isoformat(),
            document_id="doc1",
            document_version="v1",
            rules=[deny_rule, allow_rule],
            compilation_status="success",
        )
        
        verdicts, explanation = verifier.verify(
            compiled_set=compiled,
            subject="tool.shell.execute",
            payload={"command": "anything"},
        )
        
        assert explanation.final_decision == "deny"

    def test_condition_matching(self):
        verifier = ConstraintVerifier()
        
        rule = ConstraintRule(
            rule_id="conditional_rule",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[RuleCondition(field="action", operator="eq", value="destructive")],
            decision="deny",
            priority=10,
            reason_template="Destructive commands denied",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        compiled = CompiledConstraintSet(
            compiled_at=datetime.now().isoformat(),
            document_id="doc1",
            document_version="v1",
            rules=[rule],
            compilation_status="success",
        )
        
        # Destructive command should match
        verdicts, explanation = verifier.verify(
            compiled_set=compiled,
            subject="tool.shell.execute",
            payload={"command": "rm -rf /"},
        )
        
        # Should have at least one deny verdict for the matched rule
        deny_verdicts = [v for v in verdicts if v.decision == "deny"]
        assert len(deny_verdicts) >= 1


class TestCompiledConstraintSet:
    """Test the CompiledConstraintSet model."""

    def test_get_rules_for_subject_exact_match(self):
        rule = ConstraintRule(
            rule_id="exact",
            source_document_id="doc1",
            subject_pattern="tool.shell.execute",
            conditions=[],
            decision="allow",
            priority=50,
            reason_template="Test",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        compiled = CompiledConstraintSet(
            compiled_at=datetime.now().isoformat(),
            document_id="doc1",
            document_version="v1",
            rules=[rule],
        )
        
        matches = compiled.get_rules_for_subject("tool.shell.execute")
        assert len(matches) == 1
        assert matches[0].rule_id == "exact"

    def test_get_rules_for_subject_wildcard(self):
        rule = ConstraintRule(
            rule_id="wildcard",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="allow",
            priority=50,
            reason_template="Test",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        compiled = CompiledConstraintSet(
            compiled_at=datetime.now().isoformat(),
            document_id="doc1",
            document_version="v1",
            rules=[rule],
        )
        
        matches = compiled.get_rules_for_subject("tool.shell.execute")
        assert len(matches) == 1
        assert matches[0].rule_id == "wildcard"

    def test_get_rules_sorted_by_priority(self):
        rule_high = ConstraintRule(
            rule_id="high_priority",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="deny",
            priority=10,
            reason_template="High",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        rule_low = ConstraintRule(
            rule_id="low_priority",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="allow",
            priority=50,
            reason_template="Low",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        compiled = CompiledConstraintSet(
            compiled_at=datetime.now().isoformat(),
            document_id="doc1",
            document_version="v1",
            rules=[rule_low, rule_high],  # Inserted in reverse order
        )
        
        matches = compiled.get_rules_for_subject("tool.shell.execute")
        assert len(matches) == 2
        assert matches[0].priority == 10  # Should be sorted by priority
        assert matches[1].priority == 50


class TestConstraintRule:
    """Test the ConstraintRule model."""

    def test_render_reason_with_context(self):
        rule = ConstraintRule(
            rule_id="test",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="approval_required",
            priority=50,
            reason_template="{tool_name} commands require approval",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        
        reason = rule.render_reason({"tool_name": "Shell"})
        assert "Shell commands require approval" in reason

    def test_render_reason_missing_key(self):
        rule = ConstraintRule(
            rule_id="test",
            source_document_id="doc1",
            subject_pattern="tool.shell.*",
            conditions=[],
            decision="approval_required",
            priority=50,
            reason_template="{missing} commands",
            tags=[],
            created_at=datetime.now().isoformat(),
        )
        
        # Should not crash on missing key
        reason = rule.render_reason({})
        assert "{missing}" in reason  # Returns original template
