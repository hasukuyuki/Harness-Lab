"""ConstraintVerifier - Validates tool calls against compiled constraint rules.

The verifier evaluates tool invocations against a CompiledConstraintSet,
producing PolicyVerdicts with detailed explanations of which rules matched
and why particular decisions were made.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from ..types import (
    ConstraintRule,
    RuleCondition,
    CompiledConstraintSet,
    ConstraintMatch,
    MatchedRuleInfo,
    ConstraintExplanation,
    PolicyVerdict,
    VerdictDecision,
)
from ..utils import new_id, utc_now
from .parser import ConstraintParser


@dataclass
class VerificationContext:
    """Runtime context for constraint verification."""
    tool_name: str
    subject: str
    payload: Dict[str, Any]
    # Derived attributes
    action: Optional[str] = None
    path: Optional[str] = None
    command: Optional[str] = None
    network_mode: Optional[str] = None
    sandbox_required: Optional[bool] = None
    git_mutability: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for template rendering."""
        return {
            "tool_name": self.tool_name,
            "subject": self.subject,
            "action": self.action,
            "path": self.path,
            "command": self.command,
            "network_mode": self.network_mode,
            "sandbox_required": self.sandbox_required,
            "git_mutability": self.git_mutability,
        }


class ConstraintVerifier:
    """Verifies tool calls against compiled constraint rules.
    
    The verifier is responsible for:
    1. Matching rules against the verification context
    2. Evaluating conditions for matched rules
    3. Applying deny-before-allow semantics
    4. Generating detailed explanations
    5. Falling back to heuristic classification when needed
    
    The verifier maintains backward compatibility with the original
    ConstraintEngine behavior while providing enhanced visibility.
    """

    def __init__(self) -> None:
        self.parser = ConstraintParser()

    def verify(
        self,
        compiled_set: CompiledConstraintSet,
        subject: str,
        payload: Dict[str, Any],
        runtime_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[PolicyVerdict], ConstraintExplanation]:
        """Verify a tool call against compiled constraints.
        
        Args:
            compiled_set: The compiled constraint rules
            subject: The tool subject (e.g., "tool.shell.execute")
            payload: The tool invocation payload
            runtime_context: Additional runtime context (network mode, etc.)
            
        Returns:
            Tuple of (list of verdicts, detailed explanation)
        """
        # Build verification context
        ctx = self._build_context(subject, payload, runtime_context)
        
        # Find matching rules
        candidate_rules = compiled_set.get_rules_for_subject(subject)
        
        # Evaluate each rule
        matches: List[Tuple[ConstraintRule, bool, List[str]]] = []
        for rule in candidate_rules:
            matched, matched_conditions = self._evaluate_rule(rule, ctx)
            matches.append((rule, matched, matched_conditions))
        
        # Build verdicts from matched rules
        verdicts: List[PolicyVerdict] = []
        matched_rules: List[MatchedRuleInfo] = []
        
        for rule, matched, matched_conditions in matches:
            if matched:
                # Build context for reason template
                template_context = ctx.to_dict()
                template_context["rule_id"] = rule.rule_id
                
                reason = rule.render_reason(template_context)
                
                verdict = PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision=rule.decision,
                    reason=reason,
                    matched_rule=rule.subject_pattern,
                    created_at=utc_now(),
                    rule_id=rule.rule_id,
                    source_document_id=rule.source_document_id,
                    source_document_version=compiled_set.document_version,
                    used_fallback=compiled_set.used_fallback,
                    explanation_summary=f"Rule '{rule.rule_id}' matched with decision '{rule.decision}'",
                )
                verdicts.append(verdict)
                
                matched_rules.append(MatchedRuleInfo(
                    rule_id=rule.rule_id,
                    subject_pattern=rule.subject_pattern,
                    decision=rule.decision,
                    priority=rule.priority,
                    source_document_id=rule.source_document_id,
                    source_document_version=compiled_set.document_version,
                    matched_conditions=matched_conditions,
                    reason=reason,
                ))
        
        # If no rules matched, use fallback heuristic
        if not verdicts:
            fallback_verdicts, fallback_explanation = self._fallback_verification(
                compiled_set, subject, payload, ctx
            )
            return fallback_verdicts, fallback_explanation
        
        # Build explanation
        explanation = ConstraintExplanation(
            subject=subject,
            final_decision=self._calculate_final_decision(verdicts),
            final_reason=self._build_final_reason(verdicts),
            matched_rules=matched_rules,
            evaluated_rules=len(candidate_rules),
            used_fallback=compiled_set.used_fallback,
            fallback_reason=compiled_set.fallback_reason,
            compilation_status=compiled_set.compilation_status,
            compiled_rule_count=len(compiled_set.rules),
            context_snapshot=ctx.to_dict(),
        )
        
        return verdicts, explanation

    def _build_context(
        self,
        subject: str,
        payload: Dict[str, Any],
        runtime_context: Optional[Dict[str, Any]],
    ) -> VerificationContext:
        """Build verification context from subject and payload."""
        # Parse subject to extract tool name
        parts = subject.split(".")
        tool_name = parts[1] if len(parts) > 1 else "unknown"
        
        # Initialize context
        ctx = VerificationContext(
            tool_name=tool_name,
            subject=subject,
            payload=payload,
        )
        
        # Extract tool-specific attributes
        if tool_name == "shell":
            ctx.command = str(payload.get("command", ""))
            ctx.action = self._classify_shell_action(ctx.command)
        
        elif tool_name == "filesystem":
            ctx.path = str(payload.get("path", ""))
            action = payload.get("action", "")
            if action in {"read_file", "list_dir"}:
                ctx.action = "read"
            elif action == "write_file":
                ctx.action = "write"
        
        elif tool_name == "git":
            action = parts[-1] if len(parts) > 2 else ""
            if action in {"status", "diff", "log"}:
                ctx.action = "read"
            else:
                ctx.action = "mutate"
        
        elif tool_name == "http_fetch":
            ctx.action = "network"
            ctx.network_mode = runtime_context.get("network_mode") if runtime_context else None
        
        elif tool_name == "knowledge_search":
            ctx.action = "read"
        
        elif tool_name == "model_reflection":
            ctx.action = "read"
        
        elif tool_name == "mcp_proxy":
            ctx.action = "external"
        
        # Extract sandbox requirement from runtime context
        if runtime_context:
            ctx.sandbox_required = runtime_context.get("sandbox_required")
        
        return ctx

    def _classify_shell_action(self, command: str) -> Optional[str]:
        """Classify a shell command by action type."""
        if not command:
            return None
        
        classification = self.parser.classify_shell_command(command)
        
        if classification["destructive"]:
            return "destructive"
        elif classification["mutating"]:
            return "mutate"
        elif classification["read_only"]:
            return "read"
        
        return None

    def _evaluate_rule(
        self,
        rule: ConstraintRule,
        ctx: VerificationContext,
    ) -> Tuple[bool, List[str]]:
        """Evaluate a rule against the verification context.
        
        Returns:
            Tuple of (whether rule matched, list of matched condition descriptions)
        """
        if not rule.conditions:
            # Rules without conditions always match
            return True, []
        
        matched_conditions: List[str] = []
        
        for condition in rule.conditions:
            if self._evaluate_condition(condition, ctx):
                matched_conditions.append(
                    f"{condition.field} {condition.operator} {condition.value}"
                )
            else:
                # All conditions must match (AND semantics)
                return False, []
        
        return True, matched_conditions

    def _evaluate_condition(
        self,
        condition: RuleCondition,
        ctx: VerificationContext,
    ) -> bool:
        """Evaluate a single condition against the context."""
        # Get the value from context
        ctx_value = getattr(ctx, condition.field, None)
        if ctx_value is None:
            # Try to get from payload
            ctx_value = ctx.payload.get(condition.field)
        
        # Handle None values
        if ctx_value is None:
            return False
        
        # Evaluate based on operator
        op = condition.operator
        cond_value = condition.value
        
        if op == "eq":
            return str(ctx_value).lower() == str(cond_value).lower()
        
        elif op == "ne":
            return str(ctx_value).lower() != str(cond_value).lower()
        
        elif op == "contains":
            return str(cond_value).lower() in str(ctx_value).lower()
        
        elif op == "prefix":
            return str(ctx_value).lower().startswith(str(cond_value).lower())
        
        elif op == "suffix":
            return str(ctx_value).lower().endswith(str(cond_value).lower())
        
        elif op == "regex":
            try:
                return bool(re.search(str(cond_value), str(ctx_value), re.IGNORECASE))
            except re.error:
                return False
        
        elif op == "in":
            if isinstance(cond_value, (list, tuple, set)):
                return str(ctx_value).lower() in [str(v).lower() for v in cond_value]
            return str(ctx_value).lower() in str(cond_value).lower()
        
        elif op == "not_in":
            if isinstance(cond_value, (list, tuple, set)):
                return str(ctx_value).lower() not in [str(v).lower() for v in cond_value]
            return str(ctx_value).lower() not in str(cond_value).lower()
        
        # Unknown operator defaults to False
        return False

    def _fallback_verification(
        self,
        compiled_set: CompiledConstraintSet,
        subject: str,
        payload: Dict[str, Any],
        ctx: VerificationContext,
    ) -> Tuple[List[PolicyVerdict], ConstraintExplanation]:
        """Generate verdicts using fallback heuristic when no rules match.
        
        This maintains backward compatibility with the original ConstraintEngine.
        """
        verdicts: List[PolicyVerdict] = []
        
        # Tool-specific fallback logic
        if ctx.tool_name == "shell":
            verdicts.append(PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject=subject,
                decision="approval_required",
                reason="Shell commands default to review in Harness Lab.",
                matched_rule="tool.shell.*",
                created_at=utc_now(),
                rule_id=None,
                used_fallback=True,
                explanation_summary="No explicit rules matched, using fallback",
                source_document_id=compiled_set.document_id,
                source_document_version=compiled_set.document_version,
            ))
            
            if ctx.action == "read":
                verdicts.append(PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision="allow",
                    reason="Read-only shell commands may run without approval.",
                    matched_rule="tool.shell.read_only",
                    created_at=utc_now(),
                    rule_id=None,
                    used_fallback=True,
                    explanation_summary="Heuristic classification: read-only command",
                    source_document_id=compiled_set.document_id,
                    source_document_version=compiled_set.document_version,
                ))
            elif ctx.action == "mutate":
                verdicts.append(PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision="approval_required",
                    reason="Mutable shell operations require operator approval.",
                    matched_rule="tool.shell.mutable",
                    created_at=utc_now(),
                    rule_id=None,
                    used_fallback=True,
                    explanation_summary="Heuristic classification: mutating command",
                    source_document_id=compiled_set.document_id,
                    source_document_version=compiled_set.document_version,
                ))
            elif ctx.action == "destructive":
                verdicts.append(PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision="deny",
                    reason="Destructive shell patterns are denied by the research guardrails.",
                    matched_rule="tool.shell.destructive",
                    created_at=utc_now(),
                    rule_id=None,
                    used_fallback=True,
                    explanation_summary="Heuristic classification: destructive command",
                    source_document_id=compiled_set.document_id,
                    source_document_version=compiled_set.document_version,
                ))
        
        elif ctx.tool_name == "filesystem":
            if ctx.action == "read":
                verdicts.append(PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision="allow",
                    reason="Read-only filesystem access is allowed.",
                    matched_rule="tool.filesystem.read",
                    created_at=utc_now(),
                    rule_id=None,
                    used_fallback=True,
                    explanation_summary="Fallback: read-only filesystem access",
                    source_document_id=compiled_set.document_id,
                    source_document_version=compiled_set.document_version,
                ))
            elif ctx.action == "write":
                verdicts.append(PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision="approval_required",
                    reason="Filesystem writes require review.",
                    matched_rule="tool.filesystem.write",
                    created_at=utc_now(),
                    rule_id=None,
                    used_fallback=True,
                    explanation_summary="Fallback: filesystem write requires approval",
                    source_document_id=compiled_set.document_id,
                    source_document_version=compiled_set.document_version,
                ))
        
        elif ctx.tool_name == "git":
            if ctx.action == "read":
                verdicts.append(PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision="allow",
                    reason="Read-only git inspection is allowed.",
                    matched_rule="tool.git.read",
                    created_at=utc_now(),
                    rule_id=None,
                    used_fallback=True,
                    explanation_summary="Fallback: read-only git operation",
                    source_document_id=compiled_set.document_id,
                    source_document_version=compiled_set.document_version,
                ))
            else:
                verdicts.append(PolicyVerdict(
                    verdict_id=new_id("verdict"),
                    subject=subject,
                    decision="approval_required",
                    reason="Mutable git actions require review.",
                    matched_rule="tool.git.mutable",
                    created_at=utc_now(),
                    rule_id=None,
                    used_fallback=True,
                    explanation_summary="Fallback: mutable git operation requires approval",
                    source_document_id=compiled_set.document_id,
                    source_document_version=compiled_set.document_version,
                ))
        
        elif ctx.tool_name == "http_fetch":
            verdicts.append(PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject=subject,
                decision="allow",
                reason="HTTP GET is allowed.",
                matched_rule="tool.http_fetch.get",
                created_at=utc_now(),
                rule_id=None,
                used_fallback=True,
                explanation_summary="Fallback: HTTP GET allowed by default",
                source_document_id=compiled_set.document_id,
                source_document_version=compiled_set.document_version,
            ))
        
        elif ctx.tool_name == "knowledge_search":
            verdicts.append(PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject=subject,
                decision="allow",
                reason="Knowledge search is allowed.",
                matched_rule="tool.knowledge_search.query",
                created_at=utc_now(),
                rule_id=None,
                used_fallback=True,
                explanation_summary="Fallback: knowledge search allowed",
                source_document_id=compiled_set.document_id,
                source_document_version=compiled_set.document_version,
            ))
        
        elif ctx.tool_name == "model_reflection":
            verdicts.append(PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject=subject,
                decision="allow",
                reason="Local model reflection is allowed.",
                matched_rule="tool.model_reflection.run",
                created_at=utc_now(),
                rule_id=None,
                used_fallback=True,
                explanation_summary="Fallback: model reflection allowed",
                source_document_id=compiled_set.document_id,
                source_document_version=compiled_set.document_version,
            ))
        
        elif ctx.tool_name == "mcp_proxy":
            verdicts.append(PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject=subject,
                decision="approval_required",
                reason="External proxy calls require review.",
                matched_rule="tool.mcp_proxy.*",
                created_at=utc_now(),
                rule_id=None,
                used_fallback=True,
                explanation_summary="Fallback: external proxy requires approval",
                source_document_id=compiled_set.document_id,
                source_document_version=compiled_set.document_version,
            ))
        
        else:
            # Unknown tool - deny by default
            verdicts.append(PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject=subject,
                decision="deny",
                reason="Unknown operations are denied by default.",
                matched_rule="default.deny",
                created_at=utc_now(),
                rule_id=None,
                used_fallback=True,
                explanation_summary="Fallback: unknown tool denied by default",
                source_document_id=compiled_set.document_id,
                source_document_version=compiled_set.document_version,
            ))
        
        # Build explanation
        matched_rules = [
            MatchedRuleInfo(
                rule_id="fallback",
                subject_pattern=v.matched_rule,
                decision=v.decision,
                priority=50,
                source_document_id=v.source_document_id,
                source_document_version=v.source_document_version,
                matched_conditions=[],
                reason=v.reason,
            )
            for v in verdicts
        ]
        
        explanation = ConstraintExplanation(
            subject=subject,
            final_decision=self._calculate_final_decision(verdicts),
            final_reason=self._build_final_reason(verdicts),
            matched_rules=matched_rules,
            evaluated_rules=0,
            used_fallback=True,
            fallback_reason="No compiled rules matched subject",
            compilation_status=compiled_set.compilation_status,
            compiled_rule_count=len(compiled_set.rules),
            context_snapshot=ctx.to_dict(),
        )
        
        return verdicts, explanation

    def _calculate_final_decision(self, verdicts: List[PolicyVerdict]) -> str:
        """Calculate the final decision using deny-before-allow semantics."""
        precedence = {"deny": 0, "approval_required": 1, "allow": 2}
        
        if not verdicts:
            return "deny"
        
        # Sort by precedence and return the most restrictive
        sorted_verdicts = sorted(verdicts, key=lambda v: precedence.get(v.decision, 99))
        return sorted_verdicts[0].decision

    def _build_final_reason(self, verdicts: List[PolicyVerdict]) -> str:
        """Build a human-readable final reason from verdicts."""
        if not verdicts:
            return "No rules matched - denied by default"
        
        # Get the most restrictive verdict
        precedence = {"deny": 0, "approval_required": 1, "allow": 2}
        sorted_verdicts = sorted(verdicts, key=lambda v: precedence.get(v.decision, 99))
        final = sorted_verdicts[0]
        
        return final.reason

    def final_verdict(
        self,
        verdicts: List[PolicyVerdict],
        explanation: ConstraintExplanation,
    ) -> PolicyVerdict:
        """Create the final consolidated verdict from multiple verdicts."""
        precedence = {"deny": 0, "approval_required": 1, "allow": 2}
        
        if not verdicts:
            return PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject=explanation.subject,
                decision="deny",
                reason="No rules matched - denied by default",
                matched_rule="default.deny",
                created_at=utc_now(),
                rule_id=None,
                source_document_id=None,
                source_document_version=None,
                used_fallback=explanation.used_fallback,
                explanation_summary="Default deny - no matching rules",
            )
        
        # Sort by precedence
        sorted_verdicts = sorted(verdicts, key=lambda v: precedence.get(v.decision, 99))
        final = sorted_verdicts[0]
        
        # Create a consolidated verdict
        return PolicyVerdict(
            verdict_id=new_id("verdict"),
            subject=final.subject,
            decision=final.decision,
            reason=final.reason,
            matched_rule=final.matched_rule,
            created_at=utc_now(),
            rule_id=final.rule_id,
            source_document_id=final.source_document_id,
            source_document_version=final.source_document_version,
            used_fallback=explanation.used_fallback,
            explanation_summary=explanation.final_reason,
        )
