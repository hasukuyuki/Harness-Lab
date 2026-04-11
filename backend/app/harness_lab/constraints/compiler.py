"""ConstraintCompiler - Compiles parsed rules into standardized rule sets.

The compiler transforms parsed rule intentions into a normalized, executable
form (CompiledConstraintSet) that can be stored and efficiently evaluated.
"""

from __future__ import annotations

from typing import List, Optional, Dict, Any

from ..types import (
    ConstraintRule,
    RuleCondition,
    CompiledConstraintSet,
    ConstraintCompileSummary,
    ConstraintCompileResult,
)
from ..utils import new_id, utc_now
from .parser import ConstraintParser, ParsedRule, ParsedCondition


class ConstraintCompiler:
    """Compiles parsed constraint rules into standardized executable form.
    
    The compiler is responsible for:
    1. Transforming ParsedRule objects into ConstraintRule objects
    2. Assigning stable rule IDs
    3. Normalizing condition operators and values
    4. Validating rule consistency
    5. Handling compilation errors gracefully with fallback
    
    The compiler works with the parser output and produces a CompiledConstraintSet
    that can be serialized and stored in the document's payload_json.
    """

    def __init__(self) -> None:
        self.parser = ConstraintParser()

    def compile_document(
        self,
        document_id: str,
        body: str,
        tags: List[str],
        version: str = "v1",
    ) -> ConstraintCompileResult:
        """Compile a constraint document into a standardized rule set.
        
        Args:
            document_id: The ID of the constraint document
            body: The natural language body of the document
            tags: Tags associated with the document
            version: Document version
            
        Returns:
            ConstraintCompileResult with compilation status and metadata
        """
        try:
            # Parse the document
            parsed_rules = self.parser.parse(body, tags)
            
            # Transform parsed rules into standardized rules
            rules: List[ConstraintRule] = []
            errors: List[str] = []
            
            for i, parsed in enumerate(parsed_rules):
                try:
                    rule = self._compile_rule(parsed, document_id, i)
                    rules.append(rule)
                except Exception as e:
                    errors.append(f"Failed to compile rule {i}: {str(e)}")
            
            # Determine compilation status
            if not rules:
                status = "failed"
                used_fallback = True
                fallback_reason = "No rules could be compiled from document"
            elif errors:
                status = "partial"
                used_fallback = False
                fallback_reason = None
            else:
                status = "success"
                used_fallback = False
                fallback_reason = None
            
            # If compilation failed completely, use fallback rules
            if status == "failed":
                rules = self._create_fallback_rules(document_id)
                used_fallback = True
                fallback_reason = "Compilation failed, using fallback rules"
            
            return ConstraintCompileResult(
                document_id=document_id,
                status=status,
                rules_compiled=len(rules),
                errors=errors,
                used_fallback=used_fallback,
                fallback_reason=fallback_reason,
            )
            
        except Exception as e:
            # Critical failure - use fallback rules
            return ConstraintCompileResult(
                document_id=document_id,
                status="failed",
                rules_compiled=0,
                errors=[f"Critical compilation error: {str(e)}"],
                used_fallback=True,
                fallback_reason=f"Critical error: {str(e)}",
            )

    def compile_to_set(
        self,
        document_id: str,
        body: str,
        tags: List[str],
        version: str = "v1",
    ) -> CompiledConstraintSet:
        """Compile a document and return the full CompiledConstraintSet.
        
        This method is used when you need the actual compiled rules,
        not just the compilation metadata.
        """
        # Parse the document
        parsed_rules = self.parser.parse(body, tags)
        
        # Transform parsed rules into standardized rules
        rules: List[ConstraintRule] = []
        errors: List[str] = []
        used_fallback = False
        fallback_reason: Optional[str] = None
        
        for i, parsed in enumerate(parsed_rules):
            try:
                rule = self._compile_rule(parsed, document_id, i)
                rules.append(rule)
            except Exception as e:
                errors.append(f"Failed to compile rule {i}: {str(e)}")
        
        # Determine compilation status
        if not rules:
            compilation_status = "failed"
            used_fallback = True
            fallback_reason = "No rules could be compiled from document"
            rules = self._create_fallback_rules(document_id)
        elif errors:
            compilation_status = "partial"
        else:
            compilation_status = "success"
        
        return CompiledConstraintSet(
            compiled_at=utc_now(),
            document_id=document_id,
            document_version=version,
            rules=rules,
            compilation_status=compilation_status,
            compilation_errors=errors,
            used_fallback=used_fallback,
            fallback_reason=fallback_reason,
        )

    def _compile_rule(
        self,
        parsed: ParsedRule,
        document_id: str,
        index: int,
    ) -> ConstraintRule:
        """Compile a single parsed rule into a standardized ConstraintRule."""
        # Generate stable rule ID
        rule_id = f"{document_id}_rule_{index}_{parsed.subject_pattern.replace('.', '_')}"
        
        # Compile conditions
        conditions = [self._compile_condition(c) for c in parsed.conditions]
        
        # Validate decision
        if parsed.decision not in {"allow", "deny", "approval_required"}:
            raise ValueError(f"Invalid decision: {parsed.decision}")
        
        return ConstraintRule(
            rule_id=rule_id,
            source_document_id=document_id,
            subject_pattern=parsed.subject_pattern,
            conditions=conditions,
            decision=parsed.decision,
            priority=parsed.priority,
            reason_template=parsed.reason_template,
            tags=[],  # Could extract tags from source text in future
            created_at=utc_now(),
        )

    def _compile_condition(self, parsed: ParsedCondition) -> RuleCondition:
        """Compile a parsed condition into a standardized RuleCondition."""
        # Normalize operator
        operator = self._normalize_operator(parsed.operator)
        
        # Normalize value based on field type
        value = self._normalize_value(parsed.field, parsed.value)
        
        return RuleCondition(
            field=parsed.field,
            operator=operator,
            value=value,
            description=parsed.source_text if parsed.source_text else None,
        )

    def _normalize_operator(self, operator: str) -> str:
        """Normalize condition operator."""
        valid_operators = {"eq", "ne", "contains", "prefix", "suffix", "regex", "in", "not_in"}
        
        operator_map = {
            "=": "eq",
            "!=": "ne",
            "equals": "eq",
            "is": "eq",
            "starts_with": "prefix",
            "ends_with": "suffix",
            "matches": "regex",
            "has": "contains",
            "includes": "contains",
        }
        
        normalized = operator_map.get(operator.lower(), operator.lower())
        
        if normalized not in valid_operators:
            # Default to eq for unknown operators
            return "eq"
        
        return normalized

    def _normalize_value(self, field: str, value: Any) -> Any:
        """Normalize condition value based on field type."""
        if field in {"sandbox_required"}:
            # Boolean fields
            if isinstance(value, str):
                return value.lower() in {"true", "yes", "1", "required", "enabled"}
            return bool(value)
        
        if field in {"network_mode", "git_mutability", "action"}:
            # String enum fields
            return str(value).lower()
        
        if field == "path":
            # Path fields - normalize slashes
            return str(value).replace("\\", "/")
        
        # Default: return as-is
        return value

    def _create_fallback_rules(self, document_id: str) -> List[ConstraintRule]:
        """Create fallback rules when compilation fails.
        
        These rules mirror the behavior of the original heuristic ConstraintEngine
        to ensure backward compatibility.
        """
        now = utc_now()
        
        return [
            # Shell rules
            ConstraintRule(
                rule_id=f"{document_id}_fallback_shell_default",
                source_document_id=document_id,
                subject_pattern="tool.shell.*",
                conditions=[],
                decision="approval_required",
                priority=50,
                reason_template="Shell commands default to review in Harness Lab.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_shell_readonly",
                source_document_id=document_id,
                subject_pattern="tool.shell.*",
                conditions=[RuleCondition(field="action", operator="eq", value="read")],
                decision="allow",
                priority=45,
                reason_template="Read-only shell commands may run without approval.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_shell_destructive",
                source_document_id=document_id,
                subject_pattern="tool.shell.*",
                conditions=[RuleCondition(field="action", operator="eq", value="destructive")],
                decision="deny",
                priority=10,
                reason_template="Destructive shell patterns are denied by the research guardrails.",
                tags=["fallback"],
                created_at=now,
            ),
            # Filesystem rules
            ConstraintRule(
                rule_id=f"{document_id}_fallback_fs_read",
                source_document_id=document_id,
                subject_pattern="tool.filesystem.read_file",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="Read-only filesystem access is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_fs_list",
                source_document_id=document_id,
                subject_pattern="tool.filesystem.list_dir",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="Directory listing is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_fs_write",
                source_document_id=document_id,
                subject_pattern="tool.filesystem.write_file",
                conditions=[],
                decision="approval_required",
                priority=50,
                reason_template="Filesystem writes require review.",
                tags=["fallback"],
                created_at=now,
            ),
            # Git rules
            ConstraintRule(
                rule_id=f"{document_id}_fallback_git_read",
                source_document_id=document_id,
                subject_pattern="tool.git.status",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="Read-only git inspection is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_git_diff",
                source_document_id=document_id,
                subject_pattern="tool.git.diff",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="Read-only git inspection is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_git_log",
                source_document_id=document_id,
                subject_pattern="tool.git.log",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="Read-only git inspection is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_git_mutable",
                source_document_id=document_id,
                subject_pattern="tool.git.*",
                conditions=[RuleCondition(field="action", operator="eq", value="mutate")],
                decision="approval_required",
                priority=40,
                reason_template="Mutable git actions require review.",
                tags=["fallback"],
                created_at=now,
            ),
            # HTTP fetch rules
            ConstraintRule(
                rule_id=f"{document_id}_fallback_http_get",
                source_document_id=document_id,
                subject_pattern="tool.http_fetch.get",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="HTTP GET is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            ConstraintRule(
                rule_id=f"{document_id}_fallback_http_strict",
                source_document_id=document_id,
                subject_pattern="tool.http_fetch.*",
                conditions=[RuleCondition(field="network_mode", operator="eq", value="strict")],
                decision="approval_required",
                priority=20,
                reason_template="Network fetches require review in strict-network mode.",
                tags=["fallback"],
                created_at=now,
            ),
            # Knowledge search
            ConstraintRule(
                rule_id=f"{document_id}_fallback_knowledge",
                source_document_id=document_id,
                subject_pattern="tool.knowledge_search.query",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="Knowledge search is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            # Model reflection
            ConstraintRule(
                rule_id=f"{document_id}_fallback_model",
                source_document_id=document_id,
                subject_pattern="tool.model_reflection.run",
                conditions=[],
                decision="allow",
                priority=50,
                reason_template="Local model reflection is allowed.",
                tags=["fallback"],
                created_at=now,
            ),
            # MCP proxy
            ConstraintRule(
                rule_id=f"{document_id}_fallback_mcp",
                source_document_id=document_id,
                subject_pattern="tool.mcp_proxy.*",
                conditions=[],
                decision="approval_required",
                priority=30,
                reason_template="External proxy calls require review.",
                tags=["fallback"],
                created_at=now,
            ),
            # Default deny
            ConstraintRule(
                rule_id=f"{document_id}_fallback_default_deny",
                source_document_id=document_id,
                subject_pattern="*",
                conditions=[],
                decision="deny",
                priority=100,
                reason_template="Unknown operations are denied by default.",
                tags=["fallback", "default"],
                created_at=now,
            ),
        ]

    def recompile_if_needed(
        self,
        document_id: str,
        body: str,
        tags: List[str],
        existing_compiled: Optional[CompiledConstraintSet],
        version: str = "v1",
    ) -> Optional[CompiledConstraintSet]:
        """Recompile a document only if necessary.
        
        Returns None if the existing compilation is still valid.
        """
        if existing_compiled is None:
            return self.compile_to_set(document_id, body, tags, version)
        
        # Check if document content has changed
        # In a real implementation, we might use content hashing
        # For now, we assume compilation is idempotent
        return None
