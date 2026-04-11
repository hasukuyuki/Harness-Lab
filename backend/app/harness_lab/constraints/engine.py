"""ConstraintEngine - Semantic constraint enforcement with three-stage pipeline.

The engine orchestrates the constraint lifecycle:
1. Parser: Extracts rule intentions from natural language
2. Compiler: Produces standardized, executable rule sets
3. Verifier: Evaluates tool calls against compiled rules

This replaces the original heuristic ConstraintEngine while maintaining
backward compatibility through intelligent fallback.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from ..storage import HarnessLabDatabase
from ..types import (
    ConstraintCreateRequest,
    ConstraintDocument,
    ConstraintCompileSummary,
    CompiledConstraintSet,
    ConstraintExplanation,
    ConstraintVerifyResponse,
    MatchedRuleInfo,
    PolicyVerdict,
    ConstraintEngineStatus,
)
from ..utils import new_id, utc_now
from .parser import ConstraintParser
from .compiler import ConstraintCompiler
from .verifier import ConstraintVerifier


class ConstraintEngine:
    """Semantic constraint engine with three-stage pipeline.
    
    The engine provides:
    - Natural language constraint document management
    - Automatic compilation of documents into executable rules
    - Runtime verification with detailed explanations
    - Backward compatibility through fallback mode
    
    Compilation results are stored in the document's payload_json to avoid
    schema migration, making the system backward compatible.
    """

    def __init__(self, database: HarnessLabDatabase) -> None:
        self.database = database
        self.parser = ConstraintParser()
        self.compiler = ConstraintCompiler()
        self.verifier = ConstraintVerifier()

    def list_documents(self, status: Optional[str] = None) -> List[ConstraintDocument]:
        """List constraint documents, optionally filtered by status."""
        if status:
            rows = self.database.fetchall(
                "SELECT payload_json FROM constraints_documents WHERE status = ? ORDER BY updated_at DESC",
                (status,),
            )
        else:
            rows = self.database.fetchall("SELECT payload_json FROM constraints_documents ORDER BY updated_at DESC")
        
        documents = []
        for row in rows:
            try:
                doc = ConstraintDocument(**json.loads(row["payload_json"]))
                documents.append(doc)
            except (json.JSONDecodeError, ValueError):
                # Skip invalid documents
                continue
        
        return documents

    def get_document(self, document_id: str) -> ConstraintDocument:
        """Get a constraint document by ID."""
        row = self.database.fetchone(
            "SELECT payload_json FROM constraints_documents WHERE document_id = ?",
            (document_id,),
        )
        if not row:
            raise ValueError("Constraint document not found")
        return ConstraintDocument(**json.loads(row["payload_json"]))

    def get_document_with_summary(self, document_id: str) -> Dict[str, Any]:
        """Get a constraint document with compilation summary.
        
        This is the enhanced version for GET /api/constraints/{id}
        """
        document = self.get_document(document_id)
        
        # Parse the compiled data from payload_json if present
        compiled_set = self._get_compiled_set(document)
        
        summary = ConstraintCompileSummary(
            status=compiled_set.compilation_status if compiled_set else "not_compiled",
            compiled_at=compiled_set.compiled_at if compiled_set else None,
            rule_count=len(compiled_set.rules) if compiled_set else 0,
            errors=compiled_set.compilation_errors if compiled_set else [],
            used_fallback=compiled_set.used_fallback if compiled_set else False,
        )
        
        return {
            "document": document.model_dump(),
            "compilation_summary": summary.model_dump(),
        }

    def create_document(self, request: ConstraintCreateRequest) -> ConstraintDocument:
        """Create a new constraint document and compile it."""
        now = utc_now()
        document = ConstraintDocument(
            document_id=new_id("constraint"),
            title=request.title,
            body=request.body,
            scope=request.scope,
            status="candidate",
            tags=request.tags,
            priority=request.priority,
            source=request.source,
            version="v1",
            created_at=now,
            updated_at=now,
        )
        
        # Compile the document immediately
        compiled_set = self.compiler.compile_to_set(
            document_id=document.document_id,
            body=document.body,
            tags=document.tags,
            version=document.version,
        )
        
        # Store compilation summary
        document.compiled = ConstraintCompileSummary(
            status=compiled_set.compilation_status,
            compiled_at=compiled_set.compiled_at,
            rule_count=len(compiled_set.rules),
            errors=compiled_set.compilation_errors,
            used_fallback=compiled_set.used_fallback,
        )
        
        self._persist(document)
        return document

    def publish(self, document_id: str) -> ConstraintDocument:
        """Publish a constraint document."""
        document = self.get_document(document_id)
        document.status = "published"
        document.updated_at = utc_now()
        
        # Recompile on publish to ensure freshness
        compiled_set = self.compiler.compile_to_set(
            document_id=document.document_id,
            body=document.body,
            tags=document.tags,
            version=document.version,
        )
        
        document.compiled = ConstraintCompileSummary(
            status=compiled_set.compilation_status,
            compiled_at=compiled_set.compiled_at,
            rule_count=len(compiled_set.rules),
            errors=compiled_set.compilation_errors,
            used_fallback=compiled_set.used_fallback,
        )
        
        self._persist(document)
        return document

    def archive(self, document_id: str) -> ConstraintDocument:
        """Archive a constraint document."""
        document = self.get_document(document_id)
        document.status = "archived"
        document.updated_at = utc_now()
        self._persist(document)
        return document

    def verify(
        self,
        subject: str,
        payload: Dict[str, Any],
        constraint_set_id: Optional[str] = None,
        runtime_context: Optional[Dict[str, Any]] = None,
    ) -> ConstraintVerifyResponse:
        """Verify constraints against a tool invocation.
        
        This is the enhanced version that returns detailed explanations.
        For backward compatibility, the old verify() behavior is preserved
        through the verify_legacy() method.
        """
        # Get the constraint document
        if constraint_set_id:
            document = self.get_document(constraint_set_id)
        else:
            documents = self.list_documents(status="published")
            if not documents:
                # No published constraints - use fallback
                return self._fallback_response(subject, payload)
            document = documents[0]
        
        # Get or compile the constraint set
        compiled_set = self._get_or_compile_set(document)
        
        # Verify against the compiled rules
        verdicts, explanation = self.verifier.verify(
            compiled_set=compiled_set,
            subject=subject,
            payload=payload,
            runtime_context=runtime_context,
        )
        
        # Calculate final verdict
        final_verdict = self.verifier.final_verdict(verdicts, explanation)
        
        return ConstraintVerifyResponse(
            verdicts=verdicts,
            final_verdict=final_verdict,
            explanation=explanation,
            compiled_rule_count=len(compiled_set.rules),
            used_fallback=explanation.used_fallback,
            matched_rules=explanation.matched_rules,
        )

    def verify_legacy(
        self,
        subject: str,
        payload: Dict[str, Any],
        constraint_set_id: Optional[str] = None,
    ) -> List[PolicyVerdict]:
        """Legacy verify method for backward compatibility.
        
        Returns just the list of verdicts without explanation.
        This is used by existing code that expects the old API.
        """
        response = self.verify(subject, payload, constraint_set_id)
        return response.verdicts

    def final_verdict(self, verdicts: List[PolicyVerdict]) -> PolicyVerdict:
        """Calculate the final verdict from multiple verdicts.
        
        Uses deny-before-allow semantics.
        """
        precedence = {"deny": 0, "approval_required": 1, "allow": 2}
        
        if not verdicts:
            return PolicyVerdict(
                verdict_id=new_id("verdict"),
                subject="unknown",
                decision="deny",
                reason="No rules matched - denied by default",
                matched_rule="default.deny",
                created_at=utc_now(),
            )
        
        return sorted(verdicts, key=lambda v: precedence.get(v.decision, 99))[0]

    def get_engine_status(self) -> ConstraintEngineStatus:
        """Get the current status of the constraint engine.
        
        Used by health check endpoints.
        """
        try:
            all_docs = self.list_documents()
            published_docs = [d for d in all_docs if d.status == "published"]
            
            # Check if any documents are using fallback
            fallback_count = sum(
                1 for d in all_docs
                if d.compiled and d.compiled.used_fallback
            )
            
            return ConstraintEngineStatus(
                constraint_engine_version="v2",
                constraint_parser_ready=True,
                constraint_compiler_ready=True,
                constraint_fallback_mode=fallback_count > 0,
                published_constraint_count=len(published_docs),
                total_constraint_count=len(all_docs),
            )
        except Exception:
            # Return degraded status if we can't query the database
            return ConstraintEngineStatus(
                constraint_engine_version="v2",
                constraint_parser_ready=False,
                constraint_compiler_ready=False,
                constraint_fallback_mode=True,
                published_constraint_count=0,
                total_constraint_count=0,
            )

    def _get_compiled_set(self, document: ConstraintDocument) -> Optional[CompiledConstraintSet]:
        """Get the compiled constraint set for a document.
        
        This reads from the stored payload_json extension field.
        """
        # For now, we recompile on demand since the compiled data is stored
        # in the document's compiled field (ConstraintCompileSummary)
        # In a production system, we might cache the full CompiledConstraintSet
        return self.compiler.compile_to_set(
            document_id=document.document_id,
            body=document.body,
            tags=document.tags,
            version=document.version,
        )

    def _get_or_compile_set(self, document: ConstraintDocument) -> CompiledConstraintSet:
        """Get the compiled set for a document, compiling if necessary."""
        # Always recompile to ensure freshness
        # In production, we might check compilation timestamps
        return self.compiler.compile_to_set(
            document_id=document.document_id,
            body=document.body,
            tags=document.tags,
            version=document.version,
        )

    def _fallback_response(
        self,
        subject: str,
        payload: Dict[str, Any],
    ) -> ConstraintVerifyResponse:
        """Generate a fallback response when no constraints are available."""
        verdict = PolicyVerdict(
            verdict_id=new_id("verdict"),
            subject=subject,
            decision="approval_required",
            reason="No published constraint set available - defaulting to approval required",
            matched_rule="fallback.no_constraints",
            created_at=utc_now(),
            rule_id=None,
            used_fallback=True,
            explanation_summary="No constraints configured",
        )
        
        matched_rule = MatchedRuleInfo(
            rule_id="fallback",
            subject_pattern="*",
            decision="approval_required",
            priority=50,
            matched_conditions=[],
            reason=verdict.reason,
        )
        
        explanation = ConstraintExplanation(
            subject=subject,
            final_decision="approval_required",
            final_reason=verdict.reason,
            matched_rules=[matched_rule],
            evaluated_rules=0,
            used_fallback=True,
            fallback_reason="No published constraint documents available",
            compilation_status="not_compiled",
            compiled_rule_count=0,
            context_snapshot={"subject": subject, "payload": payload},
        )
        
        return ConstraintVerifyResponse(
            verdicts=[verdict],
            final_verdict=verdict,
            explanation=explanation,
            compiled_rule_count=0,
            used_fallback=True,
            matched_rules=[matched_rule],
        )

    def _persist(self, document: ConstraintDocument) -> None:
        """Persist a constraint document to the database."""
        self.database.upsert_row(
            "constraints_documents",
            {
                "document_id": document.document_id,
                "title": document.title,
                "scope": document.scope,
                "status": document.status,
                "version": document.version,
                "payload_json": json.dumps(document.model_dump(exclude_none=True), ensure_ascii=False),
                "created_at": document.created_at,
                "updated_at": document.updated_at,
            },
            "document_id",
        )


# Legacy compatibility: export the ConstraintEngine at module level
# This ensures existing imports continue to work
__all__ = ["ConstraintEngine"]
