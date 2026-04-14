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
    ConstraintPublishGateStatus,
    ConstraintScenario,
    ConstraintScenarioCreateRequest,
    ConstraintScenarioResult,
    ConstraintValidateRequest,
    ConstraintValidationReport,
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

    def create_scenario(self, request: ConstraintScenarioCreateRequest) -> ConstraintScenario:
        """Persist a reusable validation scenario for a constraint chain."""
        now = utc_now()
        scenario = ConstraintScenario(
            scenario_id=new_id("scenario"),
            root_document_id=request.root_document_id,
            name=request.name,
            subject=request.subject,
            payload=request.payload,
            expected_decision=request.expected_decision,
            tags=request.tags,
            created_at=now,
            updated_at=now,
        )
        self._persist_scenario(scenario)
        return scenario

    def list_scenarios(self, root_document_id: Optional[str] = None) -> List[ConstraintScenario]:
        """List saved validation scenarios, optionally filtered by chain."""
        if root_document_id:
            rows = self.database.fetchall(
                "SELECT payload_json FROM constraint_scenarios WHERE root_document_id = ? ORDER BY created_at ASC",
                (root_document_id,),
            )
        else:
            rows = self.database.fetchall(
                "SELECT payload_json FROM constraint_scenarios ORDER BY created_at ASC",
            )
        scenarios: List[ConstraintScenario] = []
        for row in rows:
            try:
                scenarios.append(ConstraintScenario(**json.loads(row["payload_json"])))
            except (json.JSONDecodeError, ValueError):
                continue
        return scenarios

    def create_document(self, request: ConstraintCreateRequest) -> ConstraintDocument:
        """Create a new constraint document and compile it."""
        now = utc_now()
        document_id = new_id("constraint")
        document = ConstraintDocument(
            document_id=document_id,
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
            root_document_id=document_id,  # Self for new documents
            parent_document_id=None,  # No parent for new documents
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

    def revise(self, document_id: str, body: Optional[str] = None, title: Optional[str] = None) -> ConstraintDocument:
        """Create a revision of an existing constraint document.
        
        This creates a new candidate document that is a copy of the original
        with the specified modifications. The new document will have:
        - Same root_document_id as the original
        - parent_document_id pointing to the original
        - version incremented (e.g., v1 -> v2)
        - status set to candidate
        """
        original = self.get_document(document_id)
        now = utc_now()
        new_document_id = new_id("constraint")
        
        # Always derive the next version from the full chain so sibling revisions
        # cannot accidentally reuse the same version label.
        version_num = self._next_version_number(original.root_document_id or original.document_id)
        new_version = f"v{version_num}"
        
        # Create the revised document
        document = ConstraintDocument(
            document_id=new_document_id,
            title=title or original.title,
            body=body or original.body,
            scope=original.scope,
            status="candidate",
            tags=original.tags,
            priority=original.priority,
            source=original.source,
            version=new_version,
            created_at=now,
            updated_at=now,
            root_document_id=original.root_document_id,  # Preserve root chain
            parent_document_id=document_id,  # Link to parent
        )
        
        # Compile the document
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

    def list_versions(self, root_document_id: str) -> List[ConstraintDocument]:
        """List all versions of a constraint document in the chain.
        
        Returns documents ordered by version number, from oldest to newest.
        """
        # Read through payload_json instead of trusting only the denormalized
        # column so older local/test databases remain compatible.
        rows = self.database.fetchall(
            "SELECT payload_json FROM constraints_documents ORDER BY created_at ASC",
        )

        documents = []
        for row in rows:
            try:
                doc = ConstraintDocument(**json.loads(row["payload_json"]))
                if (doc.root_document_id or doc.document_id) == root_document_id:
                    documents.append(doc)
            except (json.JSONDecodeError, ValueError):
                continue
        
        return sorted(
            documents,
            key=lambda doc: (self._version_number(doc.version), doc.created_at),
        )

    def publish_with_archive(self, document_id: str) -> ConstraintDocument:
        """Publish a constraint document and archive previous published versions.
        
        When publishing a new version in a chain, any previously published
        version in the same chain will be automatically archived.
        """
        document = self.get_document(document_id)
        gate = self.get_publish_gate(document_id)
        if not gate.publish_ready:
            raise ValueError(f"Constraint publish blocked: {'; '.join(gate.blockers)}")
        
        # Archive previous published versions in the same chain
        if document.root_document_id:
            chain_docs = self.list_versions(document.root_document_id)
            for chain_doc in chain_docs:
                if chain_doc.document_id != document_id and chain_doc.status == "published":
                    chain_doc.status = "archived"
                    chain_doc.updated_at = utc_now()
                    self._persist(chain_doc)
        
        # Now publish the new document
        document.status = "published"
        document.updated_at = utc_now()
        
        # Recompile on publish
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

    def validate_document(
        self,
        document_id: str,
        request: Optional[ConstraintValidateRequest] = None,
    ) -> ConstraintValidationReport:
        """Validate a candidate document against saved scenarios."""
        document = self.get_document(document_id)
        root_document_id = document.root_document_id or document.document_id
        scenarios = self.list_scenarios(root_document_id)
        selected_ids = set(request.scenario_ids) if request and request.scenario_ids else None
        if selected_ids is not None:
            scenarios = [scenario for scenario in scenarios if scenario.scenario_id in selected_ids]

        compiled_set = self._get_or_compile_set(document)
        results: List[ConstraintScenarioResult] = []
        hard_failure_count = 0
        blockers: List[str] = []

        if compiled_set.compilation_status == "failed":
            blockers.append("Compilation failed for this constraint document.")

        if not scenarios:
            blockers.append("No validation scenarios saved for this constraint chain.")

        for scenario in scenarios:
            response = self.verify(
                subject=scenario.subject,
                payload=scenario.payload,
                constraint_set_id=document.document_id,
            )
            actual_decision = response.final_verdict.decision
            passed = actual_decision == scenario.expected_decision
            hard_failure = self._is_hard_validation_failure(scenario.expected_decision, actual_decision)
            if hard_failure:
                hard_failure_count += 1
            if not passed:
                blockers.append(
                    f"Scenario '{scenario.name}' expected {scenario.expected_decision} but got {actual_decision}."
                )
            results.append(
                ConstraintScenarioResult(
                    scenario_id=scenario.scenario_id,
                    name=scenario.name,
                    expected_decision=scenario.expected_decision,
                    actual_decision=actual_decision,
                    passed=passed,
                    hard_failure=hard_failure,
                    used_fallback=response.used_fallback,
                    matched_rule_ids=[rule.rule_id for rule in response.matched_rules],
                    matched_document_ids=list(
                        {
                            rule.source_document_id
                            for rule in response.matched_rules
                            if rule.source_document_id
                        }
                    ),
                    explanation=response.explanation.final_reason,
                )
            )

        passed_scenarios = sum(1 for result in results if result.passed)
        failed_scenarios = len(results) - passed_scenarios
        soft_deviation_count = sum(1 for result in results if (not result.passed and not result.hard_failure))
        status = "passed" if not blockers else "failed"
        report = ConstraintValidationReport(
            report_id=new_id("constraint_validation"),
            document_id=document.document_id,
            root_document_id=root_document_id,
            document_version=document.version,
            status=status,
            compilation_status=compiled_set.compilation_status,
            compiled_rule_count=len(compiled_set.rules),
            total_scenarios=len(results),
            passed_scenarios=passed_scenarios,
            failed_scenarios=failed_scenarios,
            hard_failure_count=hard_failure_count,
            soft_deviation_count=soft_deviation_count,
            blockers=list(dict.fromkeys(blockers)),
            scenario_results=results,
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        self._persist_validation_report(report)
        return report

    def latest_validation_report(self, document_id: str) -> Optional[ConstraintValidationReport]:
        """Return the most recent validation report for a document, if any."""
        row = self.database.fetchone(
            "SELECT payload_json FROM constraint_validation_reports WHERE document_id = ? ORDER BY updated_at DESC LIMIT 1",
            (document_id,),
        )
        if not row:
            return None
        return ConstraintValidationReport(**json.loads(row["payload_json"]))

    def get_publish_gate(self, document_id: str) -> ConstraintPublishGateStatus:
        """Compute publish readiness for a constraint candidate."""
        document = self.get_document(document_id)
        root_document_id = document.root_document_id or document.document_id
        latest_report = self.latest_validation_report(document_id)
        compilation_ok = bool(document.compiled and document.compiled.status != "failed")
        scenario_count = latest_report.total_scenarios if latest_report else len(self.list_scenarios(root_document_id))
        validation_ok = bool(latest_report and latest_report.status == "passed" and latest_report.failed_scenarios == 0)
        blockers: List[str] = []

        if not compilation_ok:
            blockers.append("Compilation must succeed before publishing.")
        if scenario_count == 0:
            blockers.append("At least one validation scenario is required before publishing.")
        if latest_report is None:
            blockers.append("Run a validation suite for this candidate before publishing.")
        elif not validation_ok:
            blockers.extend(latest_report.blockers)
        if latest_report and latest_report.hard_failure_count > 0:
            blockers.append("Safety regression detected in validation suite.")

        return ConstraintPublishGateStatus(
            document_id=document.document_id,
            root_document_id=root_document_id,
            document_version=document.version,
            publish_ready=not blockers,
            compilation_ok=compilation_ok,
            validation_ok=validation_ok,
            scenario_count=scenario_count,
            hard_failure_count=latest_report.hard_failure_count if latest_report else 0,
            blockers=list(dict.fromkeys(blockers)),
            latest_validation_report=latest_report,
        )

    def _version_number(self, version: str) -> int:
        """Parse a semantic version label like v3 into an integer for sorting."""
        match = re.match(r"^v(\d+)$", version or "")
        if not match:
            return 0
        return int(match.group(1))

    def _next_version_number(self, root_document_id: str) -> int:
        """Return the next monotonic version number for a document chain."""
        versions = self.list_versions(root_document_id)
        current_max = max((self._version_number(document.version) for document in versions), default=0)
        return current_max + 1

    def diff_documents(self, document_id: str, against_document_id: str) -> Dict[str, Any]:
        """Compare two constraint documents.
        
        Returns diff information including body changes and compilation summary changes.
        """
        doc1 = self.get_document(document_id)
        doc2 = self.get_document(against_document_id)
        
        # Simple text diff for body
        body1_lines = doc1.body.splitlines()
        body2_lines = doc2.body.splitlines()
        
        added_lines = [line for line in body2_lines if line not in body1_lines]
        removed_lines = [line for line in body1_lines if line not in body2_lines]
        
        # Compilation summary diff
        compiled1 = doc1.compiled or ConstraintCompileSummary()
        compiled2 = doc2.compiled or ConstraintCompileSummary()
        
        return {
            "base_document_id": document_id,
            "target_document_id": against_document_id,
            "base_version": doc1.version,
            "target_version": doc2.version,
            "body_diff": {
                "added_lines": added_lines,
                "removed_lines": removed_lines,
                "unchanged_lines": len(body1_lines) - len(removed_lines),
            },
            "compilation_diff": {
                "rule_count_change": compiled2.rule_count - compiled1.rule_count,
                "status_change": {
                    "from": compiled1.status,
                    "to": compiled2.status,
                },
                "errors_change": {
                    "from": compiled1.errors,
                    "to": compiled2.errors,
                },
                "fallback_change": {
                    "from": compiled1.used_fallback,
                    "to": compiled2.used_fallback,
                },
            },
            "metadata_diff": {
                "title_change": doc1.title != doc2.title,
                "priority_change": doc1.priority != doc2.priority,
                "tags_added": [t for t in doc2.tags if t not in doc1.tags],
                "tags_removed": [t for t in doc1.tags if t not in doc2.tags],
            },
        }

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
            constraint_document_id=document.document_id,
            constraint_root_document_id=document.root_document_id or document.document_id,
            constraint_document_version=document.version,
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
            constraint_document_id="fallback.no_constraints",
            constraint_root_document_id="fallback.no_constraints",
            constraint_document_version="fallback",
            verdicts=[verdict],
            final_verdict=verdict,
            explanation=explanation,
            compiled_rule_count=0,
            used_fallback=True,
            matched_rules=[matched_rule],
        )

    def _is_hard_validation_failure(self, expected_decision: str, actual_decision: str) -> bool:
        """Treat any safety loosening as a hard failure."""
        precedence = {"deny": 0, "approval_required": 1, "allow": 2}
        return precedence.get(actual_decision, 99) > precedence.get(expected_decision, 99)

    def _persist_scenario(self, scenario: ConstraintScenario) -> None:
        self.database.upsert_row(
            "constraint_scenarios",
            {
                "scenario_id": scenario.scenario_id,
                "root_document_id": scenario.root_document_id,
                "name": scenario.name,
                "expected_decision": scenario.expected_decision,
                "payload_json": json.dumps(scenario.model_dump(), ensure_ascii=False),
                "created_at": scenario.created_at,
                "updated_at": scenario.updated_at,
            },
            "scenario_id",
        )

    def _persist_validation_report(self, report: ConstraintValidationReport) -> None:
        self.database.upsert_row(
            "constraint_validation_reports",
            {
                "report_id": report.report_id,
                "document_id": report.document_id,
                "root_document_id": report.root_document_id,
                "status": report.status,
                "payload_json": json.dumps(report.model_dump(), ensure_ascii=False),
                "created_at": report.created_at,
                "updated_at": report.updated_at,
            },
            "report_id",
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
                "root_document_id": document.root_document_id or document.document_id,
                "parent_document_id": document.parent_document_id,
                "payload_json": json.dumps(document.model_dump(exclude_none=True), ensure_ascii=False),
                "created_at": document.created_at,
                "updated_at": document.updated_at,
            },
            "document_id",
        )


# Legacy compatibility: export the ConstraintEngine at module level
# This ensures existing imports continue to work
__all__ = ["ConstraintEngine"]
