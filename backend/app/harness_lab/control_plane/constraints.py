"""Constraints API routes."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..bootstrap import harness_lab_services
from ..types import (
    ConstraintCreateRequest,
    ConstraintScenarioCreateRequest,
    ConstraintValidateRequest,
    ConstraintVerifyRequest,
)

router = APIRouter(prefix="/api/constraints", tags=["constraints"])


class ConstraintReviseRequest(BaseModel):
    """Request to revise a constraint document."""
    title: Optional[str] = None
    body: Optional[str] = None


@router.get("")
async def list_constraints():
    """List all constraint documents."""
    return {
        "success": True,
        "data": [
            document.model_dump() 
            for document in harness_lab_services.constraint_engine.list_documents()
        ],
    }


@router.post("")
async def create_constraint(request: ConstraintCreateRequest):
    """Create a new constraint document.
    
    The document will be automatically parsed and compiled into
    executable rules.
    """
    document = harness_lab_services.constraint_engine.create_document(request)
    return {
        "success": True,
        "data": document.model_dump(),
    }


@router.get("/scenarios")
async def list_constraint_scenarios(root_document_id: Optional[str] = None):
    scenarios = harness_lab_services.constraint_engine.list_scenarios(root_document_id=root_document_id)
    return {
        "success": True,
        "data": [scenario.model_dump() for scenario in scenarios],
    }


@router.post("/scenarios")
async def create_constraint_scenario(request: ConstraintScenarioCreateRequest):
    scenario = harness_lab_services.constraint_engine.create_scenario(request)
    return {
        "success": True,
        "data": scenario.model_dump(),
    }


@router.get("/{document_id}")
async def get_constraint(document_id: str):
    """Get a constraint document with compilation summary.
    
    Returns the document along with compilation status and rule count.
    """
    try:
        result = harness_lab_services.constraint_engine.get_document_with_summary(document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    
    return {
        "success": True,
        "data": result,
    }


@router.post("/{document_id}/publish")
async def publish_constraint(document_id: str):
    """Publish a constraint document.
    
    Published documents are active and will be used for verification.
    The document will be recompiled upon publishing.
    """
    try:
        document = harness_lab_services.constraint_engine.publish(document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    
    return {
        "success": True,
        "data": document.model_dump(),
    }


@router.post("/{document_id}/archive")
async def archive_constraint(document_id: str):
    """Archive a constraint document.
    
    Archived documents are inactive and will not be used for verification.
    """
    try:
        document = harness_lab_services.constraint_engine.archive(document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    
    return {
        "success": True,
        "data": document.model_dump(),
    }


@router.post("/verify")
async def verify_constraint(request: ConstraintVerifyRequest):
    """Verify constraints against a tool invocation.
    
    Returns detailed verdicts along with:
    - final_verdict: The consolidated decision
    - explanation: Detailed breakdown of rule matching
    - compiled_rule_count: Number of rules in the constraint set
    - used_fallback: Whether fallback logic was used
    - matched_rules: List of rules that matched
    
    The final decision follows deny-before-allow semantics:
    deny > approval_required > allow
    """
    response = harness_lab_services.constraint_engine.verify(
        subject=request.subject,
        payload=request.payload,
        constraint_set_id=request.constraint_set_id,
    )
    
    return {
        "success": True,
        "data": {
            "constraint_document_id": response.constraint_document_id,
            "constraint_root_document_id": response.constraint_root_document_id,
            "constraint_document_version": response.constraint_document_version,
            "verdicts": [verdict.model_dump() for verdict in response.verdicts],
            "final_verdict": response.final_verdict.model_dump(),
            "explanation": response.explanation.model_dump(),
            "compiled_rule_count": response.compiled_rule_count,
            "used_fallback": response.used_fallback,
            "matched_rules": [rule.model_dump() for rule in response.matched_rules],
        },
    }


@router.get("/{document_id}/explanation")
async def explain_constraint(document_id: str):
    """Get a human-readable explanation of a constraint document.
    
    This endpoint provides insight into how the document was parsed
    and what rules were compiled.
    """
    try:
        result = harness_lab_services.constraint_engine.get_document_with_summary(document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    
    document = result["document"]
    summary = result["compilation_summary"]
    
    # Build human-readable explanation
    explanation_parts = [
        f"Constraint Document: {document['title']}",
        f"Status: {document['status']}",
        f"Scope: {document['scope']}",
        f"Tags: {', '.join(document['tags']) or 'none'}",
        "",
        "Compilation Status:",
        f"  - Status: {summary['status']}",
        f"  - Rules Compiled: {summary['rule_count']}",
        f"  - Used Fallback: {summary['used_fallback']}",
    ]
    
    if summary['errors']:
        explanation_parts.append("  - Compilation Errors:")
        for error in summary['errors']:
            explanation_parts.append(f"    * {error}")
    
    explanation_parts.extend([
        "",
        "Document Body:",
        document['body'],
    ])
    
    return {
        "success": True,
        "data": {
            "document_id": document_id,
            "explanation": "\n".join(explanation_parts),
            "compilation_summary": summary,
        },
    }


@router.post("/{document_id}/revise")
async def revise_constraint(document_id: str, request: ConstraintReviseRequest):
    """Create a revision of a constraint document.
    
    This creates a new candidate version with:
    - Same root_document_id as the original
    - parent_document_id pointing to the original
    - version incremented
    - status set to candidate
    """
    try:
        document = harness_lab_services.constraint_engine.revise(
            document_id=document_id,
            title=request.title,
            body=request.body,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    
    return {
        "success": True,
        "data": document.model_dump(),
    }


@router.get("/{document_id}/versions")
async def list_constraint_versions(document_id: str):
    """List all versions in the constraint document chain.
    
    Returns documents ordered by version number, from oldest to newest.
    """
    try:
        document = harness_lab_services.constraint_engine.get_document(document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    
    versions = harness_lab_services.constraint_engine.list_versions(document.root_document_id)
    
    return {
        "success": True,
        "data": [doc.model_dump() for doc in versions],
        "root_document_id": document.root_document_id,
        "version_count": len(versions),
    }


@router.get("/{document_id}/diff")
async def diff_constraint(document_id: str, against: str = Query(..., description="Document ID to compare against")):
    """Compare two constraint documents.
    
    Returns diff information including:
    - Body changes (added/removed lines)
    - Compilation summary changes
    - Metadata changes
    """
    try:
        diff_result = harness_lab_services.constraint_engine.diff_documents(document_id, against)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    
    return {
        "success": True,
        "data": diff_result,
    }


@router.post("/{document_id}/validate")
async def validate_constraint(document_id: str, request: ConstraintValidateRequest):
    try:
        report = harness_lab_services.constraint_engine.validate_document(document_id, request)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "success": True,
        "data": report.model_dump(),
    }


@router.get("/{document_id}/gate")
async def constraint_publish_gate(document_id: str):
    try:
        gate = harness_lab_services.constraint_engine.get_publish_gate(document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "success": True,
        "data": gate.model_dump(),
    }


@router.post("/{document_id}/publish-with-archive")
async def publish_with_archive_constraint(document_id: str):
    """Publish a constraint document and archive previous published versions.
    
    When publishing a new version in a chain, any previously published
    version in the same chain will be automatically archived.
    This is the recommended way to publish constraint revisions.
    """
    try:
        document = harness_lab_services.constraint_engine.publish_with_archive(document_id)
        gate = harness_lab_services.constraint_engine.get_publish_gate(document_id)
    except ValueError as exc:
        detail = str(exc)
        if "publish blocked" in detail:
            raise HTTPException(status_code=409, detail=detail) from exc
        raise HTTPException(status_code=404, detail=detail) from exc
    
    return {
        "success": True,
        "data": {
            "document": document.model_dump(),
            "gate": gate.model_dump(),
        },
    }
