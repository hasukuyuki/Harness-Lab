"""Constraints API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..bootstrap import harness_lab_services
from ..types import ConstraintCreateRequest, ConstraintVerifyRequest

router = APIRouter(prefix="/api/constraints", tags=["constraints"])


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
