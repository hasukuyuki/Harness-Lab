from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..bootstrap import harness_lab_services
from ..types import (
    CanaryStartRequest,
    CanaryPromoteRequest,
    CanaryRollbackRequest,
    ImprovementDiagnoseRequest,
    PolicyCandidateRequest,
    WorkflowCandidateRequest,
)

router = APIRouter(prefix="/api", tags=["candidates"])


@router.get("/candidates")
async def list_candidates():
    return {"success": True, "data": [candidate.model_dump() for candidate in harness_lab_services.improvement.list_candidates()]}


@router.get("/candidates/{candidate_id}/gate")
async def get_candidate_gate(candidate_id: str):
    try:
        gate = harness_lab_services.improvement.get_candidate_gate(candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"success": True, "data": gate.model_dump()}


@router.post("/improvement/candidates/policy")
async def create_policy_candidate(request: PolicyCandidateRequest):
    payload = harness_lab_services.improvement.create_policy_candidate(
        policy_id=request.policy_id,
        trace_refs=request.trace_refs,
        rationale=request.rationale,
    )
    return {
        "success": True,
        "data": {
            "candidate": payload["candidate"].model_dump(),
            "version": payload["version"].model_dump(),
            "observations": payload["observations"],
            "diagnosis": payload["diagnosis"].model_dump(),
            "evaluations": [item.model_dump() for item in payload["evaluations"]],
            "gate": payload["gate"].model_dump(),
        },
    }


@router.post("/improvement/candidates/workflow")
async def create_workflow_candidate(request: WorkflowCandidateRequest):
    payload = harness_lab_services.improvement.create_workflow_candidate(
        workflow_id=request.workflow_id,
        trace_refs=request.trace_refs,
        rationale=request.rationale,
    )
    return {
        "success": True,
        "data": {
            "candidate": payload["candidate"].model_dump(),
            "version": payload["version"].model_dump(),
            "observations": payload["observations"],
            "diagnosis": payload["diagnosis"].model_dump(),
            "evaluations": [item.model_dump() for item in payload["evaluations"]],
            "gate": payload["gate"].model_dump(),
        },
    }


@router.post("/improvement/diagnose")
async def diagnose_improvement(request: ImprovementDiagnoseRequest):
    report = harness_lab_services.improvement.diagnose(trace_refs=request.trace_refs)
    return {"success": True, "data": report.model_dump()}


@router.post("/candidates/{candidate_id}/approve")
async def approve_candidate(candidate_id: str):
    try:
        candidate = harness_lab_services.improvement.approve_candidate(candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"success": True, "data": candidate.model_dump()}


@router.post("/candidates/{candidate_id}/publish")
async def publish_candidate(candidate_id: str):
    try:
        candidate = harness_lab_services.improvement.publish_candidate(candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "data": candidate.model_dump()}


@router.post("/candidates/{candidate_id}/rollback")
async def rollback_candidate(candidate_id: str):
    try:
        candidate = harness_lab_services.improvement.rollback_candidate(candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "data": candidate.model_dump()}


# =============================================================================
# Canary Rollout Endpoints
# =============================================================================

@router.get("/candidates/{candidate_id}/rollout")
async def get_rollout_status(candidate_id: str):
    """Get detailed rollout status for a candidate."""
    try:
        status = harness_lab_services.improvement.get_rollout_status(candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"success": True, "data": status.model_dump()}


@router.post("/candidates/{candidate_id}/canary")
async def start_canary(candidate_id: str, request: CanaryStartRequest):
    """Start canary rollout for a candidate.
    
    Args:
        candidate_id: The candidate to start canary for
        request: Canary configuration (scope type, value, description)
        
    Scope types:
        - percentage: "10" means 10% of traffic
        - session_tag: match sessions with specific tag
        - worker_label: match workers with specific label
        - goal_pattern: regex match against session goal
        - explicit_override: only explicit canary override
    """
    try:
        from ..types import CanaryScope
        scope = CanaryScope(
            scope_type=request.scope_type,
            scope_value=request.scope_value,
            description=request.description,
        )
        candidate = harness_lab_services.improvement.start_canary(candidate_id, scope)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "data": candidate.model_dump()}


@router.post("/candidates/{candidate_id}/promote")
async def promote_canary(candidate_id: str, request: CanaryPromoteRequest):
    """Promote canary candidate to full published status.
    
    Args:
        candidate_id: The candidate to promote
        request: Promote options (force to skip safety checks)
        
    Promote requirements:
        - Candidate must be in canary status
        - Minimum canary sample size (default: 10 runs)
        - No success rate regression > 10%
        - No safety score regression > 5%
        - Workflow candidates require human approval
    """
    try:
        candidate = harness_lab_services.improvement.promote_canary(
            candidate_id, force=request.force
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"success": True, "data": candidate.model_dump()}
