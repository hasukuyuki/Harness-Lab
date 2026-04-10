from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..bootstrap import harness_lab_services
from ..types import ImprovementDiagnoseRequest, PolicyCandidateRequest, WorkflowCandidateRequest

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
