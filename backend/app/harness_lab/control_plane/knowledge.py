from __future__ import annotations

from fastapi import APIRouter

from ..bootstrap import harness_lab_services
from ..types import KnowledgeReindexRequest, KnowledgeSearchRequest

router = APIRouter(prefix="/api/knowledge", tags=["knowledge"])


@router.post("/search")
async def search_knowledge(request: KnowledgeSearchRequest):
    result = harness_lab_services.knowledge.search(
        query=request.query,
        top_k=request.top_k,
        path_hint=request.path_hint,
        source_types=request.source_types,
    )
    return {"success": True, "data": result.model_dump()}


@router.post("/reindex")
async def reindex_knowledge(request: KnowledgeReindexRequest):
    status = harness_lab_services.knowledge.reindex(scope=request.scope)
    return {"success": True, "data": status.model_dump()}
