"""Knowledge search and indexing types."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import KnowledgeSourceType, KnowledgeReindexScope


class KnowledgeSearchHit(BaseModel):
    """A single hit from knowledge search."""
    chunk_id: str
    source_type: KnowledgeSourceType
    source_ref: str
    title: str
    snippet: str
    score: float
    metadata: Dict[str, Any] = Field(default_factory=dict)


class KnowledgeIndexStatus(BaseModel):
    """Status of knowledge index."""
    ready: bool
    document_count: int = 0
    chunk_count: int = 0
    last_indexed_at: Optional[str] = None
    fallback_mode: bool = True
    model_name: Optional[str] = None


class KnowledgeSearchResult(BaseModel):
    """Result of knowledge search."""
    query: str
    top_k: int = 5
    path_hint: Optional[str] = None
    source_types: List[KnowledgeSourceType] = Field(default_factory=list)
    hits: List[KnowledgeSearchHit] = Field(default_factory=list)
    used_fallback: bool = True
    source_coverage: Dict[str, int] = Field(default_factory=dict)
    status: KnowledgeIndexStatus


class KnowledgeSearchRequest(BaseModel):
    """Request to search knowledge."""
    query: str
    top_k: int = 5
    path_hint: Optional[str] = None
    source_types: List[KnowledgeSourceType] = Field(default_factory=list)


class KnowledgeReindexRequest(BaseModel):
    """Request to reindex knowledge."""
    scope: KnowledgeReindexScope = "all"
