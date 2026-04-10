"""Recovery, handoff, and review types."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import AgentRole, ReviewDecision


class RecoveryEvent(BaseModel):
    """Event during recovery handling."""
    recovery_id: str
    kind: str
    summary: str
    created_at: str


class HandoffPacket(BaseModel):
    """Packet for handing off between agent roles."""
    id: str
    from_role: AgentRole
    to_role: AgentRole
    mission_id: Optional[str] = None
    run_id: str
    task_node_id: str
    summary: str
    artifacts: List[str] = Field(default_factory=list)
    context_refs: List[str] = Field(default_factory=list)
    required_action: str
    open_questions: List[str] = Field(default_factory=list)
    created_at: str


class ReviewVerdict(BaseModel):
    """Verdict from review process."""
    id: str
    run_id: str
    task_node_id: str
    role: AgentRole
    decision: ReviewDecision
    summary: str
    repair_requested: bool = False
    created_at: str


class MissionPhaseSnapshot(BaseModel):
    """Snapshot of mission phase."""
    run_id: str
    phase: str
    active_roles: List[AgentRole] = Field(default_factory=list)
    pending_handoffs: List[str] = Field(default_factory=list)
    updated_at: str
