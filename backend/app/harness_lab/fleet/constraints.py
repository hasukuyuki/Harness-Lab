"""Dispatch constraint calculation - extracted from RuntimeService.

This module consolidates dispatch constraint, worker matching, and blocker
calculation logic, making it independent of RuntimeService private methods.

Design:
    - DispatchConstraintCalculator owns constraint/matching/blocker logic
    - Depends on ToolGateway for sandbox/risk-level queries
    - Used by fleet/adapters.py to implement DispatchConstraintProtocol
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..types import DispatchConstraint, ResearchRun, ResearchSession, TaskNode, WorkerSnapshot

if TYPE_CHECKING:
    from ..boundary.gateway import ToolGateway


class DispatchConstraintCalculator:
    """Calculates dispatch constraints and worker matching.
    
    This class extracts the constraint calculation logic from RuntimeService,
    making it a fleet-layer capability that can be used without depending
    on RuntimeService private methods.
    """
    
    def __init__(
        self,
        tool_gateway: ToolGateway,
        worker_registry,
    ) -> None:
        """Initialize constraint calculator.
        
        Args:
            tool_gateway: ToolGateway for sandbox/risk-level queries
            worker_registry: WorkerRegistry for worker listing
        """
        self.tool_gateway = tool_gateway
        self.worker_registry = worker_registry
    
    def constraint_for_node(
        self,
        session: ResearchSession,
        node: TaskNode,
    ) -> DispatchConstraint:
        """Calculate dispatch constraints for a task node.
        
        Args:
            session: ResearchSession containing intent and execution mode
            node: TaskNode to calculate constraints for
            
        Returns:
            DispatchConstraint with queue shard, labels, and requirements
        """
        metadata = node.metadata or {}
        required_capabilities: List[str] = []
        
        if node.kind == "execution":
            required_capabilities = [session.intent_declaration.suggested_action.tool_name]

        tool_name = required_capabilities[0] if required_capabilities else None
        risk_level = self._tool_risk_level(tool_name) if tool_name else "low"
        
        required_labels = [str(item) for item in metadata.get("required_labels", [])]
        preferred_labels = [str(item) for item in metadata.get("preferred_labels", [])]
        
        # Add role-based preferred labels
        if tool_name == "knowledge_search":
            preferred_labels = list(dict.fromkeys([*preferred_labels, "knowledge"]))
        if node.agent_role == "executor" and risk_level in {"medium", "high"}:
            preferred_labels = list(dict.fromkeys([*preferred_labels, "executor"]))
        if node.agent_role == "researcher":
            preferred_labels = list(dict.fromkeys([*preferred_labels, "research"]))
        
        # Determine sandbox requirement
        requires_sandbox = (
            self.tool_gateway.requires_sandbox(session.intent_declaration.suggested_action)
            if node.kind == "execution"
            else False
        )
        
        # Build queue shard
        queue_parts = [node.agent_role, risk_level]
        queue_parts.extend(required_labels or ["unlabeled"])
        
        return DispatchConstraint(
            agent_role=node.agent_role,
            required_capabilities=required_capabilities,
            required_labels=required_labels,
            preferred_labels=preferred_labels,
            execution_mode="remote_http" if session.execution_mode == "remote_worker" else None,
            requires_sandbox=requires_sandbox,
            risk_level=risk_level,
            queue_shard="/".join(queue_parts),
        )
    
    def worker_matches_node(
        self,
        worker: WorkerSnapshot,
        session: ResearchSession,
        node: TaskNode,
    ) -> bool:
        """Check if worker can execute the given node.
        
        Args:
            worker: WorkerSnapshot to check
            session: ResearchSession for constraint calculation
            node: TaskNode to match against
            
        Returns:
            True if worker can execute the node, False otherwise
        """
        # Draining workers don't match
        if getattr(worker, "drain_state", "active") == "draining":
            return False
        
        constraints = self.constraint_for_node(session, node)
        
        # Check role profile
        if getattr(worker, "role_profile", None) and worker.role_profile != node.agent_role:
            return False
        
        # Check execution mode
        if constraints.execution_mode and getattr(worker, "execution_mode", None) not in {constraints.execution_mode, "embedded"}:
            return False
        
        # Check sandbox requirement
        if constraints.requires_sandbox and not getattr(worker, "sandbox_ready", False):
            return False
        
        # Check required labels
        worker_labels = set(getattr(worker, "labels", []) or [])
        if any(label not in worker_labels for label in constraints.required_labels):
            return False
        
        # Check required capabilities
        if constraints.required_capabilities and any(
            capability not in (worker.capabilities or [])
            for capability in constraints.required_capabilities
        ):
            return False
        
        return True
    
    def worker_sort_key(
        self,
        worker: WorkerSnapshot,
        session: ResearchSession,
        node: TaskNode,
    ) -> tuple[int, int, str]:
        """Generate sort key for worker prioritization.
        
        Args:
            worker: WorkerSnapshot to sort
            session: ResearchSession for constraint calculation
            node: TaskNode to match against
            
        Returns:
            Tuple for sorting: (-preferred_hits, current_load, worker_id)
        """
        constraints = self.constraint_for_node(session, node)
        preferred_hits = sum(
            1 for label in constraints.preferred_labels
            if label in (worker.labels or [])
        )
        current_load = int(getattr(worker, "lease_count", 0) or 0)
        return (-preferred_hits, current_load, worker.worker_id)
    
    def dispatch_blockers_for_run(
        self,
        run: ResearchRun,
        session: ResearchSession,
    ) -> List[Dict[str, Any]]:
        """List reasons why ready nodes cannot be dispatched.
        
        Args:
            run: ResearchRun to check
            session: ResearchSession containing task graph
            
        Returns:
            List of blocker dictionaries with task_node_id, kind, and requirements
        """
        task_graph = session.task_graph
        if not task_graph:
            return []
        
        ready_nodes = [node for node in task_graph.nodes if node.status == "ready"]
        if not ready_nodes:
            return []
        
        workers = self.worker_registry.list_workers()
        blockers: List[Dict[str, Any]] = []
        
        for node in ready_nodes:
            constraints = self.constraint_for_node(session, node)
            role_workers = [
                worker for worker in workers
                if not worker.role_profile or worker.role_profile == node.agent_role
            ]
            matching_workers = [
                worker for worker in workers
                if self.worker_matches_node(worker, session, node)
            ]
            
            blocker = None
            
            if not role_workers:
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "no_role_worker",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif constraints.requires_sandbox and not any(worker.sandbox_ready for worker in role_workers):
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "awaiting_sandbox_ready_worker",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif constraints.required_labels and not any(
                all(label in (worker.labels or []) for label in constraints.required_labels)
                for worker in role_workers
            ):
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "awaiting_required_label",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif role_workers and not matching_workers and all(worker.drain_state == "draining" for worker in role_workers):
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "all_matching_workers_draining",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            elif role_workers and not matching_workers:
                blocker = {
                    "task_node_id": node.node_id,
                    "kind": "no_matching_worker",
                    "agent_role": node.agent_role,
                    "required_labels": constraints.required_labels,
                    "required_capabilities": constraints.required_capabilities,
                }
            
            if blocker:
                blocker["queue_shard"] = constraints.queue_shard
                blockers.append(blocker)
        
        return blockers

    def _tool_risk_level(self, tool_name: Optional[str]) -> str:
        """Resolve tool risk level without relying on runtime-private helpers."""
        if not tool_name:
            return "low"

        direct = getattr(self.tool_gateway, "risk_level", None)
        if callable(direct):
            try:
                return str(direct(tool_name))
            except Exception:
                pass

        list_tools = getattr(self.tool_gateway, "list_tools", None)
        if callable(list_tools):
            for tool in list_tools():
                if getattr(tool, "name", None) == tool_name:
                    return str(getattr(tool, "risk_level", "unknown"))

        return "unknown"
