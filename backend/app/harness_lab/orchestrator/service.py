from __future__ import annotations

from typing import Dict, List

from ..types import IntentDeclaration, ResearchSession, TaskEdge, TaskGraph, TaskNode, WorkflowTemplateVersion
from ..utils import new_id


class OrchestratorService:
    """DAG-native orchestrator that currently executes in a single-worker wave."""

    def build_task_graph(
        self,
        session: ResearchSession,
        intent: IntentDeclaration,
        workflow: WorkflowTemplateVersion | None = None,
    ) -> TaskGraph:
        if workflow and workflow.dag.get("nodes"):
            nodes_by_key = {}
            nodes = []
            for index, node_spec in enumerate(workflow.dag.get("nodes", [])):
                key = str(node_spec.get("key") or f"node_{index}")
                label = str(node_spec.get("label") or key)
                if key == "execute":
                    label = f"Execute {intent.suggested_action.tool_name}"
                node = TaskNode(
                    node_id=new_id("node"),
                    label=label,
                    kind=str(node_spec.get("kind") or "task"),
                    role=str(node_spec.get("role") or "executor"),
                    agent_role=str(node_spec.get("role") or "executor"),
                    metadata={
                        "key": key,
                        "sequence": index,
                        "session_id": session.session_id,
                        "workflow_template_id": workflow.workflow_id,
                        "suggested_tool": intent.suggested_action.tool_name if key == "execute" else None,
                        "gates": workflow.gates,
                    },
                )
                nodes.append(node)
                nodes_by_key[key] = node
            edges = []
            for edge_spec in workflow.dag.get("edges", []):
                source = nodes_by_key.get(str(edge_spec.get("source")))
                target = nodes_by_key.get(str(edge_spec.get("target")))
                if not source or not target:
                    continue
                edge_kind = str(edge_spec.get("kind") or "depends_on")
                if edge_kind in {"depends_on", "handoff"}:
                    target.dependencies.append(source.node_id)
                edges.append(
                    TaskEdge(
                        edge_id=new_id("edge"),
                        source=source.node_id,
                        target=target.node_id,
                        kind=edge_kind,
                    )
                )
            return TaskGraph(task_graph_id=new_id("graph"), nodes=nodes, edges=edges, execution_strategy="multi_agent_wave_ready")

        node_specs = [
            ("intent", "Declare intent", "intent", "planner"),
            ("context", "Assemble layered context", "context", "planner"),
            ("prompt", "Render structured prompt frame", "prompt", "planner"),
            ("policy", "Run constraint preflight", "policy", "reviewer"),
            ("execute", f"Execute {intent.suggested_action.tool_name}", "execution", "executor"),
            ("verify", "Verify trace and outcome", "verification", "reviewer"),
            ("learn", "Write research learning artifacts", "learning", "recovery"),
        ]
        nodes = [
            TaskNode(
                node_id=new_id("node"),
                label=label,
                kind=kind,
                role=role,
                agent_role=role,
                metadata={
                    "sequence": index,
                    "session_id": session.session_id,
                    "suggested_tool": intent.suggested_action.tool_name if key == "execute" else None,
                },
            )
            for index, (key, label, kind, role) in enumerate(node_specs)
        ]
        edges = []
        for left, right in zip(nodes, nodes[1:]):
            right.dependencies.append(left.node_id)
            edges.append(TaskEdge(edge_id=new_id("edge"), source=left.node_id, target=right.node_id))
        return TaskGraph(task_graph_id=new_id("graph"), nodes=nodes, edges=edges, execution_strategy="single_worker_wave_ready")

    def next_wave(self, task_graph: TaskGraph) -> List[TaskNode]:
        nodes_by_id = {node.node_id: node for node in task_graph.nodes}
        ready: List[TaskNode] = []
        for node in task_graph.nodes:
            if node.status not in {"planned", "ready"}:
                continue
            inbound_edges = [edge for edge in task_graph.edges if edge.target == node.node_id]
            if not inbound_edges:
                ready.append(node)
                continue
            if all(self._edge_satisfied(edge, nodes_by_id) for edge in inbound_edges):
                ready.append(node)
        return ready

    def mark_node_status(
        self,
        task_graph: TaskGraph,
        node_id: str,
        status: str,
        metadata: Dict[str, object] | None = None,
    ) -> TaskNode:
        node = self._node_by_id(task_graph, node_id)
        node.status = status
        if metadata:
            node.metadata.update(metadata)
        return node

    def skip_unreachable_nodes(self, task_graph: TaskGraph) -> List[TaskNode]:
        nodes_by_id = {node.node_id: node for node in task_graph.nodes}
        skipped: List[TaskNode] = []
        for node in task_graph.nodes:
            if node.status not in {"planned", "ready"}:
                continue
            inbound_edges = [edge for edge in task_graph.edges if edge.target == node.node_id]
            if not inbound_edges:
                continue
            if all(self._edge_unreachable(edge, nodes_by_id) for edge in inbound_edges):
                node.status = "skipped"
                node.metadata["skip_reason"] = "upstream_condition_not_met"
                skipped.append(node)
        return skipped

    def is_terminal(self, task_graph: TaskGraph) -> bool:
        return all(node.status in {"completed", "failed", "skipped", "blocked"} for node in task_graph.nodes)

    def has_failed_nodes(self, task_graph: TaskGraph) -> bool:
        return any(node.status == "failed" for node in task_graph.nodes)

    def has_node_kind(self, task_graph: TaskGraph, kind: str) -> bool:
        return any(node.kind == kind for node in task_graph.nodes)

    @staticmethod
    def _edge_satisfied(edge: TaskEdge, nodes_by_id: Dict[str, TaskNode]) -> bool:
        source = nodes_by_id[edge.source]
        if edge.kind == "on_failure":
            return source.status == "failed"
        if edge.kind == "handoff":
            return source.status in {"completed", "failed", "skipped"}
        return source.status == "completed"

    @staticmethod
    def _edge_unreachable(edge: TaskEdge, nodes_by_id: Dict[str, TaskNode]) -> bool:
        source = nodes_by_id[edge.source]
        if edge.kind == "on_failure":
            return source.status in {"completed", "skipped"}
        if edge.kind == "handoff":
            return False
        return source.status in {"failed", "skipped"}

    @staticmethod
    def _node_by_id(task_graph: TaskGraph, node_id: str) -> TaskNode:
        for node in task_graph.nodes:
            if node.node_id == node_id:
                return node
        raise ValueError(f"Task node not found: {node_id}")
