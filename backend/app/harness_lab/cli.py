from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[3] / ".env")

from backend.app.harness_lab.bootstrap import harness_lab_services  # noqa: E402
from backend.app.harness_lab.types import (  # noqa: E402
    KnowledgeReindexRequest,
    KnowledgeSearchRequest,
    RunRequest,
    SessionRequest,
    WorkerRegisterRequest,
)
from backend.app.harness_lab.fleet.worker_registry import WorkerRegistry  # noqa: E402
from backend.app.harness_lab.workers.runtime_client import WorkerExecutionLoop, WorkerRuntimeClient  # noqa: E402


def _emit(payload: Any, output_format: str = "text") -> None:
    if output_format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if isinstance(payload, dict):
        for key, value in payload.items():
            print(f"{key}: {value}")
        return
    if isinstance(payload, list):
        for item in payload:
            print(item)
        return
    print(payload)


def _latest_run_id() -> str | None:
    runs = harness_lab_services.runtime.list_runs(limit=1)
    return runs[0].run_id if runs else None


def _latest_session_id() -> str | None:
    sessions = harness_lab_services.runtime.list_sessions(limit=1)
    return sessions[0].session_id if sessions else None


def _default_control_plane_url() -> str:
    configured = os.getenv("HARNESS_CONTROL_PLANE_URL", "").strip()
    if configured:
        return configured.rstrip("/")
    port = os.getenv("PORT", "4600").strip() or "4600"
    return f"http://127.0.0.1:{port}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hlab", description="Harness Lab CLI control surface")
    parser.add_argument("--output-format", choices=["text", "json"], default="text")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit = subparsers.add_parser("submit", help="Create a session and execute a run")
    submit.add_argument("goal")
    submit.add_argument("--path", dest="path_hint", default="")
    submit.add_argument("--shell", dest="shell_command", default="")
    submit.add_argument("--execution-mode", default="single_worker")

    subparsers.add_parser("doctor", help="Run local control-plane diagnostics")
    subparsers.add_parser("diagnose", help="Summarize multi-agent failure clusters and blockers")
    subparsers.add_parser("fleet", help="Inspect fleet status")

    queue = subparsers.add_parser("queue", help="Inspect dispatch queues")
    queue_subparsers = queue.add_subparsers(dest="queue_command", required=True)
    queue_subparsers.add_parser("inspect", help="Inspect ready-queue shards")

    knowledge = subparsers.add_parser("knowledge", help="Index and search the knowledge runtime")
    knowledge_subparsers = knowledge.add_subparsers(dest="knowledge_command", required=True)

    knowledge_reindex = knowledge_subparsers.add_parser("reindex", help="Rebuild the knowledge index")
    knowledge_reindex.add_argument("--scope", choices=["workspace", "docs", "artifacts", "all"], default="all")
    knowledge_reindex.add_argument("--control-plane-url", default="")

    knowledge_search = knowledge_subparsers.add_parser("search", help="Search the knowledge index")
    knowledge_search.add_argument("query")
    knowledge_search.add_argument("--top-k", type=int, default=5)
    knowledge_search.add_argument("--path-hint", default="")
    knowledge_search.add_argument("--source-type", action="append", default=[])
    knowledge_search.add_argument("--control-plane-url", default="")

    attach = subparsers.add_parser("attach", help="Inspect the latest or requested run")
    attach.add_argument("--run-id", default="")

    eval_cmd = subparsers.add_parser("eval", help="Run offline replay or benchmark evaluation")
    eval_cmd.add_argument("--suite", choices=["replay", "benchmark"], default="replay")
    eval_cmd.add_argument("--candidate-id", default="")
    eval_cmd.add_argument("--trace-ref", action="append", default=[])

    subparsers.add_parser("candidates", help="List improvement candidates")

    promote = subparsers.add_parser("promote", help="Publish or promote a candidate")
    promote.add_argument("candidate_id")
    promote.add_argument("--from-canary", action="store_true", help="Promote from canary to published")
    promote.add_argument("--force", action="store_true", help="Force promotion (skip safety checks)")

    rollback = subparsers.add_parser("rollback", help="Rollback a candidate")
    rollback.add_argument("candidate_id")
    rollback.add_argument("--reason", default="", help="Reason for rollback")

    # Canary rollout commands
    canary = subparsers.add_parser("canary", help="Manage canary rollouts")
    canary_subparsers = canary.add_subparsers(dest="canary_command", required=True)

    canary_start = canary_subparsers.add_parser("start", help="Start canary rollout for a candidate")
    canary_start.add_argument("candidate_id")
    canary_start.add_argument("--scope-type", default="percentage", 
                              choices=["percentage", "session_tag", "worker_label", "goal_pattern", "explicit_override"],
                              help="Type of canary scope")
    canary_start.add_argument("--scope-value", default="10", help="Scope value (e.g., '10' for 10%)")
    canary_start.add_argument("--description", default="", help="Description of the canary scope")

    canary_subparsers.add_parser("status", help="List canary rollout status for all candidates")

    canary_promote = canary_subparsers.add_parser("promote", help="Promote canary to full published status")
    canary_promote.add_argument("candidate_id")
    canary_promote.add_argument("--force", action="store_true", help="Force promotion (skip safety checks)")

    subparsers.add_parser("approvals", help="List approval inbox")
    leases = subparsers.add_parser("leases", help="List worker leases")
    leases.add_argument("--status", default="")
    leases.add_argument("--worker-id", default="")

    workers = subparsers.add_parser("workers", help="List or register workers")
    workers.add_argument("--register", action="store_true")
    workers.add_argument("--label", default="cli-worker")
    workers.add_argument("--capability", action="append", default=[])
    workers.add_argument("--role-profile", default="")

    worker = subparsers.add_parser("worker", help="Operate a remote-style worker daemon")
    worker_subparsers = worker.add_subparsers(dest="worker_command", required=True)

    worker_register = worker_subparsers.add_parser("register", help="Register a worker")
    worker_register.add_argument("--worker-id", default="")
    worker_register.add_argument("--label", default="cli-worker")
    worker_register.add_argument("--capability", action="append", default=[])
    worker_register.add_argument("--role-profile", default="")
    worker_register.add_argument("--control-plane-url", default="")

    worker_status = worker_subparsers.add_parser("status", help="Inspect worker status")
    worker_status.add_argument("--worker-id", default="")
    worker_status.add_argument("--control-plane-url", default="")

    worker_drain = worker_subparsers.add_parser("drain", help="Drain a worker without taking new tasks")
    worker_drain.add_argument("worker_id")
    worker_drain.add_argument("--reason", default="")
    worker_drain.add_argument("--control-plane-url", default="")

    worker_resume = worker_subparsers.add_parser("resume", help="Resume a drained worker")
    worker_resume.add_argument("worker_id")
    worker_resume.add_argument("--control-plane-url", default="")

    worker_serve = worker_subparsers.add_parser("serve", help="Run a polling worker daemon")
    worker_serve.add_argument("--worker-id", default="")
    worker_serve.add_argument("--label", default="cli-worker")
    worker_serve.add_argument("--capability", action="append", default=[])
    worker_serve.add_argument("--role-profile", default="")
    worker_serve.add_argument("--control-plane-url", default="")
    worker_serve.add_argument("--interval", type=float, default=1.0)
    worker_serve.add_argument("--once", action="store_true")
    worker_serve.add_argument("--max-tasks", type=int, default=1)

    sandbox = subparsers.add_parser("sandbox", help="Inspect local sandbox readiness")
    sandbox_subparsers = sandbox.add_subparsers(dest="sandbox_command", required=True)
    sandbox_subparsers.add_parser("probe", help="Probe Docker sandbox readiness")

    runs = subparsers.add_parser("runs", help="Inspect run execution state")
    run_subparsers = runs.add_subparsers(dest="runs_command", required=True)
    runs_watch = run_subparsers.add_parser("watch", help="Watch a run until it finishes")
    runs_watch.add_argument("--run-id", default="")
    runs_watch.add_argument("--interval", type=float, default=1.0)
    runs_watch.add_argument("--once", action="store_true")

    subparsers.add_parser("serve", help="Start the FastAPI control plane")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "doctor":
        _emit(harness_lab_services.doctor_report(), args.output_format)
        return

    if args.command == "fleet":
        _emit(harness_lab_services.runtime.fleet_status().model_dump(), args.output_format)
        return

    if args.command == "queue" and args.queue_command == "inspect":
        _emit([item.model_dump() for item in harness_lab_services.runtime.queue_status()], args.output_format)
        return

    if args.command == "diagnose":
        report = harness_lab_services.improvement.diagnose()
        _emit(report.model_dump(), args.output_format)
        return

    if args.command == "knowledge":
        if args.knowledge_command == "reindex":
            if args.control_plane_url.strip():
                client = WorkerRuntimeClient(args.control_plane_url.strip())
                payload = client.reindex_knowledge(KnowledgeReindexRequest(scope=args.scope))
                _emit(payload["data"], args.output_format)
                return
            status = harness_lab_services.knowledge.reindex(scope=args.scope)
            _emit(status.model_dump(), args.output_format)
            return

        if args.knowledge_command == "search":
            request = KnowledgeSearchRequest(
                query=args.query,
                top_k=max(1, args.top_k),
                path_hint=args.path_hint or None,
                source_types=args.source_type,
            )
            if args.control_plane_url.strip():
                client = WorkerRuntimeClient(args.control_plane_url.strip())
                payload = client.search_knowledge(request)
                _emit(payload["data"], args.output_format)
                return
            result = harness_lab_services.knowledge.search(
                query=request.query,
                top_k=request.top_k,
                path_hint=request.path_hint,
                source_types=request.source_types,
            )
            _emit(result.model_dump(), args.output_format)
            return

    if args.command == "submit":
        context: dict[str, Any] = {}
        if args.path_hint:
            context["path"] = args.path_hint
        if args.shell_command:
            context["shell_command"] = args.shell_command
        session = harness_lab_services.runtime.create_session(
            SessionRequest(
                goal=args.goal,
                context=context,
                execution_mode=args.execution_mode,
            )
        )
        run = asyncio.run(harness_lab_services.runtime.create_run(RunRequest(session_id=session.session_id)))
        _emit(
            {
                "session_id": session.session_id,
                "run_id": run.run_id,
                "status": run.status,
                "policy_id": run.policy_id,
                "workflow_template_id": run.workflow_template_id,
                "assigned_worker_id": run.assigned_worker_id,
            },
            args.output_format,
        )
        return

    if args.command == "attach":
        run_id = args.run_id or _latest_run_id()
        if not run_id:
            raise SystemExit("No run is available to attach to.")
        run = harness_lab_services.runtime.get_run(run_id)
        _emit(run.model_dump(), args.output_format)
        return

    if args.command == "eval":
        latest_run_id = _latest_run_id()
        trace_refs = args.trace_ref or ([] if args.candidate_id else [latest_run_id] if latest_run_id else [])
        report = harness_lab_services.improvement.evaluate_candidate(
            suite=args.suite,
            candidate_id=args.candidate_id or None,
            trace_refs=trace_refs,
        )
        payload = report.model_dump()
        if args.output_format == "text":
            payload = {
                "evaluation_id": report.evaluation_id,
                "suite": report.suite,
                "status": report.status,
                "handoff_success_rate": report.metrics.get("handoff_success_rate"),
                "review_reject_rate": report.metrics.get("review_reject_rate"),
                "repair_rate": report.metrics.get("repair_rate"),
                "role_utilization": report.metrics.get("role_utilization"),
                "cross_role_latency": report.metrics.get("cross_role_latency"),
                "coverage_gaps": report.coverage_gaps,
            }
        _emit(payload, args.output_format)
        return

    if args.command == "candidates":
        candidates = harness_lab_services.improvement.list_candidates()
        if args.output_format == "text":
            _emit(
                [
                    {
                        "candidate_id": item.candidate_id,
                        "kind": item.kind,
                        "publish_status": item.publish_status,
                        "eval_status": item.eval_status,
                        "cluster_count": (((item.metrics or {}).get("diagnosis") or {}).get("cluster_count")),
                        "proposal_summary": (item.metrics or {}).get("proposal_summary"),
                    }
                    for item in candidates
                ],
                args.output_format,
            )
            return
        _emit([item.model_dump() for item in candidates], args.output_format)
        return

    if args.command == "promote":
        try:
            if args.from_canary:
                candidate = harness_lab_services.improvement.promote_canary(args.candidate_id, force=args.force)
            else:
                candidate = harness_lab_services.improvement.publish_candidate(args.candidate_id)
        except ValueError as exc:
            try:
                gate = harness_lab_services.improvement.get_candidate_gate(args.candidate_id)
                payload = {"error": str(exc), "gate": gate.model_dump()}
            except ValueError:
                payload = {"error": str(exc)}
            _emit(payload, args.output_format)
            raise SystemExit(1) from exc
        _emit(candidate.model_dump(), args.output_format)
        return

    if args.command == "rollback":
        candidate = harness_lab_services.improvement.rollback_candidate(args.candidate_id)
        _emit(candidate.model_dump(), args.output_format)
        return

    if args.command == "canary":
        if args.canary_command == "start":
            from backend.app.harness_lab.types import CanaryScope
            scope = CanaryScope(
                scope_type=args.scope_type,
                scope_value=args.scope_value,
                description=args.description or f"Canary: {args.scope_type}={args.scope_value}",
            )
            try:
                candidate = harness_lab_services.improvement.start_canary(args.candidate_id, scope)
                _emit({
                    "candidate_id": candidate.candidate_id,
                    "publish_status": candidate.publish_status,
                    "rollout_ring": candidate.rollout_ring,
                    "rollout_scope": candidate.rollout_scope.model_dump() if candidate.rollout_scope else None,
                }, args.output_format)
            except ValueError as exc:
                _emit({"error": str(exc)}, args.output_format)
                raise SystemExit(1) from exc
            return

        if args.canary_command == "status":
            candidates = harness_lab_services.improvement.list_candidates()
            canary_candidates = [c for c in candidates if c.publish_status in {"canary", "published"} and c.rollout_ring]
            if args.output_format == "text":
                _emit(
                    [
                        {
                            "candidate_id": item.candidate_id,
                            "kind": item.kind,
                            "publish_status": item.publish_status,
                            "rollout_ring": item.rollout_ring,
                            "scope_type": item.rollout_scope.scope_type if item.rollout_scope else None,
                            "scope_value": item.rollout_scope.scope_value if item.rollout_scope else None,
                            "canary_sample_size": item.canary_metrics.canary_sample_size if item.canary_metrics else 0,
                            "sufficient_sample": item.canary_metrics.sufficient_sample if item.canary_metrics else False,
                            "regression_detected": item.canary_metrics.regression_detected if item.canary_metrics else False,
                        }
                        for item in canary_candidates
                    ],
                    args.output_format,
                )
            else:
                _emit([item.model_dump() for item in canary_candidates], args.output_format)
            return

        if args.canary_command == "promote":
            try:
                candidate = harness_lab_services.improvement.promote_canary(args.candidate_id, force=args.force)
                _emit({
                    "candidate_id": candidate.candidate_id,
                    "publish_status": candidate.publish_status,
                    "rollout_ring": candidate.rollout_ring,
                    "promoted_at": candidate.updated_at,
                }, args.output_format)
            except ValueError as exc:
                # Get rollout status for detailed blockers
                try:
                    status = harness_lab_services.improvement.get_rollout_status(args.candidate_id)
                    _emit({
                        "error": str(exc),
                        "blockers": status.blockers,
                        "canary_metrics": status.canary_metrics.model_dump() if status.canary_metrics else None,
                    }, args.output_format)
                except ValueError:
                    _emit({"error": str(exc)}, args.output_format)
                raise SystemExit(1) from exc
            return

    if args.command == "approvals":
        _emit([item.model_dump() for item in harness_lab_services.runtime.list_approvals()], args.output_format)
        return

    if args.command == "leases":
        _emit(
            [
                item.model_dump()
                for item in harness_lab_services.runtime.list_leases(
                    status=args.status or None,
                    worker_id=args.worker_id or None,
                )
            ],
            args.output_format,
        )
        return

    if args.command == "workers":
        if args.register:
            sandbox_status = harness_lab_services.sandbox.status()
            worker = harness_lab_services.runtime.worker_registry.register_worker(
                WorkerRegisterRequest(
                    label=args.label,
                    capabilities=args.capability,
                    role_profile=args.role_profile or None,
                    sandbox_backend=sandbox_status.sandbox_backend,
                    sandbox_ready=sandbox_status.docker_ready and sandbox_status.sandbox_image_ready,
                    version="v1",
                )
            )
            _emit(worker.model_dump(), args.output_format)
            return
        _emit([item.model_dump() for item in harness_lab_services.runtime.worker_registry.list_workers()], args.output_format)
        return

    if args.command == "sandbox" and args.sandbox_command == "probe":
        _emit(harness_lab_services.sandbox.status().model_dump(), args.output_format)
        return

    if args.command == "worker":
        if args.worker_command == "register":
            control_plane_url = args.control_plane_url.strip()
            if control_plane_url:
                client = WorkerRuntimeClient(control_plane_url)
                loop = WorkerExecutionLoop(client, poll_interval_seconds=1.0)
                worker = loop.register(
                    worker_id=args.worker_id or None,
                    label=args.label,
                    capabilities=args.capability,
                    role_profile=args.role_profile or None,
                )
                _emit(worker.model_dump(), args.output_format)
                return
            worker = harness_lab_services.runtime.worker_registry.register_worker(
                WorkerRegisterRequest(
                    worker_id=args.worker_id or None,
                    label=args.label,
                    capabilities=args.capability,
                    role_profile=args.role_profile or None,
                    sandbox_backend=harness_lab_services.sandbox.status().sandbox_backend,
                    sandbox_ready=(
                        harness_lab_services.sandbox.status().docker_ready
                        and harness_lab_services.sandbox.status().sandbox_image_ready
                    ),
                    version="v1",
                )
            )
            _emit(worker.model_dump(), args.output_format)
            return

        if args.worker_command == "status":
            if args.control_plane_url.strip():
                client = WorkerRuntimeClient(args.control_plane_url.strip())
                worker_detail = client.get_worker(args.worker_id)
                _emit(worker_detail, args.output_format)
                return
            if args.worker_id:
                recent_leases = harness_lab_services.runtime.list_leases(worker_id=args.worker_id)[-5:]
                recent_lease_ids = {lease.lease_id for lease in recent_leases}
                _emit(
                    {
                        "worker": harness_lab_services.runtime.worker_registry.get_worker(args.worker_id).model_dump(),
                        "health_summary": harness_lab_services.runtime.get_worker_health_summary(args.worker_id).model_dump(),
                        "sandbox": {
                            "backend": harness_lab_services.runtime.worker_registry.get_worker(args.worker_id).sandbox_backend,
                            "ready": harness_lab_services.runtime.worker_registry.get_worker(args.worker_id).sandbox_ready,
                        },
                        "recent_leases": [lease.model_dump() for lease in recent_leases],
                        "recent_events": [
                            event.model_dump()
                            for event in harness_lab_services.runtime.list_events(limit=500)
                            if event.payload.get("worker_id") == args.worker_id
                            or event.payload.get("lease_id") in recent_lease_ids
                        ][-10:],
                    },
                    args.output_format,
                )
                return
            _emit([item.model_dump() for item in harness_lab_services.runtime.worker_registry.list_workers()], args.output_format)
            return

        if args.worker_command == "drain":
            if args.control_plane_url.strip():
                client = WorkerRuntimeClient(args.control_plane_url.strip())
                worker = client.drain_worker(args.worker_id, args.reason or None)
                _emit(worker.model_dump(), args.output_format)
                return
            worker = harness_lab_services.runtime.worker_registry.drain_worker(args.worker_id, args.reason or None)
            _emit(worker.model_dump(), args.output_format)
            return

        if args.worker_command == "resume":
            if args.control_plane_url.strip():
                client = WorkerRuntimeClient(args.control_plane_url.strip())
                worker = client.resume_worker(args.worker_id)
                _emit(worker.model_dump(), args.output_format)
                return
            worker = harness_lab_services.runtime.worker_registry.resume_worker(args.worker_id)
            _emit(worker.model_dump(), args.output_format)
            return

        if args.worker_command == "serve":
            control_plane_url = args.control_plane_url.strip() or _default_control_plane_url()
            client = WorkerRuntimeClient(control_plane_url)
            loop = WorkerExecutionLoop(client, poll_interval_seconds=max(0.1, args.interval))
            result = loop.serve(
                worker_id=args.worker_id or None,
                label=args.label,
                capabilities=args.capability,
                role_profile=args.role_profile or None,
                interval_seconds=max(0.1, args.interval),
                once=args.once,
                max_tasks=max(1, args.max_tasks),
            )
            result["control_plane_url"] = control_plane_url
            _emit(result, args.output_format)
            return

    if args.command == "runs" and args.runs_command == "watch":
        run_id = args.run_id or _latest_run_id()
        if not run_id:
            raise SystemExit("No run is available to watch.")
        while True:
            run = harness_lab_services.runtime.get_run(run_id)
            payload = {
                "run_id": run.run_id,
                "status": run.status,
                "assigned_worker_id": run.assigned_worker_id,
                "current_attempt_id": run.current_attempt_id,
                "active_lease_id": run.active_lease_id,
                "coordination_snapshot": harness_lab_services.runtime.run_coordination_snapshot(run_id).model_dump(),
                "timeline_summary": harness_lab_services.runtime.run_timeline_summary(run_id),
                "status_summary": harness_lab_services.runtime.run_status_summary(run_id),
                "mission_phase": harness_lab_services.runtime.mission_phase_snapshot(run_id).model_dump(),
                "latest_handoff": harness_lab_services.runtime.run_handoffs(run_id)[-1] if harness_lab_services.runtime.run_handoffs(run_id) else None,
                "sandbox_summary": harness_lab_services.runtime.run_sandbox_summary(run_id),
                "result": run.result,
            }
            _emit(payload, args.output_format)
            if args.once or run.status in {"completed", "failed", "awaiting_approval", "cancelled"}:
                return
            time.sleep(max(0.1, args.interval))

    if args.command == "serve":
        from backend.app.main import main as serve_main

        serve_main()
        return


if __name__ == "__main__":
    main()
