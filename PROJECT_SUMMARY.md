# Harness Lab Summary

This repository now targets a research-first Harness Lab with an emerging remote worker execution plane.

## Primary architecture

- `backend/app/harness_lab/context`: layered context assembly
- `backend/app/harness_lab/constraints`: semantic constraint engine with natural-language to rule compilation, deny-before-allow verdicts, and detailed explanations
- `backend/app/harness_lab/boundary`: tool gateway, patch staging, and artifact capture
- `backend/app/harness_lab/orchestrator`: task graph construction and wave-ready scheduling
- `backend/app/harness_lab/runtime`: session, run, mission, task attempt, and worker lease lifecycle
- `backend/app/harness_lab/improvement`: policy/workflow candidates, replay/benchmark evaluation, publish gate, and canary rollout with safe promotion governance
- `frontend/src/lab`: mission-control workbench

## Primary user experience

- create a research session from a natural-language goal
- inspect intent, context blocks, prompt frames, and task graph
- execute the session through a lease-driven worker path
- resolve approvals for risky actions
- inspect replays, attempts, leases, policies, and evaluations

## Repository status

The active repository surface is the Harness Lab core, mission-control web UI, CLI operator surface, and legacy archive.

## Important design drift

- The model layer is no longer heuristic-only: intent and reflection already use a provider-backed path with fallback.
- The execution layer is no longer purely single-worker: runs now materialize mission / attempt / lease entities and can be driven by worker polling.
- The storage layer has been cut over at the architecture level to Postgres + Redis with fail-fast startup, and the current implementation has now been smoke-tested against real Docker-backed Postgres/Redis. SQLite remains only as a test-only injected store for local regression coverage.
- The boundary layer is no longer host-only for risky actions: high-risk tools now route through a Docker sandbox with replayable sandbox traces.
- The orchestration layer is no longer single-agent only: runs now persist mission phase, role timeline, handoff packets, and review verdicts.
- The improvement layer now consumes multi-agent traces directly to diagnose failure clusters, auto-generate policy/workflow candidates, and auto-run replay + benchmark gate evaluations before promotion. It supports canary rollouts with percentage/label/pattern-based scope and promote/rollback governance with metrics-driven safety checks. The online canary analysis continuously monitors cohort metrics, generates promote/hold/rollback recommendations with structured reasoning, and tracks rollout metadata in every run/session for observability.
- The artifact layer now exposes a formal store abstraction with local and S3-compatible backends, plus artifact metadata/content APIs for replay and operator workflows.
- The constraint layer now compiles natural-language constraints into executable rule sets with detailed explanations, rather than relying solely on heuristic pattern matching.
