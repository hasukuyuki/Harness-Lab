# Harness Lab

本仓库当前是一个`研究优先、可回放、正在向生产型多 agent 平台演进`的 Harness 平台，不再是旧工作流产品，也还不是最终形态的生产级 agent 云。

## What It Is

- `backend/app/harness_lab/context`: 分层 context 管理
- `backend/app/harness_lab/constraints`: 自然语言约束与 policy verdict
- `backend/app/harness_lab/boundary`: 工具边界、patch staging、workspace 审计
- `backend/app/harness_lab/orchestrator`: task graph 与 wave-ready 调度
- `backend/app/harness_lab/runtime`: session / run / mission / attempt / lease runtime
- `backend/app/harness_lab/improvement`: candidate、eval harness、publish gate
- `backend/app/harness_lab/control_plane`: sessions、runs、workers、leases、replays、evals API
- `frontend/src/lab`: Harness Lab mission-control workbench

## Core Capabilities

- session-first research workflow
- provider-backed intent / reflection with heuristic fallback
- layered context blocks with token-budget trimming
- natural-language constraints with deny-before-allow verdicts
- fixed prompt frame ordering
- replayable execution traces, approval chain, and artifact indexing
- lease-driven remote-worker protocol with mission / attempt / lease visibility
- Docker-backed sandbox boundary for high-risk tool execution
- role-aware mission orchestration with handoff packets, review verdicts, and mission-phase visibility
- offline replay / benchmark evaluation and strict candidate publish gate
- multi-agent self-improvement loop that diagnoses traces, generates policy/workflow candidates, and auto-runs replay + benchmark gate checks

## Main API Surface

- `POST /api/sessions`
- `POST /api/intent/declare`
- `POST /api/context/assemble`
- `POST /api/prompts/render`
- `POST /api/constraints/verify`
- `POST /api/runs`
- `GET /api/runs/{id}`
- `POST /api/workers/register`
- `POST /api/workers/{worker_id}/poll`
- `GET /api/leases`
- `POST /api/leases/{lease_id}/heartbeat`
- `POST /api/leases/{lease_id}/complete`
- `GET /api/replays/{id}`
- `POST /api/evals/replay`
- `POST /api/evals/benchmark`

## Local Development

### Infra
```bash
docker compose -f docker/docker-compose.yml up -d harness-lab-postgres harness-lab-redis
```

### Backend
```bash
export HARNESS_DB_URL=postgresql://harness_lab:harness_lab@127.0.0.1:5432/harness_lab
export HARNESS_REDIS_URL=redis://127.0.0.1:6379/0
python3 -m uvicorn backend.app.main:app --reload --port 4600
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

### CLI Worker
```bash
python3 -m backend.app.harness_lab.cli worker serve --label cli-worker
```

Open:
- Workbench: `http://localhost:3000`
- API docs: `http://localhost:4600/docs`

## Current Limits

- single-user local control plane only
- lease-driven remote worker plane now runs against real Postgres + Redis and has been smoke-tested through `session -> run -> worker poll -> lease -> complete`; the local SQLite path survives only as an injected test store, not a runtime backend
- sandboxing now uses Docker for high-risk tool paths, but it is still a single-host boundary rather than a production microVM or multi-tenant isolation layer
- custom natural-language constraints still resolve through heuristic policy behavior instead of rich semantic rule compilation
- multi-agent orchestration is now role-aware and replayable, but it is still workflow-bounded rather than a fully autonomous swarm
- self-improvement now diagnoses multi-agent traces and auto-evaluates candidates, but it still only optimizes policy/workflow versions rather than platform source code
