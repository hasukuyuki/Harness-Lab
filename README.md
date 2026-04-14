... 正在录音 ...# Harness Lab

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
- switchable artifact backend with local filesystem and S3-compatible object storage (MinIO for dev, AWS S3 for production)
- lease-driven remote-worker protocol with mission / attempt / lease visibility
- Pluggable sandbox execution backend with `SandboxExecutor` abstraction layer
  - **Docker** (default, production-ready): hardened container with rootless, no-new-privileges, capability drop
  - **MicroVM** (real backend): Firecracker-style local runner with readiness probes, VM trace metadata, and artifact-backed evidence
  - **MicroVM stub** (fallback/testing only): validates abstraction compatibility when a real MicroVM backend is unavailable
  - Backend selection via `HARNESS_SANDBOX_BACKEND` environment variable
  - Unified `SandboxSpec/SandboxTrace/SandboxResult` contracts across all backends
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
- `GET /api/artifacts/{id}`
- `GET /api/artifacts/{id}/content`
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
# Start all infrastructure services (Postgres, Redis, MinIO)
docker compose -f docker/docker-compose.yml up -d

# Or start individual services
docker compose -f docker/docker-compose.yml up -d harness-lab-postgres harness-lab-redis
docker compose -f docker/docker-compose.yml up -d harness-lab-minio  # For S3 artifact backend
```

### Backend
```bash
export HARNESS_DB_URL=postgresql://harness_lab:harness_lab@127.0.0.1:5432/harness_lab
export HARNESS_REDIS_URL=redis://127.0.0.1:6379/0
python3 -m uvicorn backend.app.main:app --reload --port 4600
```

```bash
# Optional: switch sandbox backend to the real local MicroVM runner
export HARNESS_SANDBOX_BACKEND=microvm
export HARNESS_MICROVM_BINARY=python3
export HARNESS_MICROVM_KERNEL_IMAGE=backend/data/harness_lab/microvm/vmlinux.bin
export HARNESS_MICROVM_ROOTFS_IMAGE=backend/data/harness_lab/microvm/rootfs.img
export HARNESS_MICROVM_WORKDIR=backend/data/harness_lab/microvm/workdir
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

## Test Matrix

```bash
# Default backend regression
pytest backend/tests -q

# Focused sandbox + platform regression
pytest backend/tests/unit/test_sandbox_hardening.py \
  backend/tests/unit/test_microvm_executor.py \
  backend/tests/test_sandbox_boundary.py \
  backend/tests/test_harness_lab_platform.py -q
```

```bash
# Optional infra-backed integration checks
docker compose -f docker/docker-compose.yml up -d harness-lab-postgres harness-lab-redis harness-lab-minio
pytest backend/tests/integration -q
```

Open:
- Workbench: `http://localhost:3000`
- API docs: `http://localhost:4600/docs`

## Current Limits

- single-user local control plane only
- lease-driven remote worker plane now runs against real Postgres + Redis and has been smoke-tested through `session -> run -> worker poll -> lease -> complete`; the local SQLite path survives only as an injected test store, not a runtime backend
- sandboxing now supports both Docker and a real `microvm` backend with kernel/rootfs/workdir readiness checks; the local runner is production-shape infrastructure, but not yet a full multi-tenant VM fabric
- semantic constraints are now parsed, compiled, and verified through a dedicated engine with deny-before-allow verdicts, matched rule explanations, and operator-facing governance workbench; the current limitation is authoring/governance UX depth rather than core engine capability
- multi-agent orchestration is now role-aware and replayable, but it is still workflow-bounded rather than a fully autonomous swarm
- self-improvement now diagnoses multi-agent traces and auto-evaluates candidates, but it still only optimizes policy/workflow versions rather than platform source code
- artifact storage supports both local filesystem and S3-compatible backends with real-time health checks; control plane proxies artifact reads consistently across backends
