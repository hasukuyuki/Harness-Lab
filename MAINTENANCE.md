# Harness Lab Maintenance

> 本文档是当前维护基线（single source of truth），用于记录已落地决策、已完成收口、仍待处理漂移项，以及统一回归命令。

---

## ADR

### ADR-001: 存储层采用 Postgres + Redis

**状态**: done  
**决策**: Postgres 作为 truth source，Redis 负责 dispatch/lease 热路径。  
**边界**: artifact blob 目前仍以本地文件系统为默认后端（已具备 local/S3-compatible 抽象）。

### ADR-002: 可替换沙箱执行后端

**状态**: done  
**决策**: 执行边界统一通过 `SandboxExecutor` 抽象，支持 `docker`、`microvm`、`microvm_stub`。  
**边界**: 当前 `microvm` 为本地 runner 语义，未进入多租户 fleet 级隔离。

### ADR-003: Lease 驱动执行协议

**状态**: done  
**决策**: worker 采用 `poll -> heartbeat -> complete/fail/release` 协议，stale lease 默认自动 reclaim。  
**边界**: 已有 drain/reclaim/stuck 诊断，但 fleet 运维能力仍可继续深化。

### ADR-004: 副作用分类与执行证据链

**状态**: done  
**决策**: 高风险动作走 sandbox 并产出标准化 evidence（trace/stdout/stderr/changed_paths/artifacts）。  
**边界**: 当前以 control plane 代理读取 artifact 内容，未做 presigned URL 直连。

### ADR-005: Semantic Constraints Governance Workbench

**状态**: done  
**决策**: 约束治理工作台支持 `registry/detail/verify/versions/scenarios/validation gate` 主链。  
**边界**: 约束治理已闭环到发布闸门，但更深的 rollout UX 与多人协作审批仍可后续增强。

### ADR-006: Runtime/Fleet Maintenance Cutover

**状态**: done  
**决策**: fleet 成为 worker/lease/dispatch 唯一维护面，runtime 仅保留 session/run/replay facade。  
**收口点**:
- `DispatchConstraintCalculator` 抽离到 fleet 层并成为统一计算源
- `RunCoordinationProtocol` 增加 `record_handoffs()`，清理 lease manager 中原 TODO
- runtime 中旧 dispatch wrapper 已下线，不再保留重复实现路径

---

## Current Architecture

```text
harness_lab/
├── runtime/
│   ├── service.py              # session/run/replay facade + orchestration glue
│   └── execution_plane.py      # RunCoordinator + LocalWorkerAdapter
├── fleet/
│   ├── worker_registry.py      # worker lifecycle
│   ├── dispatcher.py           # queue shard + worker matching + dispatch claim
│   ├── lease_manager.py        # lease lifecycle + reclaim + callbacks
│   ├── constraints.py          # DispatchConstraintCalculator
│   ├── protocols.py            # Runtime/Fleet protocol contracts
│   └── adapters.py             # Runtime adapters for fleet protocols
├── boundary/
│   ├── docker_executor.py
│   ├── microvm_executor.py
│   └── sandbox.py
└── types/
    └── domain-split models
```

---

## Known Drift (Open)

### Architecture / Code

| Item | Severity | Notes |
|---|---|---|
| `runtime/service.py` 仍偏大 | medium | 虽已移除重复 dispatch 逻辑，但文件体量仍高，可继续拆分 facade/read model。 |
| `runtime/execution_plane.py` 命名仍承载历史心智 | low | 当前仅 RunCoordinator + LocalWorkerAdapter，后续可考虑命名与目录再收束。 |
| `control_plane/workers.py` 的 `eligible_task_count` 为扫描式计算 | low | 当前正确但成本偏高，后续可做缓存/索引化。 |

### Docs / Process

| Item | Severity | Notes |
|---|---|---|
| `SUGGESTIONS.md` 仍包含多处过时描述 | medium | 当前文档明显落后于真实实现，建议下一轮统一重写或降级为历史参考。 |
| 提交规范（message tags）未形成硬规则 | low | 可以在下一轮维护时补一份简版规范。 |

---

## Completed Milestones

- types 按域拆分完成（原 `types.py` 历史版本已归档）
- fleet module 建立并完成 main-path cutover
- lease manager 协议化与 adapter 解耦完成
- dispatch constraint 统一为 fleet calculator（单一计算源）
- semantic constraints governance 主链完成
- microvm-ready sandbox abstraction 完成
- artifact store abstraction（local + s3-compatible）完成

---

## Regression Baseline

```bash
# 1) 默认后端回归
pytest backend/tests -q

# 2) 平台关键链路（worker/lease/run/constraint evidence）
pytest backend/tests/test_harness_lab_platform.py -q

# 3) 约束治理链路
pytest backend/tests/integration/test_constraints_integration.py -q

# 4) 前端构建
cd frontend
node ./node_modules/typescript/bin/tsc --noEmit
node ./node_modules/vite/bin/vite.js build
```

最近一次维护收口基线（2026-04-14）:
- `pytest backend/tests -q` -> `243 passed, 7 skipped, 3 warnings`
- `pytest backend/tests/test_harness_lab_platform.py -q` -> `26 passed, 3 warnings`
- `pytest backend/tests/integration/test_constraints_integration.py -q` -> `21 passed`

---

## Next Priority

`Fleet Reliability Deepening`（生产可靠性深化）:
- worker routing/drain/maintenance 的运维体验进一步做硬
- queue shard 与 stuck diagnostics 持续完善
- 在保持 API 稳定前提下继续压低 runtime 复杂度

---

## Change Log

### 2026-04-14

- MAINTENANCE 文档重写为单一真相版本，移除重复和冲突状态描述
- 同步 Runtime/Fleet cutover 真实状态（含 dispatch wrapper 下线）
- 固化当前回归基线与下一优先级
