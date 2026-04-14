# Harness Lab Suggestions (Current Baseline)

> 本文档是“当前建议”，不是历史路线图。已与 `MAINTENANCE.md` 对齐，优先反映真实代码状态与下一步执行优先级。

---

## Current Snapshot

### 已落地（Done）

- Postgres + Redis 执行底座
- Lease-driven worker 协议（poll/heartbeat/complete/fail/release/reclaim）
- Fleet 模块化（worker_registry / dispatcher / lease_manager / protocol adapters）
- Runtime/Fleet cutover（dispatch constraint 已统一到 fleet calculator）
- Sandboxed execution（docker + microvm + microvm_stub 抽象）
- Semantic constraints governance 主链（registry/detail/verify/scenarios/validation gate）
- Artifact store abstraction（local + s3-compatible）
- Multi-agent trace 链路（phase/handoff/review/replay/eval）

### 当前边界（Known Limits）

- 单租户、自托管前提；未进入多租户 SaaS
- `microvm` 目前是本地 runner 语义，不是 fleet 级隔离编排
- `runtime/service.py` 体量仍偏大（虽已剥离重复 dispatch 逻辑）
- `SUGGESTIONS` 与 `MAINTENANCE` 之外的部分历史文档仍可能有过时表述

---

## Next Priority (Recommended)

## 1) Fleet Reliability Deepening

目标：把“能跑”推进到“可长期稳定运维”。

- 强化 worker drain/maintenance 语义与 operator 体验
- 强化 queue shard 观测与 dispatch blocker 诊断
- 强化 stale lease reclaim 与 late callback 可观测性
- 在不破坏现有 API 形状前提下，持续收敛 runtime 复杂度

建议验收：

- worker 可安全 drain/resume，不影响在跑 lease 的收尾
- stuck run 原因可在 health/doctor/web 中直接定位
- 角色/标签/能力路由稳定，避免“谁先 poll 到就执行”

## 2) Constraint Governance Deepening

目标：在已闭环基础上增强治理深度，而不是重做内核。

- 增加约束版本 diff 的可读性和发布阻塞解释
- 强化 validation scenario 资产化（可复用、可回归）
- 将 run/replay 中的 constraint evidence 进一步前端可视化

建议验收：

- operator 能快速解释“为什么这版约束能发/不能发”
- 同一约束链的版本变化可直接定位到规则/决策差异

## 3) Runtime Service Decomposition (Maintenance Track)

目标：继续减轻 `runtime/service.py` 复杂度，降低后续演化成本。

- 将 read-model/snapshot 生成逻辑逐步下沉
- 将控制面聚合查询与执行面协调逻辑分层
- 保持行为不变，优先做“可维护性重构”

建议验收：

- 关键行为无回归
- 代码边界更清晰，新增能力不再堆回 runtime 巨石

---

## Guardrails

- 不回退到 SQLite 作为运行时主路径（仅测试注入）
- 不绕过 approval 与 deny-before-allow 安全边界
- 不在本阶段引入破坏式 API 变更
- 所有维护改动必须通过统一回归基线

---

## Regression Commands

```bash
# backend
pytest backend/tests -q
pytest backend/tests/test_harness_lab_platform.py -q
pytest backend/tests/integration/test_constraints_integration.py -q

# frontend
cd frontend
node ./node_modules/typescript/bin/tsc --noEmit
node ./node_modules/vite/bin/vite.js build
```

参考基线（2026-04-14）:
- `pytest backend/tests -q` -> `243 passed, 7 skipped, 3 warnings`
- `pytest backend/tests/test_harness_lab_platform.py -q` -> `26 passed, 3 warnings`
- `pytest backend/tests/integration/test_constraints_integration.py -q` -> `21 passed`

---

## Suggested Working Agreement

- `MAINTENANCE.md` 负责“事实与状态”
- `SUGGESTIONS.md` 负责“下一步执行建议”
- 如两者冲突，以 `MAINTENANCE.md` 为准，并先修正文档再推进实现
