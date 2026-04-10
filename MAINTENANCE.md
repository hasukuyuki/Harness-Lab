# Harness Lab 维护记录

> 本文档记录架构决策、已知问题和维护状态。

---

## 架构决策记录 (ADR)

### ADR-001: 存储层采用 Postgres + Redis 双后端

**状态**: ✅ 已完成  
**决策**: 主存储使用 PostgreSQL，Redis 用于事件流和 worker 协调  
**原因**: 
- SQLite 在单实例下工作良好，但难以扩展到多 worker 部署
- Redis 提供 lease 过期、dispatch queue 的原子操作支持

**仍未解决的边界**: 
对象存储尚未接入，artifact 仍是本地文件系统。

---

### ADR-002: Docker 沙箱作为高风险工具边界

**状态**: ✅ 已完成  
**决策**: shell/git/http_fetch/write_file 等高风险操作在 Docker 容器内执行  
**原因**:
- 单主机执行有安全风险
- Docker 提供可复现的执行环境

**仍未解决的边界**: 
仍是 Docker 单机边界，不是 microVM/多租户隔离。

---

### ADR-003: Worker Lease 协议

**状态**: ✅ 已完成  
**决策**: 采用 poll/heartbeat/complete 三步 lease 协议  
**流程**:
1. Worker 调用 `poll_worker()` 获取 dispatch
2. 持有 lease 期间周期性 `heartbeat_lease()`
3. 任务完成后调用 `complete_lease()` 或 `fail_lease()`

**状态机**:
```
leased → running → completed/failed/released
  ↓
expired (由 reclaim_stale_leases 处理)
```

**仍未解决的边界**: 
已有 drain/reclaim，但尚未形成成熟 fleet orchestration。

---

## 已知问题 (Known Drift)

### 架构层面

| 问题 | 严重度 | 计划修复 | 备注 |
|------|--------|----------|------|
| `runtime/service.py` 直接访问 `task_graph.nodes` | 中 | 阶段一 | 应通过 orchestrator 接口访问 |
| LeaseManager 与 WorkerService 存在间接循环依赖 | 中 | 阶段一 | 通过 RuntimeService 解耦，需进一步理清 |
| `dispatch_queue.py` 位于顶层，模块边界不清晰 | 中 | 阶段一 | 直接并入 fleet/dispatcher |

### 代码层面

| 问题 | 严重度 | 计划修复 | 备注 |
|------|--------|----------|------|
| `types.py` 936 行，所有类型在一个文件 | 中 | 阶段一 1.2 | 按域拆分 |
| `execution_plane.py` 1000+ 行，LeaseManager 与 RuntimeService 深度耦合 | 高 | 阶段一 1.1 | LeaseManager 迁移需先定义 RuntimeService 接口边界 |
| `runtime/service.py` 1000+ 行，职责过多 | 高 | 阶段一 1.1 | 可逐步委托给 fleet（非阻塞）|

### 文档层面

| 问题 | 严重度 | 计划修复 | 备注 |
|------|--------|----------|------|
| `SUGGESTIONS.md` 描述过时（SQLite、Worker框架） | 中 | 阶段二 2.1 | 更新为实际状态 |

---

## 模块边界约定

### 当前边界（维护前）

```
harness_lab/
├── runtime/
│   ├── service.py          # RuntimeService: session/run/lease/worker 全管
│   ├── execution_plane.py  # RunCoordinator + LeaseManager + LocalWorkerAdapter
│   └── models.py           # ModelRegistry
├── workers/
│   └── service.py          # WorkerService: worker 注册/心跳
├── dispatch_queue.py       # 顶层模块，与 runtime 关系不清
└── types.py                # 所有类型定义
```

### 目标边界（维护后）

```
harness_lab/
├── runtime/
│   ├── service.py          # RuntimeService: session/run 管理，委托 lease 给 fleet
│   ├── run_coordinator.py  # Run 状态机协调（保留在 runtime）
│   ├── local_worker.py     # LocalWorker（保留在 runtime）
│   └── models.py           # ModelRegistry
├── fleet/                  # 新增模块：统一调度层
│   ├── __init__.py
│   ├── worker_registry.py  # Worker 生命周期（从 workers/service.py 演进）
│   ├── lease_manager.py    # Lease 状态机（从 execution_plane.py 直接迁移）
│   └── dispatcher.py       # 任务分发（从 dispatch_queue.py 并入）
├── workers/
│   └── runtime_client.py   # Worker 运行时客户端
├── types/                  # 按域拆分
│   ├── __init__.py
│   ├── base.py
│   ├── session_run.py
│   ├── worker_lease.py
│   └── ...
└── dispatch_queue.py       # 删除，逻辑并入 fleet/dispatcher.py
```

### 关键职责划分

| 模块 | 职责 | 不处理 |
|------|------|--------|
| `runtime/service.py` | Session/Run 生命周期、Prompt 组装、Context 管理 | Lease 状态机、Worker 注册 |
| `runtime/run_coordinator.py` | Task graph 状态流转、节点就绪判断 | 具体 lease 分配 |
| `fleet/lease_manager.py` | Lease 创建/心跳/完成/回收、状态机 | Run 级别协调 |
| `fleet/worker_registry.py` | Worker 注册/心跳/状态推导 | Lease 分配决策 |
| `fleet/dispatcher.py` | Ready queue 管理、task-to-worker 匹配 | Worker 健康判断 |

---

## 维护进度

### 阶段一：结构维护（已完成）

- [x] 1.1 建立 fleet/ 模块边界
  - [x] 创建 `fleet/` 目录
  - [x] 创建 `fleet/worker_registry.py`（从 workers/service.py 演进，提取纯数据层）
  - [x] 创建 `fleet/dispatcher.py`（框架，并入 dispatch 逻辑待完成）
  - [x] 创建 `fleet/lease_manager.py`（占位，记录迁移阻塞因素）
- [x] 1.2 收束 types.py
  - [x] 创建 `types/` 目录结构
  - [x] 按域迁移类型定义（base, session_run, worker_lease, policy_constraint, sandbox, improvement, knowledge, recovery, tool, system）
  - [x] 更新 `types/__init__.py` 保持向后兼容
  - [x] 原 `types.py` 已备份为 `types.py.bak`

### 下一阶段：Runtime/Fleet Interface Extraction

**目标**: 让 LeaseManager 从"依赖 RuntimeService 私有实现"变成"依赖稳定协议"

**关键接口提取**:

| 接口 | 当前位置 | 用途 |
|------|----------|------|
| `dispatch_constraint_for_node(node, run, session)` | `RuntimeService._dispatch_constraint_for_node()` | 计算任务分派约束 |
| `advance_run_after_attempt(run, session, node, outcome)` | `LeaseManager` 内联逻辑 + `RunCoordinator.after_lease_transition()` | 推进 Run 状态机 |
| `build_dispatch_context(run, session, node, worker)` | `LeaseManager.create_dispatch()` 内 | 构建 DispatchEnvelope |
| `record_dispatch_blocker(run, session, node, reason)` | `RuntimeService._dispatch_blockers_for_run()` | 记录分派阻塞原因 |

**实施步骤**:
1. 在 `fleet/` 或 `runtime/` 定义接口协议（抽象基类或 Protocol）
2. `RuntimeService` 实现这些接口
3. `LeaseManager` 改为依赖接口而非具体 `RuntimeService`
4. 迁移 `LeaseManager` 到 `fleet/`
5. 更新 `fleet/__init__.py` 导出

### 阶段二：基线维护

- [ ] 2.1 更新 SUGGESTIONS.md
- [ ] 2.2 建立提交规范（git message tags）
- [ ] 2.3 更新本文件（MAINTENANCE.md）记录最终状态

### 阶段三：功能推进

- [ ] 3.1 Stronger artifact store
- [ ] 3.2 Fleet reliability 深化
- [ ] 3.3 Semantic constraints

---

## 回归测试清单

每次维护阶段完成后执行：

```bash
# 1. 环境检查
docker compose -f docker/docker-compose.yml ps

# 2. 单元测试
cd backend && python -m pytest tests/ -v --tb=short

# 3. 集成测试（手动）
curl -s http://localhost:4600/api/health | jq .

# 4. 完整流程测试
curl -X POST http://localhost:4600/api/sessions \
  -H "Content-Type: application/json" \
  -d '{"goal": "测试任务", "context": {"path": "."}}'

# 验证: 能创建 session、执行 run、查看 replay
```

---

## 变更日志

### 2026-04-10
- 创建 MAINTENANCE.md
- 记录当前架构决策（ADR-001/002/003），每个标注"仍未解决的边界"
- 明确模块边界规划

**结构维护 + Cut Over 完成：**

- **1.2 types.py 拆分** ✅
  - 按域拆分为 10 个子模块
  - 原 `types.py` 备份为 `types.py.bak`

- **1.1 fleet/ 模块建立 + Cut Over** ✅
  - 创建 `fleet/worker_registry.py`（Worker 纯数据层）
  - 创建 `fleet/dispatcher.py`（分发器框架）
  - 创建 `fleet/protocols.py`（5 个协议定义）
  - 创建 `fleet/adapters.py`（RuntimeService 适配器）
  - 创建 `fleet/lease_manager.py`（协议化 LeaseManager）
  - **Cut Over**: `RuntimeService` 切换到 `fleet.LeaseManager`
  - **解耦**: `adapters.py` 不再依赖旧 `LeaseManager`
  - **清理**: 删除 `runtime/execution_plane.py` 中的旧 `LeaseManager`

- **回归修复** ✅
  - 修复 `fleet/__init__.py` 导入错误
  - 修复 `MAINTENANCE.md` 健康检查路由错误
  - 修复 `types/worker_lease.py` 类型默认值不匹配

**基线维护完成：**
- **2.1 SUGGESTIONS.md 更新** ✅

---

## 当前架构状态

```
harness_lab/
├── runtime/
│   ├── service.py              # 使用 fleet.LeaseManager (协议化)
│   ├── execution_plane.py      # RunCoordinator + LocalWorkerAdapter (旧 LeaseManager 已删除)
│   └── ...
├── fleet/                      # 新增模块
│   ├── __init__.py             # 导出 LeaseManager, WorkerRegistry 等
│   ├── lease_manager.py        # 协议化 LeaseManager (原 V2)
│   ├── worker_registry.py      # Worker 纯数据层
│   ├── protocols.py            # 5 个协议定义
│   ├── adapters.py             # RuntimeService 适配器
│   └── dispatcher.py           # 分发器框架
└── types/                      # 按域拆分
```

**已完成收口**：
- ✅ `RuntimeService` 使用 `WorkerRegistry` 替代 `worker_service`
- ✅ 旧 `LeaseManager` 归档到 `legacy/`
- ✅ 集成测试覆盖核心流程


---

## Interface Extraction 完成 ✅ / Cut Over 未完成 ⏳

**Runtime/Fleet Interface Extraction** 已完成，但 **main-path cutover** 尚未进行。

### 当前准确状态

| 组件 | 状态 | 说明 |
|------|------|------|
| Protocol 定义 | ✅ 完成 | `fleet/protocols.py` 5 个协议 |
| Adapter 实现 | ✅ 完成 | `fleet/adapters.py` 全部适配器 |
| LeaseManagerV2 | ✅ 完成 | `fleet/lease_manager_v2.py` 协议化实现 |
| **Main-path cutover** | ⏳ **未完成** | `runtime/service.py` 仍用旧 `LeaseManager` |
| **Execution result decoupling** | ⏳ **未完成** | `adapters.py` 仍回调旧 `lease_manager._apply_*` |

### 阻塞问题

```python
# runtime/service.py (line 60, 93)
from .execution_plane import LeaseManager, ...
...
self.lease_manager = LeaseManager(self)  # 仍是旧实现

# fleet/adapters.py (line 197, 208)
def apply_execution_success(...):
    # 反向耦合！仍调用旧 LeaseManager 私有方法
    self.runtime.lease_manager._apply_execution_success(...)
```

### Cut Over 完成 ✅

**Step 1: 切换到 LeaseManagerV2 实例化** ✅
- [x] 修改 `RuntimeService.__init__()` 使用 `LeaseManagerV2`
- [x] 通过 `create_protocol_adapters()` 注入依赖

**Step 2: 解耦 execution result handling** ✅
- [x] 把 `_apply_execution_success/_failure` 从旧 `LeaseManager` 抽出
- [x] 内联到 `RuntimeTaskExecutionAdapter`
- [x] 消除 adapter 对旧实现的反向依赖

**Step 3: 主路径验证测试** ✅
- [x] 补一组"LeaseManagerV2 确实在使用"的测试
- [x] 确认 RuntimeService 导入的是 LeaseManagerV2
- [x] 确认 Adapter 不再回调旧 LeaseManager

**Step 4: 最终清理** ✅
- [x] 删除 `runtime/execution_plane.py` 中的旧 `LeaseManager`
- [x] `fleet/lease_manager_v2.py` 重命名为 `fleet/lease_manager.py`
- [x] 类名从 `LeaseManagerV2` 改为 `LeaseManager`
- [x] 更新所有引用（RuntimeService、测试文件、fleet/__init__.py）
- [x] 运行验证确认 Cut Over 完成

### 验收标准

```python
# ✅ 完成
self.lease_manager = LeaseManager(  # 来自 fleet/lease_manager
    database=database,
    coordination=adapters["coordination"],
    constraints=adapters["constraints"],
    ...
)
```
```
