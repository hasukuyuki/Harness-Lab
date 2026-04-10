# Harness Lab 推进建议

> 由 Kimi Code CLI 生成于 2026-04-08

## 📊 当前状态评估

### 已完成的核心能力 ✅
- 完整的 Session-Run-Mission-Attempt-Lease 执行链
- Provider-backed 意图识别 + heuristic fallback
- 分层 Context 组装与 Prompt 构建
- 约束策略与审批门控
- 可回放的执行追踪
- Worker 租约协议（poll/heartbeat/complete/reclaim 完整链路）
- Docker 沙箱执行（shell/git/write_file 等高风险操作容器化）
- Postgres + Redis 存储层（已 smoke-tested）
- 离线评估与发布门控
- Mission Control Web UI (React + Ant Design)
- 多 Agent 编排（role-aware，支持 handoff 和 review）

### 当前限制 ⚠️
- 单用户本地控制平面（非多租户）
- 沙箱是单主机 Docker，非生产级 microVM/隔离
- 自然语言约束仍使用启发式解析，非语义规则编译
- knowledge_search 工具待完善
- 自改进循环优化 policy/workflow 版本，未触及平台源代码

---

## 🎯 推进路线图

### 阶段一：夯实基础（2-3 周）- 让现有功能真正可用

| 任务 | 优先级 | 说明 | 验收标准 |
|------|--------|------|----------|
| 完善 CLI 体验 | 🔴 高 | 当前 CLI 只有基础命令，需要更友好的交互 | `hlab session create/run/replay` 流畅可用 |
| 补充核心测试 | 🔴 高 | runtime/service.py 1600+ 行代码缺乏测试覆盖 | 核心模块测试覆盖率达 60%+ |
| 向量检索落地 | 🟡 中 | FAISS 目录已创建，但 knowledge_search 工具待完善 | 能搜索本地代码和文档 |
| 错误处理优化 | 🟡 中 | 添加更详细的日志和错误提示 | 用户能根据日志定位问题 |

**验证步骤：**
```bash
# 1. 启动服务
cd frontend && npm run dev &
python3 -m uvicorn backend.app.main:app --reload --port 4600

# 2. 跑通完整流程
curl -X POST http://localhost:4600/api/sessions \
  -H "Content-Type: application/json" \
  -d '{"goal": "分析项目结构", "context": {"path": "."}}'
```

---

### 阶段二：能力增强（1 个月）- 从演示到实用

| 任务 | 技术方案 | 预期收益 |
|------|----------|----------|
| Knowledge Search 真落地 | 集成 FAISS + sentence-transformers | 实现本地知识库检索能力 |
| Worker 协议完善 | 分离 CLI Worker 进程，支持远程注册 | 真正的分布式执行 |
| 约束引擎语义化 | 用小型 LLM 解析自然语言约束 | 提高策略准确性 |
| 偏好学习闭环 | 基于反馈调整 prompt/约束权重 | PRD 中提到的自我迭代 |

**推荐实现顺序：**
1. **文件系统工具完善** - `backend/app/harness_lab/boundary/gateway.py`
   - 文件读写、目录遍历
   - Git 状态检查
   - HTTP 内容获取

2. **向量检索集成**
   - 本地文档索引
   - 代码语义搜索
   - 检索结果与 Context 组装打通

3. **Worker 分离**
   - 独立 CLI Worker 进程
   - 心跳与租约协议
   - 远程 Worker 注册

---

### 阶段三：生产化（2-3 个月）- 从玩具到工具

| 任务 | 依赖 | 难度 | 建议 |
|------|------|------|------|
| ~~Postgres + Redis 存储层~~ | ✅ 已完成 | - | 已迁移并 smoke-tested |
| 容器化沙箱执行 | Docker SDK | 高 | 优先解决安全问题 |
| 多用户/权限系统 | JWT + RBAC | 中 | 如需对外提供服务再考虑 |
| 真正的多 Agent 协作 | 角色定义 + 协作协议 | 高 | 最后做，架构最复杂 |

**存储层状态：**
- ✅ Postgres：主存储，已配置并测试
- ✅ Redis：用于事件流、lease 过期追踪、dispatch queue
- ⚠️ 当前仅单实例部署，多实例扩展需后续工作

---

## 🚀 推荐立即开始的 3 件事

### 1. 补测试（技术债）

```bash
# 创建测试目录结构
mkdir -p backend/tests/{unit,integration,e2e}

# 优先测试文件（按重要性排序）
backend/tests/
├── unit/
│   ├── test_runtime_service.py      # 核心执行逻辑
│   ├── test_constraints_engine.py   # 安全策略
│   ├── test_context_manager.py      # 上下文组装
│   └── test_models.py               # 数据模型
├── integration/
│   ├── test_api_sessions.py         # API 端到端
│   └── test_worker_protocol.py      # Worker 交互
└── e2e/
    └── test_full_workflow.py        # 完整流程
```

**测试要点：**
- 意图识别的 fallback 逻辑
- 约束裁决的 deny-before-allow
- Lease 超时和心跳机制
- 审批流程的状态流转

---

### 2. 让 Knowledge Search 工作

当前 `knowledge_search` 工具是空的，建议实现：

```python
# backend/app/harness_lab/boundary/gateway.py

class KnowledgeSearchTool:
    """基于 FAISS 的本地知识检索"""
    
    def __init__(self, vector_db_path: str):
        self.index = self._load_or_create_index(vector_db_path)
        self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
    
    async def search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        # 1. 向量化 query
        # 2. FAISS 检索
        # 3. 组装 ContextBlock
        pass
    
    async def index_file(self, path: str) -> None:
        # 1. 读取文件内容
        # 2. 分块
        # 3. 编码并添加到索引
        pass
```

**应用场景：**
- 搜索项目中的函数定义
- 查找相关文档
- 基于历史会话检索类似问题

---

### 3. 完善 CLI 工具链

建议添加的命令：

```bash
# Session 管理
hlab session list                    # 列出所有会话
hlab session create "分析项目结构"    # 创建新会话
hlab session show <id>               # 查看会话详情

# 执行
hlab run --session <id>              # 执行已有会话
hlab run --goal "快速任务"            # 临时执行
hlab run --watch                     # 实时查看执行日志

# Worker
hlab worker list                     # 查看 Worker 状态
hlab worker serve --label "gpu-worker"  # 启动 Worker
hlab worker logs <worker-id>         # 查看 Worker 日志

# 回放与调试
hlab replay show <run-id>            # 查看执行回放
hlab replay diff <id1> <id2>         # 对比两次执行
hlab eval benchmark                  # 运行基准测试

# 系统
hlab doctor                          # 健康检查
hlab config                          # 查看/修改配置
```

---

## 📋 与原 PRD 的对比

| PRD 需求 | 当前状态 | 差距分析 | 建议 |
|----------|----------|----------|------|
| n8n 工作流引擎 | ❌ 已演进为自研 Harness | 架构升级，不再需要 | 保持当前架构 |
| 意图识别 | ✅ Provider-backed + Fallback | 已完成 | 可优化 prompt |
| 目标拆解 | ✅ Task Graph | 已完成 | 增强多 agent 拆解 |
| 向量检索 | 🟡 框架有了，待完善 | 中等 | 优先实现 |
| 偏好建模 | 🟡 数据库有了，算法待完善 | 中等 | 阶段二实现 |
| 自我迭代 | 🟡 improvement 模块存在 | 待增强 | 基于 eval 结果反馈 |

**与原设计的关键差异：**
1. **放弃了 n8n**：改为自研的 Harness 架构，更适合研究型 workflow
2. **简化了模型栈**：当前只用 DeepSeek API，未接入本地 Qwen
3. **增强了可观测性**：Replay、Trace、Eval 是原 PRD 未覆盖的
4. **存储层升级**：从 SQLite 迁移到 Postgres + Redis
5. **执行层隔离**：从本地执行升级到 Docker 沙箱
6. **Worker 协议**：从概念框架到完整实现的 poll/heartbeat/complete/reclaim 协议

---

## 🛠️ 技术债务清单

| 问题 | 位置 | 优先级 | 解决方案 |
|------|------|--------|----------|
| 存储层混杂 | storage.py | 中 | 抽象 Repository 接口 |
| 前端单文件过大 | App.tsx (1000+ 行) | 低 | 拆分为页面组件 |
| 缺少 API 文档 | - | 中 | 添加 OpenAPI 描述 |
| 配置分散 | .env + 代码 | 低 | 统一配置中心 |

---

## 💡 其他建议

### 关于模型选择
- 当前 DeepSeek 方案已够用
- 如需离线使用，可添加 Ollama 支持
- 本地小模型（Qwen-1.8B）可用于：敏感数据脱敏、快速意图分类

### 关于安全
- **当前风险**：Worker 在本地直接执行 shell 命令
- **短期缓解**：加强约束策略，危险命令必须审批
- **长期方案**：Docker 容器沙箱

### 关于扩展性
- 插件机制：在 `boundary/gateway.py` 中添加 Tool 注册机制
- 自定义节点：Workflow Template 中支持用户自定义步骤

---

## 📌 下一步行动（具体到这周）

1. **周一**：验证当前系统能跑通一个完整 Session → Run → Replay 流程
2. **周二-三**：写 3-5 个核心单元测试
3. **周四**：实现简单的 `hlab` CLI 入口（基于现有的 cli.py）
4. **周五**：Knowledge Search 原型（能索引和搜索 README 文件）

**成功标准：**
```bash
hlab session create "分析项目结构" --path ./
# 能看到 Intent Declaration
# 能执行并得到结果
# 能查看 Replay
```

---

## 📚 相关文件

- 原 PRD：`ai_workflow_prd.txt`
- 架构设计：`design/harness-architecture-design.md`
- 当前限制：`README.md` Current Limits 部分
- 使用说明：`USAGE.md`
