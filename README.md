# AI 工作流平台

基于 n8n 的模块化、可扩展 AI 工作流平台，支持语义理解、任务拆解、知识检索和自我迭代。

## 项目结构

```
ai-workflow-platform/
├── backend/                 # FastAPI 后端
│   ├── app/
│   │   ├── api/            # API 路由
│   │   ├── core/           # 核心模块
│   │   ├── models/         # 数据模型
│   │   └── services/       # 业务逻辑
├── frontend/               # 前端界面
├── n8n/                   # n8n 工作流配置
├── data/                  # 数据存储
│   ├── vector_db/         # FAISS 向量数据库
│   ├── preferences.db     # SQLite 偏好数据库
│   └── knowledge/         # 知识库文件
└── docker/                # Docker 配置
```

## 快速开始

### 方式一：一键启动（推荐）
```bash
python quick_start.py
```

### 方式二：手动启动
1. **初始化项目**
   ```bash
   python scripts/setup.py
   ```

2. **配置环境**
   - 复制 `.env.example` 为 `.env`
   - 编辑 `.env` 文件，设置你的 OpenAI API 密钥

3. **启动服务**
   ```bash
   python scripts/start.py
   ```

4. **访问应用**
   - 前端界面: http://localhost:3000
   - API文档: http://localhost:8000/docs
   - 后端API: http://localhost:8000

### 方式三：Docker启动
```bash
cd docker
docker-compose up -d
```

## 使用示例

1. 打开前端界面 http://localhost:3000
2. 输入问题，如："请解释什么是机器学习"
3. 系统会自动进行：
   - 语义分析识别意图
   - 拆解为具体任务
   - 从知识库检索相关信息
   - 生成结构化回答
4. 对回答进行评分，帮助系统学习你的偏好

## 功能特性

- 🧠 **智能语义理解**: 自动识别用户意图和需求类型
- 📋 **自动任务拆解**: 将复杂目标分解为可执行的子任务
- 🔍 **向量知识检索**: 基于FAISS的语义搜索，精准匹配相关信息
- 💭 **引导式推理**: 使用大语言模型进行结构化思考
- 📊 **结构化输出**: 生成格式化、易理解的结果
- 🎯 **偏好学习**: 基于用户反馈不断优化个人体验
- 🔄 **自我迭代**: 持续学习和改进系统性能

## 技术架构

- **后端**: FastAPI + Python
- **工作流引擎**: n8n (可视化工作流设计)
- **数据库**: SQLite (偏好存储) + FAISS (向量检索)
- **AI模型**: 
  - 本地模型: Qwen-1.8B/7B (偏好建模)
  - API模型: OpenAI/通义千问 (推理生成)
- **前端**: 原生HTML/CSS/JavaScript
- **部署**: Docker + Docker Compose

## 项目结构详解

```
ai-workflow-platform/
├── backend/                    # 后端服务
│   └── app/
│       ├── main.py            # FastAPI应用入口
│       └── core/              # 核心模块
│           ├── workflow_engine.py      # 工作流引擎
│           ├── intent_analyzer.py      # 意图分析
│           ├── task_planner.py         # 任务规划
│           ├── vector_db.py            # 向量数据库
│           ├── reasoning_model.py      # 推理模型
│           └── preference_model.py     # 偏好学习
├── frontend/                   # 前端界面
│   └── index.html             # 单页面应用
├── n8n/                       # n8n工作流配置
│   └── workflows/             # 工作流模板
├── docker/                    # Docker配置
│   ├── docker-compose.yml     # 服务编排
│   ├── Dockerfile.backend     # 后端镜像
│   └── Dockerfile.frontend    # 前端镜像
├── scripts/                   # 工具脚本
│   ├── setup.py              # 初始化脚本
│   └── start.py               # 启动脚本
├── data/                      # 数据存储
│   ├── vector_db/            # FAISS向量数据库
│   ├── preferences.db        # SQLite偏好数据库
│   └── knowledge/            # 知识库文件
├── quick_start.py            # 一键启动脚本
├── test_workflow.py          # 功能测试脚本
└── requirements.txt          # Python依赖
```

## 测试验证

运行功能测试：
```bash
python test_workflow.py
```

测试将验证：
- ✅ 意图分析模块
- ✅ 任务规划模块  
- ✅ 向量数据库模块
- ✅ 偏好学习模块
- ✅ 完整工作流程

## 配置说明

### API密钥配置
支持多种AI服务：

**OpenAI**:
```bash
OPENAI_API_KEY=sk-your-openai-key
OPENAI_BASE_URL=https://api.openai.com/v1
```

**通义千问**:
```bash
OPENAI_API_KEY=sk-your-qwen-key
OPENAI_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
```

**DeepSeek**:
```bash
OPENAI_API_KEY=sk-your-deepseek-key
OPENAI_BASE_URL=https://api.deepseek.com/v1
```

## 扩展开发

### 添加自定义知识库
```python
from backend.app.core.vector_db import VectorDB
import asyncio

async def add_knowledge():
    vector_db = VectorDB()
    await vector_db.add_document(
        text="你的知识内容",
        doc_id="unique_doc_id",
        metadata={"category": "custom"}
    )

asyncio.run(add_knowledge())
```

### 自定义意图类型
在 `backend/app/core/intent_analyzer.py` 中添加新的意图模式：
```python
self.intent_patterns["custom_intent"] = ["关键词1", "关键词2"]
```

## 故障排除

### 常见问题

1. **依赖安装失败**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

2. **向量数据库初始化失败**
   ```bash
   rm -rf data/vector_db
   python scripts/setup.py
   ```

3. **API调用失败**
   - 检查网络连接
   - 验证API密钥有效性
   - 确认API服务地址正确

## 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 联系方式

- 项目主页: [GitHub Repository](https://github.com/your-username/ai-workflow-platform)
- 问题反馈: [Issues](https://github.com/your-username/ai-workflow-platform/issues)
- 使用文档: [USAGE.md](USAGE.md)

---

⭐ 如果这个项目对你有帮助，请给个星标支持！