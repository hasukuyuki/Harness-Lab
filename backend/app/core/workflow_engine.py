import asyncio
import uuid
from typing import Dict, Any, List
from datetime import datetime

class WorkflowEngine:
    """工作流引擎 - 协调各个模块的执行"""
    
    def __init__(self):
        self.workflows = {}
    
    async def execute(self, query: str, user_id: str, intent_analyzer, task_planner, vector_db, reasoning_model) -> Dict[str, Any]:
        """执行完整的AI工作流"""
        workflow_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        try:
            # 1. 语义分析 - 识别意图
            intent_result = await intent_analyzer.analyze(query)
            
            # 2. 任务拆解
            tasks = await task_planner.plan(query, intent_result)
            
            # 3. 知识检索
            knowledge = await vector_db.search(query, top_k=5)
            
            # 4. 引导式思考和推理
            reasoning_result = await reasoning_model.reason(
                query=query,
                intent=intent_result,
                tasks=tasks,
                knowledge=knowledge
            )
            
            # 5. 构建结果
            result = {
                "workflow_id": workflow_id,
                "query": query,
                "intent": intent_result,
                "tasks": tasks,
                "knowledge": knowledge,
                "reasoning": reasoning_result,
                "timestamp": start_time.isoformat(),
                "execution_time": (datetime.now() - start_time).total_seconds()
            }
            
            # 6. 存储工作流结果
            self.workflows[workflow_id] = result
            
            # 7. 向量化并存储结果（异步）
            asyncio.create_task(self._store_result(result, vector_db))
            
            return result
            
        except Exception as e:
            return {
                "workflow_id": workflow_id,
                "error": str(e),
                "timestamp": start_time.isoformat()
            }
    
    async def _store_result(self, result: Dict[str, Any], vector_db):
        """将结果向量化并存储到知识库"""
        try:
            # 构建存储文本
            storage_text = f"""
            查询: {result['query']}
            意图: {result['intent']['category']}
            推理结果: {result['reasoning']['output']}
            """
            
            # 向量化并存储
            await vector_db.add_document(storage_text, result['workflow_id'])
        except Exception as e:
            print(f"存储结果失败: {e}")
    
    def get_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """获取工作流结果"""
        return self.workflows.get(workflow_id)