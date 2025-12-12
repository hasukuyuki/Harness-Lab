from typing import Dict, Any, List
import re

class TaskPlanner:
    """任务规划模块 - 将复杂目标拆解为子任务"""
    
    def __init__(self):
        # 任务拆解模板
        self.task_templates = {
            "question": [
                "理解问题核心",
                "收集相关信息", 
                "分析和推理",
                "组织答案"
            ],
            "task": [
                "明确任务目标",
                "分析所需资源",
                "制定执行步骤",
                "验证结果"
            ],
            "search": [
                "确定搜索范围",
                "构建搜索查询",
                "执行搜索",
                "筛选和排序结果"
            ],
            "analysis": [
                "收集数据",
                "数据预处理",
                "执行分析",
                "生成报告"
            ],
            "creation": [
                "明确创作要求",
                "收集素材",
                "执行创作",
                "审查和优化"
            ],
            "learning": [
                "确定学习目标",
                "收集学习资料",
                "组织学习内容",
                "验证理解"
            ]
        }
    
    async def plan(self, query: str, intent_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """根据查询和意图生成任务计划"""
        
        intent_category = intent_result.get("category", "general")
        
        # 1. 获取基础任务模板
        base_tasks = self.task_templates.get(intent_category, [
            "分析输入",
            "处理请求", 
            "生成输出"
        ])
        
        # 2. 根据查询复杂度调整任务
        tasks = self._customize_tasks(query, base_tasks, intent_result)
        
        # 3. 添加任务元数据
        task_list = []
        for i, task_name in enumerate(tasks):
            task = {
                "id": f"task_{i+1}",
                "name": task_name,
                "description": self._generate_task_description(task_name, query),
                "priority": self._calculate_priority(task_name, intent_category),
                "estimated_time": self._estimate_time(task_name),
                "dependencies": self._get_dependencies(i, tasks),
                "status": "pending"
            }
            task_list.append(task)
        
        return task_list
    
    def _customize_tasks(self, query: str, base_tasks: List[str], intent_result: Dict[str, Any]) -> List[str]:
        """根据具体查询定制任务"""
        customized_tasks = base_tasks.copy()
        
        # 根据查询长度和复杂度调整
        word_count = intent_result.get("word_count", 0)
        
        if word_count > 20:  # 复杂查询
            customized_tasks.insert(1, "拆解复杂需求")
        
        # 根据实体类型添加特定任务
        entities = intent_result.get("entities", [])
        if any("DATE:" in entity for entity in entities):
            customized_tasks.insert(-1, "处理时间相关信息")
        
        if any("NUMBER:" in entity for entity in entities):
            customized_tasks.insert(-1, "处理数值计算")
        
        return customized_tasks
    
    def _generate_task_description(self, task_name: str, query: str) -> str:
        """为任务生成描述"""
        descriptions = {
            "理解问题核心": f"分析查询'{query[:50]}...'的核心问题",
            "收集相关信息": "从知识库中检索相关信息",
            "分析和推理": "基于收集的信息进行逻辑推理",
            "组织答案": "将推理结果组织成结构化答案",
            "明确任务目标": "确定任务的具体目标和成功标准",
            "分析所需资源": "评估完成任务所需的资源和工具",
            "制定执行步骤": "制定详细的执行计划",
            "验证结果": "检查任务完成质量"
        }
        
        return descriptions.get(task_name, f"执行任务: {task_name}")
    
    def _calculate_priority(self, task_name: str, intent_category: str) -> int:
        """计算任务优先级 (1-5, 5最高)"""
        high_priority_tasks = ["理解问题核心", "明确任务目标", "确定学习目标"]
        medium_priority_tasks = ["收集相关信息", "分析和推理", "执行分析"]
        
        if task_name in high_priority_tasks:
            return 5
        elif task_name in medium_priority_tasks:
            return 3
        else:
            return 2
    
    def _estimate_time(self, task_name: str) -> int:
        """估算任务时间（秒）"""
        time_estimates = {
            "理解问题核心": 2,
            "收集相关信息": 5,
            "分析和推理": 10,
            "组织答案": 3,
            "明确任务目标": 2,
            "分析所需资源": 3,
            "制定执行步骤": 5,
            "验证结果": 2
        }
        
        return time_estimates.get(task_name, 3)
    
    def _get_dependencies(self, task_index: int, tasks: List[str]) -> List[str]:
        """获取任务依赖关系"""
        if task_index == 0:
            return []
        else:
            return [f"task_{task_index}"]  # 简单的顺序依赖