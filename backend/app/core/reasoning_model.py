import openai
import os
from typing import Dict, Any, List
from dotenv import load_dotenv

load_dotenv()

class ReasoningModel:
    """推理模型模块 - 使用大语言模型进行引导式思考"""
    
    def __init__(self):
        # 配置OpenAI API
        self.client = openai.OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
        
        # 推理模板
        self.reasoning_templates = {
            "question": """
你是一个专业的问答助手。请基于以下信息回答用户问题：

用户问题: {query}
意图分析: {intent}
任务计划: {tasks}
相关知识: {knowledge}

请按照以下步骤进行思考：
1. 理解问题的核心
2. 分析相关知识的适用性
3. 进行逻辑推理
4. 组织结构化答案

要求：
- 答案要准确、完整
- 引用相关知识来源
- 保持逻辑清晰
- 如果信息不足，请明确指出
""",
            
            "task": """
你是一个任务执行助手。请帮助用户完成以下任务：

用户任务: {query}
意图分析: {intent}
任务计划: {tasks}
相关知识: {knowledge}

请按照以下步骤执行：
1. 明确任务目标和要求
2. 分析可用资源和约束
3. 制定详细执行方案
4. 预测可能的问题和解决方案

要求：
- 方案要可行、具体
- 考虑实际约束条件
- 提供备选方案
- 包含质量检查步骤
""",
            
            "analysis": """
你是一个数据分析专家。请对以下内容进行分析：

分析对象: {query}
意图分析: {intent}
任务计划: {tasks}
相关知识: {knowledge}

请按照以下框架进行分析：
1. 数据概览和背景
2. 关键指标和趋势
3. 深入分析和洞察
4. 结论和建议

要求：
- 分析要客观、深入
- 使用数据支撑观点
- 提供可行的建议
- 标注分析的局限性
"""
        }
    
    async def reason(self, query: str, intent: Dict[str, Any], tasks: List[Dict[str, Any]], 
                    knowledge: List[Dict[str, Any]]) -> Dict[str, Any]:
        """执行推理过程"""
        
        intent_category = intent.get("category", "question")
        
        # 1. 选择推理模板
        template = self.reasoning_templates.get(intent_category, self.reasoning_templates["question"])
        
        # 2. 准备上下文信息
        context = self._prepare_context(query, intent, tasks, knowledge)
        
        # 3. 构建提示词
        prompt = template.format(**context)
        
        # 4. 调用大语言模型
        try:
            response = await self._call_llm(prompt)
            
            # 5. 后处理和结构化输出
            structured_output = self._structure_output(response, intent_category)
            
            return {
                "success": True,
                "output": structured_output,
                "raw_response": response,
                "prompt_used": prompt[:200] + "...",  # 截断显示
                "model_info": {
                    "model": "gpt-3.5-turbo",
                    "temperature": 0.7
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "fallback_output": self._generate_fallback(query, intent_category)
            }
    
    def _prepare_context(self, query: str, intent: Dict[str, Any], tasks: List[Dict[str, Any]], 
                        knowledge: List[Dict[str, Any]]) -> Dict[str, str]:
        """准备上下文信息"""
        
        # 格式化任务信息
        tasks_text = "\n".join([f"- {task['name']}: {task['description']}" for task in tasks])
        
        # 格式化知识信息
        knowledge_text = "\n".join([
            f"- {item['doc_id']}: {item['text'][:200]}..." 
            for item in knowledge[:3]  # 只取前3个最相关的
        ])
        
        # 格式化意图信息
        intent_text = f"类别: {intent['category']}, 置信度: {intent['confidence']:.2f}"
        
        return {
            "query": query,
            "intent": intent_text,
            "tasks": tasks_text,
            "knowledge": knowledge_text if knowledge_text else "暂无相关知识"
        }
    
    async def _call_llm(self, prompt: str) -> str:
        """调用大语言模型"""
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "你是一个专业的AI助手，擅长分析问题和提供解决方案。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            # 如果OpenAI API不可用，使用本地模型或规则
            raise Exception(f"LLM调用失败: {e}")
    
    def _structure_output(self, response: str, intent_category: str) -> Dict[str, Any]:
        """结构化输出结果"""
        
        # 基础结构化
        structured = {
            "content": response,
            "type": intent_category,
            "sections": self._extract_sections(response),
            "key_points": self._extract_key_points(response),
            "confidence": self._estimate_confidence(response)
        }
        
        return structured
    
    def _extract_sections(self, text: str) -> List[Dict[str, str]]:
        """提取文本段落"""
        sections = []
        paragraphs = text.split('\n\n')
        
        for i, paragraph in enumerate(paragraphs):
            if paragraph.strip():
                sections.append({
                    "id": f"section_{i+1}",
                    "content": paragraph.strip()
                })
        
        return sections
    
    def _extract_key_points(self, text: str) -> List[str]:
        """提取关键点"""
        # 简单的关键点提取
        key_points = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith(('- ', '• ', '1. ', '2. ', '3. ')):
                key_points.append(line)
        
        return key_points[:5]  # 最多5个关键点
    
    def _estimate_confidence(self, response: str) -> float:
        """估算回答的置信度"""
        # 简单的置信度估算
        confidence_indicators = ["确定", "明确", "显然", "肯定"]
        uncertainty_indicators = ["可能", "也许", "大概", "不确定", "可能性"]
        
        confidence_score = 0.5  # 基础分数
        
        for indicator in confidence_indicators:
            if indicator in response:
                confidence_score += 0.1
        
        for indicator in uncertainty_indicators:
            if indicator in response:
                confidence_score -= 0.1
        
        return max(0.1, min(1.0, confidence_score))
    
    def _generate_fallback(self, query: str, intent_category: str) -> Dict[str, Any]:
        """生成备用回答"""
        fallback_responses = {
            "question": f"抱歉，我无法完全回答您的问题：{query}。请提供更多信息或尝试重新表述。",
            "task": f"抱歉，我无法完成任务：{query}。请检查任务描述是否清晰完整。",
            "analysis": f"抱歉，我无法分析：{query}。请提供更多数据或明确分析要求。"
        }
        
        return {
            "content": fallback_responses.get(intent_category, "抱歉，我无法处理您的请求。"),
            "type": "fallback",
            "sections": [],
            "key_points": [],
            "confidence": 0.1
        }