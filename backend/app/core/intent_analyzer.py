import re
from typing import Dict, Any, List

class IntentAnalyzer:
    """意图分析模块 - 识别用户输入的意图"""
    
    def __init__(self):
        # 预定义意图类别和关键词
        self.intent_patterns = {
            "question": ["什么", "如何", "怎么", "为什么", "?", "？"],
            "task": ["帮我", "请", "需要", "想要", "执行", "完成"],
            "search": ["查找", "搜索", "找", "检索"],
            "analysis": ["分析", "评估", "比较", "总结"],
            "creation": ["创建", "生成", "制作", "写", "设计"],
            "learning": ["学习", "教", "解释", "理解"]
        }
        
        # 简化版本，不使用transformers
        self.sentiment_analyzer = None
    
    async def analyze(self, query: str) -> Dict[str, Any]:
        """分析用户查询的意图"""
        
        # 1. 基于规则的意图识别
        intent_scores = {}
        query_lower = query.lower()
        
        for intent, keywords in self.intent_patterns.items():
            score = 0
            for keyword in keywords:
                if keyword in query_lower:
                    score += 1
            intent_scores[intent] = score
        
        # 2. 确定主要意图
        primary_intent = max(intent_scores, key=intent_scores.get) if intent_scores else "general"
        confidence = intent_scores.get(primary_intent, 0) / len(query.split()) if query.split() else 0
        
        # 3. 提取实体和关键词
        entities = self._extract_entities(query)
        keywords = self._extract_keywords(query)
        
        # 4. 情感分析（如果模型可用）
        sentiment = None
        if self.sentiment_analyzer:
            try:
                sentiment_result = self.sentiment_analyzer(query)[0]
                sentiment = {
                    "label": sentiment_result["label"],
                    "score": sentiment_result["score"]
                }
            except:
                pass
        
        return {
            "category": primary_intent,
            "confidence": confidence,
            "intent_scores": intent_scores,
            "entities": entities,
            "keywords": keywords,
            "sentiment": sentiment,
            "query_length": len(query),
            "word_count": len(query.split())
        }
    
    def _extract_entities(self, query: str) -> List[str]:
        """简单的实体提取"""
        # 提取可能的实体（数字、日期、专有名词等）
        entities = []
        
        # 数字
        numbers = re.findall(r'\d+', query)
        entities.extend([f"NUMBER:{num}" for num in numbers])
        
        # 日期模式
        dates = re.findall(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}', query)
        entities.extend([f"DATE:{date}" for date in dates])
        
        # 大写开头的词（可能是专有名词）
        proper_nouns = re.findall(r'\b[A-Z][a-z]+\b', query)
        entities.extend([f"PROPER:{noun}" for noun in proper_nouns])
        
        return entities
    
    def _extract_keywords(self, query: str) -> List[str]:
        """提取关键词"""
        # 简单的关键词提取（去除停用词）
        stop_words = {"的", "是", "在", "有", "和", "与", "或", "但", "然而", "因为", "所以"}
        
        words = query.split()
        keywords = [word for word in words if word not in stop_words and len(word) > 1]
        
        return keywords[:10]  # 返回前10个关键词