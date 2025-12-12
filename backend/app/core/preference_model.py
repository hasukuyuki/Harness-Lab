import sqlite3
import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

class PreferenceModel:
    """偏好模型模块 - 基于用户反馈的偏好学习"""
    
    def __init__(self, db_path: str = "data/preferences.db"):
        self.db_path = db_path
        
        # 确保目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 初始化数据库
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 用户偏好表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_preferences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                preference_type TEXT NOT NULL,
                preference_value TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 反馈记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feedback_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                rating INTEGER NOT NULL,
                feedback_text TEXT,
                intent_category TEXT,
                query_text TEXT,
                response_quality REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 偏好模式表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS preference_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                pattern_type TEXT NOT NULL,
                pattern_data TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                usage_count INTEGER DEFAULT 0,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def update_preferences(self, workflow_id: str, rating: int, feedback: Optional[str] = None, 
                               user_id: str = "default", workflow_data: Dict[str, Any] = None):
        """更新用户偏好"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 1. 记录反馈
            intent_category = workflow_data.get("intent", {}).get("category") if workflow_data else None
            query_text = workflow_data.get("query") if workflow_data else None
            
            cursor.execute('''
                INSERT INTO feedback_records 
                (workflow_id, user_id, rating, feedback_text, intent_category, query_text, response_quality)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (workflow_id, user_id, rating, feedback, intent_category, query_text, rating / 5.0))
            
            # 2. 分析反馈并更新偏好
            await self._analyze_feedback(cursor, user_id, rating, workflow_data)
            
            # 3. 更新偏好模式
            await self._update_preference_patterns(cursor, user_id, rating, workflow_data)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    async def _analyze_feedback(self, cursor, user_id: str, rating: int, workflow_data: Dict[str, Any]):
        """分析反馈并提取偏好"""
        
        if not workflow_data:
            return
        
        # 分析意图偏好
        intent_category = workflow_data.get("intent", {}).get("category")
        if intent_category:
            preference_weight = self._rating_to_weight(rating)
            self._update_preference(cursor, user_id, "intent_preference", intent_category, preference_weight)
        
        # 分析响应长度偏好
        reasoning_output = workflow_data.get("reasoning", {}).get("output", {})
        if reasoning_output:
            content_length = len(reasoning_output.get("content", ""))
            length_category = self._categorize_length(content_length)
            preference_weight = self._rating_to_weight(rating)
            self._update_preference(cursor, user_id, "response_length", length_category, preference_weight)
        
        # 分析知识使用偏好
        knowledge_used = len(workflow_data.get("knowledge", []))
        if knowledge_used > 0:
            knowledge_preference = "high" if knowledge_used >= 3 else "medium" if knowledge_used >= 1 else "low"
            preference_weight = self._rating_to_weight(rating)
            self._update_preference(cursor, user_id, "knowledge_usage", knowledge_preference, preference_weight)
    
    def _update_preference(self, cursor, user_id: str, preference_type: str, preference_value: str, weight: float):
        """更新单个偏好项"""
        
        # 检查是否已存在
        cursor.execute('''
            SELECT id, weight FROM user_preferences 
            WHERE user_id = ? AND preference_type = ? AND preference_value = ?
        ''', (user_id, preference_type, preference_value))
        
        existing = cursor.fetchone()
        
        if existing:
            # 更新现有偏好（加权平均）
            old_weight = existing[1]
            new_weight = (old_weight * 0.8 + weight * 0.2)  # 指数移动平均
            
            cursor.execute('''
                UPDATE user_preferences 
                SET weight = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_weight, existing[0]))
        else:
            # 创建新偏好
            cursor.execute('''
                INSERT INTO user_preferences (user_id, preference_type, preference_value, weight)
                VALUES (?, ?, ?, ?)
            ''', (user_id, preference_type, preference_value, weight))
    
    async def _update_preference_patterns(self, cursor, user_id: str, rating: int, workflow_data: Dict[str, Any]):
        """更新偏好模式"""
        
        if not workflow_data:
            return
        
        # 构建模式数据
        pattern_data = {
            "intent": workflow_data.get("intent", {}).get("category"),
            "task_count": len(workflow_data.get("tasks", [])),
            "knowledge_count": len(workflow_data.get("knowledge", [])),
            "execution_time": workflow_data.get("execution_time", 0),
            "rating": rating
        }
        
        pattern_json = json.dumps(pattern_data)
        confidence = rating / 5.0
        
        # 查找相似模式
        cursor.execute('''
            SELECT id, confidence, usage_count FROM preference_patterns
            WHERE user_id = ? AND pattern_type = 'workflow_pattern'
        ''', (user_id,))
        
        patterns = cursor.fetchall()
        
        # 简单的模式匹配（实际应用中可以使用更复杂的相似度计算）
        similar_pattern = None
        for pattern in patterns:
            # 这里可以实现更复杂的模式相似度计算
            similar_pattern = pattern
            break
        
        if similar_pattern:
            # 更新现有模式
            new_confidence = (similar_pattern[1] * 0.8 + confidence * 0.2)
            new_usage_count = similar_pattern[2] + 1
            
            cursor.execute('''
                UPDATE preference_patterns 
                SET confidence = ?, usage_count = ?, last_used = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_confidence, new_usage_count, similar_pattern[0]))
        else:
            # 创建新模式
            cursor.execute('''
                INSERT INTO preference_patterns (user_id, pattern_type, pattern_data, confidence, usage_count)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, "workflow_pattern", pattern_json, confidence, 1))
    
    async def get_user_preferences(self, user_id: str = "default") -> Dict[str, Any]:
        """获取用户偏好"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 获取偏好数据
            cursor.execute('''
                SELECT preference_type, preference_value, weight 
                FROM user_preferences 
                WHERE user_id = ?
                ORDER BY weight DESC
            ''', (user_id,))
            
            preferences = {}
            for pref_type, pref_value, weight in cursor.fetchall():
                if pref_type not in preferences:
                    preferences[pref_type] = []
                preferences[pref_type].append({
                    "value": pref_value,
                    "weight": weight
                })
            
            # 获取偏好模式
            cursor.execute('''
                SELECT pattern_type, pattern_data, confidence, usage_count
                FROM preference_patterns
                WHERE user_id = ?
                ORDER BY confidence DESC, usage_count DESC
            ''', (user_id,))
            
            patterns = []
            for pattern_type, pattern_data, confidence, usage_count in cursor.fetchall():
                patterns.append({
                    "type": pattern_type,
                    "data": json.loads(pattern_data),
                    "confidence": confidence,
                    "usage_count": usage_count
                })
            
            return {
                "preferences": preferences,
                "patterns": patterns,
                "user_id": user_id
            }
            
        finally:
            conn.close()
    
    def _rating_to_weight(self, rating: int) -> float:
        """将评分转换为权重"""
        # 1-5分转换为0.1-1.0权重
        return max(0.1, rating / 5.0)
    
    def _categorize_length(self, length: int) -> str:
        """将内容长度分类"""
        if length < 100:
            return "short"
        elif length < 500:
            return "medium"
        else:
            return "long"
    
    async def get_recommendation(self, user_id: str, intent_category: str) -> Dict[str, Any]:
        """基于偏好获取推荐配置"""
        
        preferences = await self.get_user_preferences(user_id)
        
        recommendation = {
            "response_style": "balanced",
            "knowledge_usage": "medium",
            "detail_level": "medium"
        }
        
        # 根据意图偏好调整
        intent_prefs = preferences.get("preferences", {}).get("intent_preference", [])
        for pref in intent_prefs:
            if pref["value"] == intent_category and pref["weight"] > 0.7:
                recommendation["confidence_boost"] = True
        
        # 根据响应长度偏好调整
        length_prefs = preferences.get("preferences", {}).get("response_length", [])
        if length_prefs:
            top_length_pref = length_prefs[0]
            recommendation["response_style"] = top_length_pref["value"]
        
        # 根据知识使用偏好调整
        knowledge_prefs = preferences.get("preferences", {}).get("knowledge_usage", [])
        if knowledge_prefs:
            top_knowledge_pref = knowledge_prefs[0]
            recommendation["knowledge_usage"] = top_knowledge_pref["value"]
        
        return recommendation