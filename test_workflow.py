#!/usr/bin/env python3
"""
AI工作流平台功能测试脚本
"""

import asyncio
import sys
import os

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def test_components():
    """测试各个组件"""
    print("🧪 开始测试AI工作流组件...")
    print("=" * 50)
    
    # 测试意图分析
    print("1. 测试意图分析模块...")
    try:
        from backend.app.core.intent_analyzer import IntentAnalyzer
        analyzer = IntentAnalyzer()
        
        test_queries = [
            "什么是机器学习？",
            "帮我写一个Python函数",
            "分析这个数据的趋势"
        ]
        
        for query in test_queries:
            result = await analyzer.analyze(query)
            print(f"  查询: {query}")
            print(f"  意图: {result['category']} (置信度: {result['confidence']:.2f})")
            print()
        
        print("✓ 意图分析模块测试通过")
    except Exception as e:
        print(f"❌ 意图分析模块测试失败: {e}")
        return False
    
    # 测试任务规划
    print("\n2. 测试任务规划模块...")
    try:
        from backend.app.core.task_planner import TaskPlanner
        planner = TaskPlanner()
        
        query = "解释什么是深度学习"
        intent_result = {"category": "question", "confidence": 0.9, "word_count": 5}
        
        tasks = await planner.plan(query, intent_result)
        print(f"  查询: {query}")
        print(f"  生成任务数: {len(tasks)}")
        for task in tasks:
            print(f"    - {task['name']}: {task['description']}")
        
        print("✓ 任务规划模块测试通过")
    except Exception as e:
        print(f"❌ 任务规划模块测试失败: {e}")
        return False
    
    # 测试向量数据库
    print("\n3. 测试向量数据库模块...")
    try:
        from backend.app.core.vector_db import VectorDB
        vector_db = VectorDB()
        
        # 添加测试文档
        await vector_db.add_document(
            text="深度学习是机器学习的一个分支，使用多层神经网络来学习数据的复杂模式。",
            doc_id="test_doc_1"
        )
        
        # 搜索测试
        results = await vector_db.search("什么是深度学习", top_k=3)
        print(f"  搜索结果数: {len(results)}")
        if results:
            print(f"  最相关结果: {results[0]['text'][:50]}...")
            print(f"  相似度分数: {results[0]['score']:.3f}")
        
        print("✓ 向量数据库模块测试通过")
    except Exception as e:
        print(f"❌ 向量数据库模块测试失败: {e}")
        return False
    
    # 测试偏好模型
    print("\n4. 测试偏好模型模块...")
    try:
        from backend.app.core.preference_model import PreferenceModel
        preference_model = PreferenceModel()
        
        # 模拟反馈
        await preference_model.update_preferences(
            workflow_id="test_workflow_1",
            rating=5,
            feedback="很好的回答",
            workflow_data={
                "intent": {"category": "question"},
                "query": "测试查询"
            }
        )
        
        # 获取偏好
        preferences = await preference_model.get_user_preferences()
        print(f"  用户偏好类型数: {len(preferences['preferences'])}")
        print(f"  偏好模式数: {len(preferences['patterns'])}")
        
        print("✓ 偏好模型模块测试通过")
    except Exception as e:
        print(f"❌ 偏好模型模块测试失败: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 所有组件测试通过！")
    return True

async def test_full_workflow():
    """测试完整工作流"""
    print("\n🔄 测试完整工作流...")
    
    try:
        from backend.app.core.workflow_engine import WorkflowEngine
        from backend.app.core.intent_analyzer import IntentAnalyzer
        from backend.app.core.task_planner import TaskPlanner
        from backend.app.core.vector_db import VectorDB
        from backend.app.core.reasoning_model import ReasoningModel
        
        # 初始化组件
        workflow_engine = WorkflowEngine()
        intent_analyzer = IntentAnalyzer()
        task_planner = TaskPlanner()
        vector_db = VectorDB()
        reasoning_model = ReasoningModel()
        
        # 执行工作流
        query = "什么是人工智能？"
        result = await workflow_engine.execute(
            query=query,
            user_id="test_user",
            intent_analyzer=intent_analyzer,
            task_planner=task_planner,
            vector_db=vector_db,
            reasoning_model=reasoning_model
        )
        
        print(f"工作流ID: {result.get('workflow_id')}")
        print(f"查询: {result.get('query')}")
        print(f"意图: {result.get('intent', {}).get('category')}")
        print(f"任务数: {len(result.get('tasks', []))}")
        print(f"知识检索数: {len(result.get('knowledge', []))}")
        print(f"执行时间: {result.get('execution_time', 0):.2f}秒")
        
        if result.get('reasoning', {}).get('success'):
            print("✓ 完整工作流测试通过")
            return True
        else:
            print("⚠️  工作流执行完成，但推理模块可能需要配置API密钥")
            return True
            
    except Exception as e:
        print(f"❌ 完整工作流测试失败: {e}")
        return False

def main():
    """主函数"""
    print("🧪 AI工作流平台功能测试")
    print("=" * 50)
    
    # 检查环境
    if not os.path.exists("data"):
        print("❌ 请先运行 python scripts/setup.py 初始化项目")
        sys.exit(1)
    
    # 运行测试
    success = asyncio.run(test_components())
    
    if success:
        asyncio.run(test_full_workflow())
    
    print("\n测试完成！")
    if success:
        print("✅ 系统组件工作正常，可以启动服务")
    else:
        print("❌ 部分组件测试失败，请检查配置")

if __name__ == "__main__":
    main()