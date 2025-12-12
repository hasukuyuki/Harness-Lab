#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI工作流平台初始化脚本
"""

import os
import sys
import subprocess
import sqlite3
from pathlib import Path

# 设置控制台编码
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def create_directories():
    """创建必要的目录结构"""
    directories = [
        "data",
        "data/vector_db", 
        "data/knowledge",
        "models",
        "logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ 创建目录: {directory}")

def setup_environment():
    """设置环境变量"""
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            import shutil
            shutil.copy(".env.example", ".env")
            print("✓ 创建 .env 文件")
            print("⚠️  请编辑 .env 文件，配置您的 API 密钥")
        else:
            print("❌ 未找到 .env.example 文件")
            return False
    return True

def install_dependencies():
    """安装Python依赖"""
    try:
        print("📦 安装Python依赖...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True)
        print("✓ Python依赖安装完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ 依赖安装失败: {e}")
        return False

def initialize_database():
    """初始化数据库"""
    try:
        # 初始化偏好数据库
        from backend.app.core.preference_model import PreferenceModel
        preference_model = PreferenceModel()
        print("✓ 偏好数据库初始化完成")
        
        # 初始化向量数据库
        from backend.app.core.vector_db import VectorDB
        vector_db = VectorDB()
        print("✓ 向量数据库初始化完成")
        
        return True
    except Exception as e:
        print(f"❌ 数据库初始化失败: {e}")
        return False

def add_sample_knowledge():
    """添加示例知识库"""
    try:
        from backend.app.core.vector_db import VectorDB
        import asyncio
        
        vector_db = VectorDB()
        
        # 示例知识
        sample_knowledge = [
            {
                "text": "Python是一种高级编程语言，具有简洁的语法和强大的功能。它广泛用于Web开发、数据科学、人工智能等领域。",
                "doc_id": "python_intro",
                "metadata": {"category": "programming", "language": "python"}
            },
            {
                "text": "机器学习是人工智能的一个分支，通过算法让计算机从数据中学习模式，无需明确编程。常见算法包括线性回归、决策树、神经网络等。",
                "doc_id": "ml_intro", 
                "metadata": {"category": "ai", "topic": "machine_learning"}
            },
            {
                "text": "FastAPI是一个现代、快速的Python Web框架，用于构建API。它基于标准Python类型提示，具有自动API文档生成、高性能等特点。",
                "doc_id": "fastapi_intro",
                "metadata": {"category": "web", "framework": "fastapi"}
            }
        ]
        
        async def add_knowledge():
            for item in sample_knowledge:
                await vector_db.add_document(
                    text=item["text"],
                    doc_id=item["doc_id"], 
                    metadata=item["metadata"]
                )
        
        asyncio.run(add_knowledge())
        print("✓ 示例知识库添加完成")
        return True
        
    except Exception as e:
        print(f"❌ 知识库初始化失败: {e}")
        return False

def check_requirements():
    """检查系统要求"""
    print("🔍 检查系统要求...")
    
    # 检查Python版本
    if sys.version_info < (3, 8):
        print("❌ 需要Python 3.8或更高版本")
        return False
    print(f"✓ Python版本: {sys.version}")
    
    # 检查pip
    try:
        import pip
        print("✓ pip可用")
    except ImportError:
        print("❌ 未找到pip")
        return False
    
    return True

def main():
    """主函数"""
    print("🚀 AI工作流平台初始化开始...")
    print("=" * 50)
    
    # 检查系统要求
    if not check_requirements():
        sys.exit(1)
    
    # 创建目录
    create_directories()
    
    # 设置环境
    if not setup_environment():
        sys.exit(1)
    
    # 安装依赖
    if not install_dependencies():
        sys.exit(1)
    
    # 初始化数据库
    if not initialize_database():
        sys.exit(1)
    
    # 添加示例知识
    if not add_sample_knowledge():
        print("⚠️  示例知识库添加失败，但不影响系统运行")
    
    print("=" * 50)
    print("🎉 初始化完成！")
    print("\n下一步:")
    print("1. 编辑 .env 文件，配置您的API密钥")
    print("2. 运行 'python -m backend.app.main' 启动后端服务")
    print("3. 访问 http://localhost:8000/docs 查看API文档")
    print("4. 访问前端界面开始使用")

if __name__ == "__main__":
    main()