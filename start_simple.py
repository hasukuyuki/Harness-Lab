#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI工作流平台简化启动脚本
"""

import os
import sys
import subprocess
from pathlib import Path

def create_directories():
    """创建必要的目录"""
    directories = ["data", "data/vector_db", "data/knowledge", "models", "logs"]
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {directory}")

def setup_environment():
    """设置环境"""
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            import shutil
            shutil.copy(".env.example", ".env")
            print("Created .env file from .env.example")
            print("Please edit .env file to configure your API keys")
        else:
            print("Error: .env.example file not found")
            return False
    return True

def install_dependencies():
    """安装依赖"""
    try:
        print("Installing Python dependencies...")
        result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("Dependencies installed successfully")
            return True
        else:
            print(f"Failed to install dependencies: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error installing dependencies: {e}")
        return False

def start_backend():
    """启动后端服务"""
    try:
        print("Starting backend service...")
        print("Backend will start at: http://localhost:8000")
        print("API documentation: http://localhost:8000/docs")
        print("Press Ctrl+C to stop the service")
        
        # 启动后端服务
        subprocess.run([sys.executable, "-m", "backend.app.main"])
        
    except KeyboardInterrupt:
        print("\nService stopped by user")
    except Exception as e:
        print(f"Error starting backend: {e}")

def main():
    """主函数"""
    print("AI Workflow Platform - Simple Startup")
    print("=" * 50)
    
    # 检查Python版本
    if sys.version_info < (3, 8):
        print("Error: Python 3.8 or higher is required")
        sys.exit(1)
    
    print(f"Python version: {sys.version}")
    
    # 创建目录
    create_directories()
    
    # 设置环境
    if not setup_environment():
        sys.exit(1)
    
    # 安装依赖
    if not install_dependencies():
        print("Warning: Failed to install some dependencies")
        print("You may need to install them manually:")
        print("pip install -r requirements.txt")
    
    # 初始化数据库
    try:
        print("Initializing databases...")
        # 简单的数据库初始化
        os.makedirs("data", exist_ok=True)
        print("Database directories created")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")
    
    print("=" * 50)
    print("Setup completed!")
    print("\nNext steps:")
    print("1. Edit .env file to configure your API keys")
    print("2. The backend service will start automatically")
    print("3. Open http://localhost:8000/docs to see API documentation")
    print("4. Open frontend/index.html in your browser to use the interface")
    
    # 启动后端
    start_backend()

if __name__ == "__main__":
    main()