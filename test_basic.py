#!/usr/bin/env python3
"""
基础功能测试
"""

import sys
import os

def test_imports():
    """测试基本导入"""
    print("Testing basic imports...")
    
    try:
        import fastapi
        print("✓ FastAPI imported successfully")
    except ImportError as e:
        print(f"❌ FastAPI import failed: {e}")
        return False
    
    try:
        import uvicorn
        print("✓ Uvicorn imported successfully")
    except ImportError as e:
        print(f"❌ Uvicorn import failed: {e}")
        return False
    
    try:
        from dotenv import load_dotenv
        print("✓ python-dotenv imported successfully")
    except ImportError as e:
        print(f"❌ python-dotenv import failed: {e}")
        return False
    
    try:
        import requests
        print("✓ requests imported successfully")
    except ImportError as e:
        print(f"❌ requests import failed: {e}")
        return False
    
    return True

def test_directories():
    """测试目录结构"""
    print("\nTesting directory structure...")
    
    required_dirs = [
        "backend",
        "backend/app", 
        "backend/app/core",
        "frontend",
        "data"
    ]
    
    for directory in required_dirs:
        if os.path.exists(directory):
            print(f"✓ {directory} exists")
        else:
            print(f"❌ {directory} missing")
            return False
    
    return True

def test_files():
    """测试关键文件"""
    print("\nTesting key files...")
    
    required_files = [
        "backend/app/main.py",
        "backend/app/core/workflow_engine.py",
        "backend/app/core/intent_analyzer.py",
        "frontend/index.html",
        ".env"
    ]
    
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✓ {file_path} exists")
        else:
            print(f"❌ {file_path} missing")
            return False
    
    return True

def test_simple_server():
    """测试简单服务器启动"""
    print("\nTesting simple server startup...")
    
    try:
        from fastapi import FastAPI
        app = FastAPI()
        
        @app.get("/")
        def read_root():
            return {"message": "AI Workflow Platform is running"}
        
        print("✓ FastAPI app created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Server test failed: {e}")
        return False

def main():
    """主测试函数"""
    print("AI Workflow Platform - Basic Tests")
    print("=" * 50)
    
    tests = [
        ("Import Test", test_imports),
        ("Directory Test", test_directories), 
        ("File Test", test_files),
        ("Server Test", test_simple_server)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        if test_func():
            passed += 1
            print(f"✓ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All basic tests passed! The project structure is ready.")
        print("\nNext steps:")
        print("1. Configure your API key in .env file")
        print("2. Run: python -m backend.app.main")
        print("3. Open http://localhost:8000/docs")
        return True
    else:
        print("❌ Some tests failed. Please check the setup.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)