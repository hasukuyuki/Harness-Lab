#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI工作流平台启动脚本
"""

import os
import sys
import subprocess
import time
import webbrowser
from pathlib import Path

# 设置控制台编码
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def check_environment():
    """检查环境配置"""
    if not os.path.exists(".env"):
        print("❌ 未找到 .env 文件，请先运行 python scripts/setup.py")
        return False
    
    # 检查必要目录
    required_dirs = ["data", "data/vector_db"]
    for directory in required_dirs:
        if not os.path.exists(directory):
            print(f"❌ 未找到目录: {directory}")
            return False
    
    return True

def start_backend():
    """启动后端服务"""
    print("🚀 启动后端服务...")
    try:
        # 启动FastAPI服务
        backend_process = subprocess.Popen([
            sys.executable, "-m", "backend.app.main"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # 等待服务启动
        time.sleep(3)
        
        # 检查服务是否正常启动
        if backend_process.poll() is None:
            print("✓ 后端服务启动成功 (http://localhost:8000)")
            return backend_process
        else:
            stdout, stderr = backend_process.communicate()
            print(f"❌ 后端服务启动失败:")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return None
            
    except Exception as e:
        print(f"❌ 启动后端服务时出错: {e}")
        return None

def start_frontend():
    """启动前端服务"""
    print("🌐 启动前端服务...")
    
    # 检查是否有HTTP服务器可用
    try:
        # 尝试使用Python内置HTTP服务器
        frontend_process = subprocess.Popen([
            sys.executable, "-m", "http.server", "3000", "--directory", "frontend"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        time.sleep(2)
        
        if frontend_process.poll() is None:
            print("✓ 前端服务启动成功 (http://localhost:3000)")
            return frontend_process
        else:
            print("❌ 前端服务启动失败")
            return None
            
    except Exception as e:
        print(f"❌ 启动前端服务时出错: {e}")
        return None

def check_service_health():
    """检查服务健康状态"""
    import requests
    
    try:
        # 检查后端健康状态
        response = requests.get("http://localhost:8000/api/health", timeout=5)
        if response.status_code == 200:
            print("✓ 后端服务健康检查通过")
            return True
        else:
            print(f"❌ 后端服务健康检查失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ 无法连接到后端服务: {e}")
        return False

def open_browser():
    """打开浏览器"""
    try:
        webbrowser.open("http://localhost:3000")
        print("🌐 已在浏览器中打开应用")
    except Exception as e:
        print(f"⚠️  无法自动打开浏览器: {e}")
        print("请手动访问: http://localhost:3000")

def main():
    """主函数"""
    print("🚀 启动AI工作流平台...")
    print("=" * 50)
    
    # 检查环境
    if not check_environment():
        sys.exit(1)
    
    # 启动后端服务
    backend_process = start_backend()
    if not backend_process:
        sys.exit(1)
    
    # 等待后端完全启动
    print("⏳ 等待后端服务完全启动...")
    time.sleep(5)
    
    # 健康检查
    if not check_service_health():
        print("❌ 后端服务未正常启动")
        backend_process.terminate()
        sys.exit(1)
    
    # 启动前端服务
    frontend_process = start_frontend()
    if not frontend_process:
        print("⚠️  前端服务启动失败，但后端服务正常")
        print("您可以直接访问 http://localhost:8000/docs 查看API文档")
    
    print("=" * 50)
    print("🎉 服务启动完成！")
    print("\n可用服务:")
    print("- 后端API: http://localhost:8000")
    print("- API文档: http://localhost:8000/docs")
    if frontend_process:
        print("- 前端界面: http://localhost:3000")
    
    # 打开浏览器
    if frontend_process:
        time.sleep(2)
        open_browser()
    
    print("\n按 Ctrl+C 停止服务")
    
    try:
        # 保持服务运行
        while True:
            time.sleep(1)
            
            # 检查进程是否还在运行
            if backend_process.poll() is not None:
                print("❌ 后端服务意外停止")
                break
                
            if frontend_process and frontend_process.poll() is not None:
                print("⚠️  前端服务意外停止")
                frontend_process = None
                
    except KeyboardInterrupt:
        print("\n🛑 正在停止服务...")
        
        # 停止后端服务
        if backend_process:
            backend_process.terminate()
            backend_process.wait()
            print("✓ 后端服务已停止")
        
        # 停止前端服务
        if frontend_process:
            frontend_process.terminate()
            frontend_process.wait()
            print("✓ 前端服务已停止")
        
        print("👋 再见！")

if __name__ == "__main__":
    main()