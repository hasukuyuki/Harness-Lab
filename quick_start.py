#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI工作流平台快速启动脚本
一键完成初始化和启动
"""

import os
import sys
import subprocess
import time

# 设置控制台编码
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def main():
    print("🚀 AI工作流平台 - 快速启动")
    print("=" * 50)
    
    # 检查Python版本
    if sys.version_info < (3, 8):
        print("❌ 需要Python 3.8或更高版本")
        sys.exit(1)
    
    # 步骤1: 初始化
    print("📋 步骤1: 初始化项目...")
    try:
        result = subprocess.run([sys.executable, "scripts/setup.py"], 
                              capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ 初始化失败:")
            print(result.stderr)
            sys.exit(1)
        print("✓ 初始化完成")
    except Exception as e:
        print(f"❌ 初始化出错: {e}")
        sys.exit(1)
    
    # 步骤2: 检查环境配置
    print("\n📋 步骤2: 检查环境配置...")
    if not os.path.exists(".env"):
        print("⚠️  请配置 .env 文件中的API密钥")
        print("编辑 .env 文件，设置 OPENAI_API_KEY")
        
        # 等待用户确认
        input("配置完成后按回车继续...")
    
    # 步骤3: 启动服务
    print("\n📋 步骤3: 启动服务...")
    try:
        subprocess.run([sys.executable, "scripts/start.py"])
    except KeyboardInterrupt:
        print("\n👋 服务已停止")
    except Exception as e:
        print(f"❌ 启动出错: {e}")

if __name__ == "__main__":
    main()