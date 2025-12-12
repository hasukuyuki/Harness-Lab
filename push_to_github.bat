@echo off
echo 🚀 AI工作流平台 - GitHub上传脚本
echo ================================

echo.
echo 请先在GitHub上创建新仓库: ai-workflow-platform
echo 然后输入您的GitHub用户名:
set /p username="GitHub用户名: "

echo.
echo 正在连接到GitHub仓库...
git remote add origin https://github.com/%username%/ai-workflow-platform.git

echo.
echo 正在推送代码到GitHub...
git branch -M main
git push -u origin main

echo.
echo ✅ 完成! 您的代码已上传到:
echo https://github.com/%username%/ai-workflow-platform

pause