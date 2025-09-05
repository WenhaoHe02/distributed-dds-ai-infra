@echo off
REM 激活 conda 环境
call conda activate rapidocr

REM 检查是否成功激活环境
if errorlevel 1 (
    echo 错误：无法激活 conda 环境 "rapidocr"
    pause
    exit /b 1
)

REM 运行 Python 脚本
python task_runner.py

REM 检查 Python 脚本执行状态
if errorlevel 1 (
    echo 错误：Python 脚本执行失败
    pause
    exit /b 1
)

echo 任务执行完成
pause