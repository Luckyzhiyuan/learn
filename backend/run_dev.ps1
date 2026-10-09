# ===== 后端开发启动脚本（Windows PowerShell）=====
# 首次使用前先安装依赖：
#   python -m venv .venv && .\.venv\Scripts\Activate.ps1
#   pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# 使用项目虚拟环境（若存在）
if (Test-Path ".venv\Scripts\python.exe") {
    $PY = ".venv\Scripts\python.exe"
} else {
    $PY = "python"
}

Write-Host "[1/2] 运行一次元数据采集(对 sample_ddls 建表文件入库)..." -ForegroundColor Cyan
& $PY -c "from app.services.metadata_ingest import sync_from_files; print(sync_from_files())"

Write-Host "[2/2] 启动后端服务 http://$($env:APP_HOST ?? '127.0.0.1'):8000 ..." -ForegroundColor Cyan
& $PY -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload