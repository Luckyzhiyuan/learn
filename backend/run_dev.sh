#!/usr/bin/env bash
# 后端开发启动脚本（Linux / macOS）
set -e
cd "$(dirname "$0")"

if [ -x ".venv/bin/python" ]; then PY=".venv/bin/python"; else PY="python"; fi

echo "[1/2] 元数据采集(对 sample_ddls 建表文件入库)..."
"$PY" -c "from app.services.metadata_ingest import sync_from_files; print(sync_from_files())"

echo "[2/2] 启动后端服务 http://127.0.0.1:8000 ..."
"$PY" -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload