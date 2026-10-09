# 智能数仓平台

酒旅数仓数据研发全链路助手：元数据管理、SQL 智能建模/优化/评估、NL2SQL、数据质量、离线调度、血缘解析、飞书通知，LLM 网关接入千问 Qwen。

## 技术栈
- **后端**：Python 3.12 · FastAPI · SQLAlchemy 2.0 · Pydantic v2 · Celery · sqlglot(hive)
- **数据库**：SQLite（开发默认）/ MySQL 8.0(utf8mb4)
- **检索**：Elasticsearch 8（不可用自动回退 SQL）
- **安全**：JWT + AES-GCM(仅加密 LLM Key) + bcrypt
- **LLM**：千问 Qwen3-8b（OpenAI 兼容接口）

## 目录结构
```
backend/
  app/            # 后端源码（config/core/db/schemas/services/routers/tasks/main.py）
  scripts/        # smoke_test.py 冒烟测试等
  sample_ddls/    # 示例建表文件(供元数据 file 模式采集)
  .env            # 本地开发配置(SQLite/直跑)
  .env.example    # 真实环境模板(MySQL/ES/Redis/千问)
  run_dev.ps1     # Windows 启动脚本
  run_dev.sh      # Linux/macOS 启动脚本
docker/           # docker-compose(MySQL8/Redis/ES) + init/init.sql
```

## 快速启动（开发直跑模式，无需外部 Depend)
1. 安装依赖
   ```bash
   cd backend
   python -m venv .venv && .\.venv\Scripts\Activate.ps1   # 或 source .venv/bin/activate
   pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
   ```
2. 启动后端（自动建表 + 注入种子数据 + 采集 sample_ddls 元数据）
   - Windows: `.\run_dev.ps1`  或
   - 手动: `python -m uvicorn app.main:app --reload`
3. 访问
   - Swagger 文档: http://127.0.0.1:8000/docs
   - 健康检查: http://127.0.0.1:8000/api/v1/health

冒烟测试（可选）：
```bash
python scripts/smoke_test.py
```

## 切换到真实环境
拿到真实连接串后，改 `backend/.env` 即可，无需改代码：
- `DB_URL=mysql+pymysql://用户:密码@主机:3306/smart_dw?charset=utf8mb4`
- `QWEN_API_KEY=sk-真实Key`
- `ES_URL=http://主机:9200`、`ES_ENABLED=true`
- `REDIS_URL` / `CELERY_BROKER_URL` / `CELERY_RESULT_BACKEND`（`TASK_MODE=dev` 时无需 Redis）
一键对比模板：参考 [backend/.env.example](backend/.env.example)。

MySQL 建表脚本：[docker/init/init.sql](docker/init/init.sql)（对应 Spec §2 全量表）。

## 外部组件（可选，生产/联真环境）
```bash
docker compose -f docker/docker-compose.yml up -d
```
启动 MySQL8 / Redis / Elasticsearch8，`init/init.sql` 自动建库建表。

## LLM 网关
Key 仅存服务端不外泄：配置 `QWEN_API_KEY` 后服务启动时自动 AES-GCM 加密存储；`QWEN_FALLBACK_ENABLED=true` 时即使千问不可达，NL2SQL 也回退到规则/SQL 生成，保证开箱即用。