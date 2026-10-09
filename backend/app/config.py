"""应用配置（pydantic-settings）。"""
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- 应用 ----
    app_name: str = "智能数仓平台"
    app_host: str = "127.0.0.1"
    app_port: int = 8000
    debug: bool = True

    # ---- 数据库 (SQLAlchemy URL) ----
    # 开发默认 SQLite；切真实环境请改为 mysql+pymysql://user:pass@host:3306/smart_dw?charset=utf8mb4
    db_url: str = "sqlite:///./smart_dw.db"

    # ---- Redis / Celery ----
    redis_url: str = "redis://127.0.0.1:6379/0"
    celery_broker_url: str = "redis://127.0.0.1:6379/1"
    celery_result_backend: str = "redis://127.0.0.1:6379/2"
    # dev=sync(任务同步执行，无需 Redis)/ prod=queue(走 Celery)
    task_mode: str = "dev"

    # ---- Elasticsearch ----
    es_url: str = ""
    es_index_meta: str = "meta_index"
    es_index_kb: str = "kb_index"  # 知识库向量索引（Spec §5.7）
    # 为空或不可达时回退到 SQL 检索
    es_enabled: bool = False

    # ---- Sparspark ----
    spark_master: str = "local[*]"
    spark_submit_conf: str = "--driver-memory 2g"

    # ---- 千问 LLM ----
    qwen_api_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
    qwen_model: str = "qwen3-8b"
    qwen_api_key: str = ""
    qwen_timeout: int = 30
    qwen_max_retry: int = 2
    qwen_fallback_enabled: bool = True

    # ---- 加密 / JWT ----
    llm_key_enc_master: str = "smart_dw_dev_master_key_0123456789abcdef"
    jwt_secret: str = "smart_dw_dev_jwt_secret_0123456789"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440

    # ---- 元数据本地文件 ----
    metadata_file_dir: str = str(ROOT / "sample_ddls")
    # 默认开发者账号（仅无真实登录时的简化认证）
    default_employee_no: str = "1207799"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()