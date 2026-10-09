"""数据访问层。"""


def target_metadata_name() -> str:
    from app.config import settings
    return settings.db_url

# 兼容 SQLite 与 MySQL 的 JSON 类型