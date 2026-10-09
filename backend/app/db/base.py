"""Declarative Base 及通用 JSON 类型（兼容 SQLite/MySQL）。"""
from sqlalchemy import BigInteger, JSON as SA_JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.engine import make_url


class Base(DeclarativeBase):
    pass


def is_sqlite(url: str) -> bool:
    return make_url(url).get_backend_name() == "sqlite"


class SmartJSON(SA_JSON):
    """SQLite 用 TEXT 存储 JSON；MySQL 用原生 JSON。"""

    def load_dialect_impl(self, dialect):
        if dialect.name == "sqlite":
            from sqlalchemy import Text
            return dialect.type_descriptor(Text())
        return dialect.type_descriptor(SA_JSON())