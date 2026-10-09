"""SQLAlchemy 引擎与会话，兼容 MySQL / SQLite。"""
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session

from app.config import settings
from app.db.base import Base, is_sqlite

_engine = None
_SessionLocal = None


def _current_url():
    return settings.db_url


def init_engine() -> None:
    global _engine, _SessionLocal
    url = _current_url()
    _sqlite = is_sqlite(url)
    connect_args = {"check_same_thread": False} if _sqlite else {}

    engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args)

    if _sqlite:
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, _):
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA journal_mode=WAL")
            cur.close()

    _SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    _engine = engine


def get_engine():
    global _engine
    if _engine is None:
        init_engine()
    return _engine


def get_session() -> Session:
    global _SessionLocal
    if _SessionLocal is None:
        init_engine()
    return _SessionLocal()


def create_all() -> None:
    """建表（开发用）。真实环境请用 init/init.sql 的 MySQL DDL。"""
    from app.db import models  # noqa: F401
    Base.metadata.create_all(bind=get_engine())


def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()