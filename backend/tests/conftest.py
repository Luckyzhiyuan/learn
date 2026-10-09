"""pytest 根配置：把后端切到独立临时 SQLite，避免污染开发库。

注意：必须在 import app.* 之前设置 DB_URL 等环境变量（settings 为 lru_cache）。
"""
import os
import tempfile

# 临时库路径（每次会话一个唯一文件，避免跨会话残留）
_TMP = os.path.join(tempfile.gettempdir(), "smart_dw_test_kb.db")
os.environ["DB_URL"] = f"sqlite:///{_TMP}"
os.environ["TASK_MODE"] = "dev"

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def db_session():
    """建表并预置知识库数据，返回可用的 Session。"""
    from sqlalchemy.orm import sessionmaker
    from app.db.session import get_engine, Base  # noqa
    from app.db import models  # noqa  (确保模型已注册)

    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    s = Session()

    # 预置：同义词 / 口径 / 示例SQL / 元数据表
    from app.db.models import KBAlias, KBDocument, KBSqlExample, MetaDatabase, MetaTable, MetaColumn
    if s.query(KBAlias).count() == 0:
        s.add(KBAlias(term="UV", standard="COUNT(DISTINCT userkey)", remark="去重用户数"))
        s.add(KBAlias(term="昨日", standard="dt=DATE_SUB(CURRENT_DATE,1)", remark="日期词"))
        s.add(KBDocument(kb_type="glossary", title="酒店UV口径",
                         content="UV=去重用户数，使用 userkey COUNT(DISTINCT userkey)",
                         tags="uv,去重,用户", created_by="1207799"))
        s.add(KBSqlExample(question="统计昨天国内酒店UV",
                           sql="SELECT COUNT(DISTINCT userkey) FROM foo WHERE dt=X", tags="uv,昨日"))
        d = s.query(MetaDatabase).filter(MetaDatabase.db_name == "mid_hotel").first()
        if not d:
            d = MetaDatabase(db_name="mid_hotel", layer="dwd")
            s.add(d)
            s.flush()
            t = MetaTable(db_id=d.id, table_name="dwd_hotel_order_detail",
                          comment="酒店订单明细", is_partitioned=1)
            s.add(t)
            s.flush()
            s.add(MetaColumn(table_id=t.id, column_name="userkey",
                             column_type="string", comment="统一用户标识"))
        s.commit()
    yield s
    s.close()

    # 清理临时文件
    for suffix in ("", "-wal", "-shm"):
        p = _TMP + suffix
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass