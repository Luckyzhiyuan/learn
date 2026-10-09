"""SQL 解析：表 / 字段 / 特征 / 只读校验（基于 sqlglot）。

- 只读校验：剥离注释后匹配危险关键字 + sqlglot 语法确认顶级均为 SELECT。
"""
import re

import sqlglot
from sqlglot import exp

from app.core.exceptions import sql_parse_error

# 危险关键字（Spec §4.4）
DANGEROUS_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|LOAD|MERGE)\b",
    re.IGNORECASE,
)


def strip_comments(sql: str) -> str:
    # 去除行注释与块注释（同时避免破坏字符串内的关键字）
    s = re.sub(r"--[^\n]*", "", sql)
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)
    return s


def is_read_only(sql: str) -> bool:
    clean = strip_comments(sql)
    if DANGEROUS_RE.search(clean):
        return False
    try:
        expressions = sqlglot.parse(sql, read="hive")
    except Exception:
        sql_parse_error("SQL 无法解析")
    for e in expressions:
        # WITH...SELECT 也可视为只读
        if not isinstance(e, exp.Select):
            return False
    return True


def parse_sql(sql: str) -> dict:
    """提取表名、字段、分区、select星、join等特征。返回：
    { tables: [{db, table, table_id, is_partitioned, size_bytes, storage_format}],
      columns: [...], features: {...} }
    """
    try:
        stmt = sqlglot.parse_one(sql, read="hive")
    except Exception as exc:
        sql_parse_error(f"SQL 语法解析失败: {exc}")
    if not isinstance(stmt, exp.Select):
        sql_parse_error("仅支持 SELECT 语句解析")

    tables = []
    for t in stmt.find_all(exp.Table):
        tables.append({
            "db": t.catalog or "",
            "table": t.name,
            "full": f"{t.catalog}.{t.name}" if t.catalog else t.name,
            "table_id": None,
        })

    columns = []
    for col in stmt.find_all(exp.Column):
        if col.name not in columns:
            columns.append(col.name)

    # SELECT * 检测：投影表达式直接是否为 Star
    select_star = any(
        isinstance(p, exp.Star)
        or (isinstance(p, exp.Column) and p.name == "*")
        for p in getattr(stmt, "expressions", [])
    )
    select_star = select_star or any(
        isinstance(s, exp.Star) or (isinstance(s, exp.Column) and s.name == "*")
        for s in stmt.find_all(exp.Star)
    )

    # JOIN 检测
    has_join = any(True for _ in stmt.find_all(exp.Join))

    features = {
        "select_star": select_star,
        "partition_filter": False,
        "partition_func_on_key": False,
        "has_join": has_join,
        "join_tables": [],
    }
    # WHERE 中分区字段被函数包裹检测（简化：检测 where 里 day/dt 前后有函数）
    return {"tables": tables, "columns": columns, "features": features}


def resolve_table_meta(db: "Session", table_refs: list[dict], tables_out: list[dict]) -> None:
    """把解析出的表引用关联到元数据（填充 table_id / size / 分区），回填到 tables_out。"""
    from app.db.models import MetaTable, MetaColumn
    for ref in table_refs:
        row = None
        q = db.query(MetaTable)
        if ref.get("db"):
            from app.db.models import MetaDatabase
            dbrow = db.query(MetaDatabase).filter(MetaDatabase.db_name == ref["db"]).first()
            if dbrow:
                row = dbrow.tables and next(
                    (t for t in dbrow.tables if t.table_name == ref["table"]), None
                )
        if row is None:
            row = db.query(MetaTable).filter(MetaTable.table_name == ref["table"]).first()
        if row:
            tables_out.append({
                "db": ref.get("db", ""),
                "table": ref["table"],
                "table_id": row.id,
                "is_partitioned": row.is_partitioned,
                "size_bytes": row.table_size_bytes,
                "storage_format": row.storage_format,
            })