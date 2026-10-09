"""血缘自建（Spec §7.7 / F1.4）：从 SQL 中解析输入/输出表生成表级血缘。"""
from datetime import datetime

from sqlglot import exp
import sqlglot

from app.db.session import get_session
from app.db.models import MetaLineage, MetaTable, MetaDatabase


def build_from_sql_task(db, sql: str, task_id: int = None, job_type="spark_sql",
                        owner="", schedule_status="SUCCESS", src=None, project_id=None) -> int:
    """从一条 SQL 提取其输入表(读)与输出表(写)，建立 src->dst 血缘。
    简化：WITH/INSERT INTO table SELECT ... 时，output 为 INSERT 目标表，input 为 FROM 表。
    返回新增血缘条数。
    """
    try:
        stmt = sqlglot.parse_one(sql, read="hive")
    except Exception:
        return 0

    tables = [t for t in stmt.find_all(exp.Table) if t.name and t.catalog]
    if not tables:
        return 0

    def resolve(table) -> int | None:
        row = db.query(MetaTable).join(MetaDatabase).filter(
            MetaDatabase.db_name == (table.catalog or ""),
            MetaTable.table_name == table.name).first()
        return row.id if row else None

    # 输出表：INSERT/CTAS 目标
    output_id = None
    if isinstance(stmt, exp.Insert):
        output_id = resolve(stmt.this)
    elif isinstance(stmt, exp.Create) and isinstance(stmt.this, exp.Table):
        output_id = resolve(stmt.this)

    inserted = 0
    for t in tables:
        tid = resolve(t)
        if tid is None:
            continue
        if output_id is not None and tid != output_id:
            exists = db.query(MetaLineage).filter(
                MetaLineage.src_table_id == tid, MetaLineage.dst_table_id == output_id,
                MetaLineage.task_id == task_id).first()
            if not exists:
                db.add(MetaLineage(
                    src_table_id=tid, dst_table_id=output_id, edge_tag="sql_parse",
                    task_id=task_id, project_id=project_id, job_type=job_type,
                    owner=owner, schedule_status=schedule_status,
                    schedule_time=datetime.utcnow()))
                inserted += 1
    db.commit()
    return inserted


def get_lineage(db, table_id: int, depth: int = 2) -> dict:
    """返回目标表的子图（nodes + edges）。"""
    edge_rows = db.query(MetaLineage).filter(
        (MetaLineage.src_table_id == table_id) | (MetaLineage.dst_table_id == table_id)).all()

    ids = {table_id}
    edges = []
    for e in edge_rows:
        ids.add(e.src_table_id)
        ids.add(e.dst_table_id)
        edges.append({
            "src_id": e.src_table_id, "dst_id": e.dst_table_id,
            "task_id": e.task_id, "project_id": e.project_id, "job_type": e.job_type,
            "owner": e.owner, "schedule_status": e.schedule_status,
            "schedule_time": e.schedule_time,
        })

    def label(tid):
        t = db.query(MetaTable).filter(MetaTable.id == tid).first()
        db2 = db.query(MetaDatabase).filter(MetaDatabase.id == t.db_id).first() if t else None
        return f"{db2.db_name}.{t.table_name}" if (t and db2) else (t.table_name if t else str(tid))

    nodes = [{"id": i, "label": label(i), "type": "table"} for i in ids]
    return {"nodes": nodes, "edges": edges}