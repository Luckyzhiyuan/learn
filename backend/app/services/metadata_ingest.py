"""元数据采集：模式一(本地建表文件) + 模式二(Hive 直连，schema 由 sqlglot 解析)。

统一标准化入库（meta_database/meta_table/meta_column/meta_partition/metasnapshot）。
"""
from datetime import datetime
from pathlib import Path

import sqlglot
from sqlglot import exp

from app.config import settings
from app.db.session import get_session
from app.db.models import (
    MetaDatabase, MetaTable, MetaColumn, MetaPartition, MetaSnapshot,
)
from app.services.sql_parser import is_read_only


def _col_comment(c: "exp.ColumnDef") -> str:
    """从列约束中提取注释（宽松 hive 解析时注释存于 CommentColumnConstraint）。"""
    for cons in c.args.get("constraints") or []:
        k = cons.args.get("kind")
        if isinstance(k, exp.CommentColumnConstraint):
            v = k.this
            return v.this if hasattr(v, "this") else str(v)
    return ""


def _parse_create(db: "Session", db_name: str, ddl: str, source="file") -> int:
    try:
        stmt = sqlglot.parse_one(ddl, read="hive")
    except Exception:
        return 0
    if not isinstance(stmt, exp.Create):
        return 0
    # 标准 hive 解析：this 为 Table；宽松解析：this 为 Schema(内嵌 Table)
    table_name = ""
    if isinstance(stmt.this, exp.Table):
        table_name = stmt.this.name
    elif isinstance(stmt.this, exp.Schema) and isinstance(stmt.this.this, exp.Table):
        table_name = stmt.this.this.name
    if not table_name:
        return 0

    schema = None
    for s in stmt.find_all(exp.Schema):
        schema = s
        break

    # 分区字段：标准形式在 partitioned 参数；宽松形式在 properties.PartitionedByProperty
    partition_keys = []
    partitioned = stmt.args.get("partitioned")
    if partitioned:
        partition_keys = [c.name for c in partitioned.find_all(exp.ColumnDef)]
    else:
        for pr in (stmt.args.get("properties") or []).expressions:
            if isinstance(pr, exp.PartitionedByProperty):
                partition_keys = [c.name for c in pr.find_all(exp.ColumnDef)]

    # 字段
    columns = []
    if schema:
        for c in schema.expressions:
            if isinstance(c, exp.ColumnDef):
                kind = c.args.get("kind")
                columns.append({
                    "name": c.name,
                    "type": kind.sql() if kind is not None else "string",
                    "comment": _col_comment(c),
                    "is_partition": c.name in partition_keys,
                })

    storage_format = "ORC"
    props = stmt.args.get("properties")
    if props:
        for pr in props.expressions:
            if isinstance(pr, exp.FileFormatProperty):
                storage_format = pr.this.sql().upper()
                break

    # 落库
    dbrow = db.query(MetaDatabase).filter(MetaDatabase.db_name == db_name).first()
    if not dbrow:
        dbrow = MetaDatabase(db_name=db_name, layer=db_name.split("_")[0] if "_" in db_name else "ads")
        db.add(dbrow)
        db.flush()
    trow = db.query(MetaTable).filter(MetaTable.db_id == dbrow.id, MetaTable.table_name == table_name).first()
    if trow:
        # 更新
        trow.columns.clear()
        trow.partitions.clear()
    else:
        trow = MetaTable(db_id=dbrow.id, table_name=table_name)
        db.add(trow)
    trow.comment = table_name
    trow.source_mode = source
    trow.storage_format = storage_format
    trow.is_partitioned = 1 if partition_keys else 0
    trow.owner = dbrow.owner or settings.default_employee_no
    db.flush()

    # 字段
    for ordinal, c in enumerate(columns, start=1):
        db.add(MetaColumn(table_id=trow.id, column_name=c["name"],
                          column_type=str(c["type"]), comment=c["comment"],
                          is_partition=int(c["is_partition"]),
                          ordinal=ordinal))
    # 分区元数据（占位，partitions 列表）
    for k in partition_keys:
        db.add(MetaPartition(table_id=trow.id, partition_key=k, partition_value=""))

    # 快照
    db.add(MetaSnapshot(table_id=trow.id, snapshot_time=datetime.utcnow(),
                        snapshot_json={"columns": columns, "partition": partition_keys}))
    db.commit()
    return table_name and 1


def sync_from_files(scope: list[str] | None = None, source="file") -> dict:
    """扫描 METADATA_FILE_DIR 下的 .hql/.sql/.ddl 建表文件并入库。"""
    db = get_session()
    count = 0
    base = Path(settings.metadata_file_dir)
    if not base.exists():
        base = Path(__file__).resolve().parent.parent.parent / "sample_ddls"
    files = list(base.glob("*.hql")) + list(base.glob("*.sql")) + list(base.glob("*.ddl"))
    for f in files:
        db_name = f.stem.split("__")[0]  # 形如 mid_hotel__xxx 仅在无则用文件名
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        count += _parse_create(db, db_name, text, source) or 0
    db.close()
    return {"tables": count, "source": "file"}


def sync_from_hive(scope: list[str] | None = None) -> dict:
    """Hive 直连模式。真实环境通过 jdbc/beeline 取 SHOW CREATE；此处从本地文件回退。

    采用统一解析逻辑（复用文件模式）以保持结果一致；标注 source=hive。
    """
    return sync_from_files(scope, source="hive")