"""元数据路由（Spec §3.2）。"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.exceptions import not_found
from app.core.response import ok, page
from app.deps import get_db_session
from app.db.models import MetaDatabase, MetaTable
from app.services import metadata_ingest, lineage as lineage_svc

router = APIRouter(prefix="/api/v1", tags=["metadata"])


@router.get("/databases")
def databases(keyword: str | None = None, parent_id: int | None = None,
              db: Session = Depends(get_db_session)):
    q = db.query(MetaDatabase)
    if keyword:
        q = q.filter(MetaDatabase.db_name.like(f"%{keyword}%"))
    rows = q.all()
    return ok(page([{
        "id": r.id, "db_name": r.db_name, "layer": r.layer, "owner": r.owner,
        "location": r.location, "remark": r.remark,
    } for r in rows], len(rows), 1, 100))


@router.get("/databases/{dbname}/tables")
def tables(dbname: str, keyword: str | None = None, db: Session = Depends(get_db_session)):
    d = db.query(MetaDatabase).filter(MetaDatabase.db_name == dbname).first()
    if not d:
        not_found("库不存在")
    q = db.query(MetaTable).filter(MetaTable.db_id == d.id)
    if keyword:
        q = q.filter(MetaTable.table_name.like(f"%{keyword}%") | MetaTable.comment.like(f"%{keyword}%"))
    rows = q.all()
    return ok(page([{
        "id": t.id, "table_name": t.table_name, "comment": t.comment, "db_name": dbname,
        "table_type": t.table_type, "storage_format": t.storage_format, "compress": t.compress,
        "is_partitioned": t.is_partitioned, "row_count": t.row_count, "table_size_bytes": t.table_size_bytes,
        "file_count": t.file_count, "little_file_count": t.little_file_count, "owner": t.owner,
    } for t in rows], len(rows), 1, 100))


@router.get("/tables/{tid}")
def table_detail(tid: int, db: Session = Depends(get_db_session)):
    t = db.query(MetaTable).filter(MetaTable.id == tid).first()
    if not t:
        not_found("表不存在")
    p = db.query(MetaDatabase).filter(MetaDatabase.id == t.db_id).first()
    return ok({
        "id": t.id, "db_name": p.db_name if p else "", "table_name": t.table_name,
        "comment": t.comment, "table_type": t.table_type, "storage_format": t.storage_format,
        "compress": t.compress, "is_partitioned": t.is_partitioned, "row_count": t.row_count,
        "table_size_bytes": t.table_size_bytes, "file_count": t.file_count,
        "little_file_count": t.little_file_count, "owner": t.owner, "location": t.location,
        "source_mode": t.source_mode, "db_id": t.db_id,
        "columns": [{
            "id": c.id, "table_id": c.table_id, "column_name": c.column_name,
            "column_type": c.column_type, "comment": c.comment, "is_partition": c.is_partition,
            "is_primary": c.is_primary, "ordinal": c.ordinal} for c in t.columns],
        "partitions": [{
            "id": v.id, "table_id": v.table_id, "partition_key": v.partition_key,
            "partition_value": v.partition_value, "size_bytes": v.size_bytes,
            "row_count": v.row_count, "file_count": v.file_count} for v in t.partitions],
        "created_at": t.created_at.isoformat() if t.created_at else None,
    })


@router.get("/tables/{tid}/columns")
def columns(tid: int, db: Session = Depends(get_db_session)):
    t = db.query(MetaTable).filter(MetaTable.id == tid).first()
    if not t:
        not_found("表不存在")
    return ok([{
        "id": c.id, "column_name": c.column_name, "column_type": c.column_type,
        "comment": c.comment, "is_partition": c.is_partition, "is_primary": c.is_primary,
        "ordinal": c.ordinal} for c in t.columns])


@router.get("/tables/{tid}/partitions")
def partitions(tid: int, db: Session = Depends(get_db_session)):
    t = db.query(MetaTable).filter(MetaTable.id == tid).first()
    if not t:
        not_found("表不存在")
    return ok([{
        "id": v.id, "partition_key": v.partition_key, "partition_value": v.partition_value,
        "size_bytes": v.size_bytes, "row_count": v.row_count, "file_count": v.file_count} for v in t.partitions])


@router.get("/tables/{tid}/lineage")
def lineage(tid: int, db: Session = Depends(get_db_session)):
    if not db.query(MetaTable).filter(MetaTable.id == tid).first():
        not_found("表不存在")
    return ok(lineage_svc.get_lineage(db, tid))


@router.post("/metadata/sync")
def sync(body: dict, db: Session = Depends(get_db_session)):
    mode = body.get("mode", "all")
    scope = body.get("scope", [])
    source = body.get("source", "hive")
    # 开发模式同步执行
    import uuid
    job_id = f"job-{uuid.uuid4().hex[:12]}"
    if source == "file":
        metadata_ingest.sync_from_files(scope, "file")
    else:
        metadata_ingest.sync_from_hive(scope)
    return ok({"job_id": job_id, "status": "SUBMITTED"})