"""血缘路由。"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.exceptions import not_found
from app.core.response import ok
from app.deps import get_db_session
from app.db.models import MetaTable
from app.services import lineage as lsvc

router = APIRouter(prefix="/api/v1/tables/{tid}/lineage", tags=["lineage"])


# NOTE: 主血缘接口已挂在 metadata 路由，此处保留独立子图入口（多跳）
@router.get("/graph")
def graph(tid: int, db: Session = Depends(get_db_session)):
    if not db.query(MetaTable).filter(MetaTable.id == tid).first():
        not_found("表不存在")
    return ok(lsvc.get_lineage(db, tid, depth=3))