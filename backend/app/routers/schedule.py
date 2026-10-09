"""调度路由（Spec §3.7）。"""
import uuid
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.exceptions import not_found
from app.core.response import ok
from app.deps import get_db_session
from app.db.models import TaskInfo, TaskInstance

router = APIRouter(prefix="/api/v1/tasks", tags=["schedule"])


@router.get("")
def tasks(keyword: str | None = None, status: str | None = None,
          db: Session = Depends(get_db_session)):
    q = db.query(TaskInfo)
    if keyword:
        q = q.filter(TaskInfo.task_name.like(f"%{keyword}%"))
    if status:
        q = q.filter(TaskInfo.status == status)
    rows = q.all()
    return ok({"list": [{
        "task_id": t.id, "task_name": t.task_name, "status": t.status,
        "owner": t.owner, "schedule_cron": t.schedule_cron,
        "created_at": t.created_at.isoformat() if t.created_at else None} for t in rows],
        "total": len(rows), "page": 1, "page_size": 100})


@router.get("/{tid}/instances")
def instances(tid: int, db: Session = Depends(get_db_session)):
    rows = db.query(TaskInstance).filter(TaskInstance.task_id == tid).order_by(
        TaskInstance.id.desc()).limit(50).all()
    return ok({"list": [{
        "instance_id": i.instance_id, "status": i.status, "app_id": i.app_id,
        "history_url": i.history_url, "start_time": i.start_time, "end_time": i.end_time} for i in rows]})


@router.get("/{tid}/logs")
def logs(tid: int, db: Session = Depends(get_db_session)):
    i = db.query(TaskInstance).filter(TaskInstance.task_id == tid).order_by(
        TaskInstance.id.desc()).first()
    return ok({"instance_id": i.instance_id if i else "", "log": i.log_text if i else "",
               "history_url": i.history_url if i else ""})