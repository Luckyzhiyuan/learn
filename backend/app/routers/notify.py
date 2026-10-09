"""通知路由（Spec §3.8）。"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.exceptions import not_found
from app.core.response import ok
from app.deps import get_db_session, get_current_user
from app.db.models import NotifyMessage

router = APIRouter(prefix="/api/v1/notify", tags=["notify"])


@router.get("/messages")
def messages(read_flag: int | None = None, page: int = 1,
             user: str = Depends(get_current_user),
             db: Session = Depends(get_db_session)):
    q = db.query(NotifyMessage).filter(NotifyMessage.user_id == user)
    if read_flag is not None:
        q = q.filter(NotifyMessage.read_flag == read_flag)
    rows = q.order_by(NotifyMessage.id.desc()).limit(50).all()
    unread = db.query(NotifyMessage).filter(
        NotifyMessage.user_id == user, NotifyMessage.read_flag == 0).count()
    return ok({"list": [{
        "id": m.id, "title": m.title, "content": m.content, "biz_type": m.biz_type,
        "ref_id": m.ref_id, "read_flag": m.read_flag, "created_at": m.created_at} for m in rows],
        "unread_count": unread, "total": len(rows), "page": page, "page_size": 50})


@router.put("/messages/{mid}/read")
def read(mid: int, body: dict | None = None, user: str = Depends(get_current_user),
         db: Session = Depends(get_db_session)):
    m = db.query(NotifyMessage).filter(NotifyMessage.id == mid,
                                       NotifyMessage.user_id == user).first()
    if not m:
        not_found("消息不存在")
    m.read_flag = 1
    db.commit()
    return ok({"id": m.id, "read_flag": 1})