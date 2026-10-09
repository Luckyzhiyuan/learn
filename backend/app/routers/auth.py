"""认证路由（简化）：POST /auth/login 返回 Token，便于前端联调。"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.exceptions import unauthorized
from app.core.security import create_access_token
from app.db.models import SysUser
from app.deps import get_db_session

router = APIRouter(prefix="/api/v1", tags=["auth"])


class LoginReq(BaseModel):
    employee_no: str
    user_name: str = ""


@router.post("/auth/login")
def login(body: LoginReq, db: Session = Depends(get_db_session)):
    user = db.query(SysUser).filter(SysUser.employee_no == body.employee_no).first()
    if not user and body.employee_no == "1207799":
        user = SysUser(employee_no="1207799", user_name=body.user_name or "张三", role="admin")
        db.add(user)
        db.commit()
    if not user:
        unauthorized("账号不存在")
    token = create_access_token(user.employee_no, {"role": user.role, "name": user.user_name})
    return {"token": token, "employee_no": user.employee_no, "user_name": user.user_name, "role": user.role}