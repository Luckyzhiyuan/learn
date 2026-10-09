"""FastAPI 依赖注入：DB session、当前用户。"""
from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from app.config import settings
from app.core.exceptions import unauthorized
from app.core.security import decode_token
from app.db.session import get_db

_bearer = HTTPBearer(auto_error=False)


def get_current_user(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> str:
    """返回当前用户 employee_no。
    开发模式：未带 Token 时返回默认账号，方便本地联调；带 Token 则校验。
    """
    if creds is None:
        if settings.debug:
            return settings.default_employee_no
        unauthorized("未认证")
    try:
        payload = decode_token(creds.credentials)
        return payload.get("sub") or settings.default_employee_no
    except Exception:
        if settings.debug:
            return settings.default_employee_no
        unauthorized("Token 失效")


def get_db_session(db: Session = Depends(get_db)) -> Session:
    return db