"""统一返回包装：{code, message, data}（Spec §3.1 / §6）。"""
from typing import Any
from pydantic import BaseModel


class ApiResponse(BaseModel):
    code: int = 0
    message: str = "success"
    data: Any = None


def ok(data: Any = None, message: str = "success") -> dict:
    return {"code": 0, "message": message, "data": data}


def page(list_: list, total: int, page: int, page_size: int) -> dict:
    return {"list": list_, "total": total, "page": page, "page_size": page_size}