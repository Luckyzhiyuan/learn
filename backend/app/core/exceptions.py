"""统一业务异常（Spec §6 错误码）。"""
from fastapi import HTTPException, status


class BizError(HTTPException):
    """可预期的业务异常，code 为业务码，http_status 为 HTTP 状态码。"""

    def __init__(self, code: int, message: str, http_status: int = 400):
        self.code = code
        self.biz = message
        super().__init__(status_code=http_status, detail=message)


def not_found(msg: str = "资源不存在"):
    raise BizError(40001, msg, status.HTTP_404_NOT_FOUND)


def unauthorized(msg: str = "未认证 / Token 失效"):
    raise BizError(40002, msg, status.HTTP_401_UNAUTHORIZED)


def forbidden(msg: str = "无权限"):
    raise BizError(40003, msg, status.HTTP_403_FORBIDDEN)


def sql_parse_error(msg: str = "SQL 语法解析失败"):
    raise BizError(40010, msg)


def nl2sql_fail(msg: str = "NL2SQL 生成失败"):
    raise BizError(40011, msg, status.HTTP_502_BAD_GATEWAY)


def sql_not_readonly(msg: str = "SQL 非只读，已拦截"):
    raise BizError(40012, msg, status.HTTP_403_FORBIDDEN)


def llm_error(msg: str = "千问 API 调用异常"):
    raise BizError(40013, msg, status.HTTP_502_BAD_GATEWAY)