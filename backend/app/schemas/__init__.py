"""Pydantic v2 请求/响应模型。"""
from typing import Optional
from pydantic import BaseModel, ConfigDict


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PageParams(BaseModel):
    page: int = 1
    page_size: int = 20


class PageResult(BaseModel):
    list: list
    total: int
    page: int
    page_size: int