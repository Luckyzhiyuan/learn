"""智能建模模块 schema（Spec §3.3）。"""
from typing import Optional

from pydantic import BaseModel, Field


class ModelingColumnIn(BaseModel):
    name: str
    type: str = "string"
    comment: str = ""
    is_primary: bool = False


class ModelingReq(BaseModel):
    db_name: str = "mid_hotel"
    table_name: str
    table_comment: str = ""
    columns: list[ModelingColumnIn] = Field(min_length=1)
    partition_by: str = "day"
    storage_format: str = "ORC"
    compress: str = "SNAPPY"


class Violation(BaseModel):
    rule: str
    level: str = "error"
    message: str


class ValidateOut(BaseModel):
    valid: bool
    violations: list[Violation] = Field(default_factory=list)
    warnings: list[dict] = Field(default_factory=list)


class GenerateOut(BaseModel):
    sql: str