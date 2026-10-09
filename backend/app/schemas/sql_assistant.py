"""SQL 智能开发助手 schema（Spec §3.4）。"""
from typing import Optional

from pydantic import BaseModel, Field


class SqlRequest(BaseModel):
    sql: str = Field(min_length=1)


class OptimizeRequest(SqlRequest):
    task_id: int | None = None


class SqlTable(BaseModel):
    db: str = Field(default="")
    table: str
    table_id: int | None = None
    is_partitioned: int = 0
    size_bytes: int = 0
    storage_format: str = ""


class SqlFeatures(BaseModel):
    select_star: bool = False
    partition_filter: bool = False
    partition_func_on_key: bool = False
    has_join: bool = False
    join_tables: list[int] = Field(default_factory=list)


class ParseOut(BaseModel):
    tables: list[SqlTable] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    features: SqlFeatures = Field(default_factory=SqlFeatures)


class Diagnosis(BaseModel):
    issue: str
    severity: str = "medium"
    detail: str = ""


class Suggestion(BaseModel):
    rule: str
    level: str = "must_do"
    message: str


class OptimizeOut(BaseModel):
    diagnosis: list[Diagnosis] = Field(default_factory=list)
    suggestions: list[Suggestion] = Field(default_factory=list)


class Change(BaseModel):
    rule: str
    desc: str = ""


class RewriteOut(BaseModel):
    original_sql: str
    rewritten_sql: str
    diff: str = ""
    changes: list[Change] = Field(default_factory=list)


class EvalDim(BaseModel):
    dim: str
    before: str
    after: str
    improve: str


class EvaluateOut(BaseModel):
    before: dict = Field(default_factory=dict)
    after: dict = Field(default_factory=dict)
    report: dict = Field(default_factory=dict)