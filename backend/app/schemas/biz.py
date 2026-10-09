"""数据质量 / 检索问答 / LLM / 通知 / 调度 schema。"""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.schemas import ORMModel


class QualityRuleIn(BaseModel):
    rule_name: str
    rule_type: str
    table_id: int
    column_id: int | None = None
    params: dict | None = None
    sql: str | None = None
    threshold: float | None = None
    enabled: bool = True


class QualityRuleOut(BaseModel):
    id: int
    rule_name: str
    rule_type: str
    table_id: int
    column_id: int | None = None
    params: dict | None = None
    sql: str | None = None
    threshold: float | None = None
    enabled: int = 1
    created_by: str = ""
    created_at: datetime | None = None


class QualityReportOut(BaseModel):
    id: int
    rule_id: int
    rule_name: str = ""
    table_name: str = ""
    execute_time: datetime | None = None
    pass_: int = 1
    actual_value: float | None = None
    detail: str = ""
    notified: int = 0


class SearchReq(BaseModel):
    keyword: str = Field(min_length=1)
    entity_type: str = "table,column"
    page: int = 1
    page_size: int = 20


class SearchReq(BaseModel):
    keyword: str = Field(min_length=1)
    entity_type: str = "table,column"
    page: int = 1
    page_size: int = 20


class NL2SqlGenReq(BaseModel):
    question: str = Field(min_length=1, max_length=500)
    db_hint: str | None = None
    n: int = Field(1, ge=1, le=3)
    use_kb: bool = True  # 是否启用知识库 RAG 检索注入（Spec §5.8）


class NL2SqlExecuteReq(BaseModel):
    sql_id: str
    sql: str
    limit: int = Field(100, ge=1, le=1000)


class NL2SqlFeedbackReq(BaseModel):
    feedback: int = Field(..., ge=-1, le=1)


class QaAskReq(BaseModel):
    question: str = Field(min_length=1)


class NotifyOut(ORMModel):
    id: int
    title: str
    content: str
    biz_type: str = "quality"
    ref_id: int | None = None
    read_flag: int = 0
    created_at: datetime | None = None


class LlmConfigIn(BaseModel):
    provider: str = "qwen"
    model_name: str = "qwen3-8b"
    api_url: str
    api_key: str | None = None
    enabled: bool = True
    fallback: bool = True
    temperature: float = Field(0.0, ge=0.0, le=1.0)
    max_tokens: int = Field(2048, ge=1, le=8192)
    timeout_sec: int = Field(30, ge=1, le=120)


class LlmConfigOut(BaseModel):
    provider: str
    model_name: str
    api_url: str
    api_key_masked: str = ""
    enabled: bool
    fallback: bool
    temperature: float
    max_tokens: int
    timeout_sec: int