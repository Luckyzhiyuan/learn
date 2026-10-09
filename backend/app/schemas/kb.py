"""知识库（RAG）schema（Spec §5.8）。"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class KBDocumentCreate(BaseModel):
    kb_type: Literal['glossary', 'alias', 'sql_example', 'mapping', 'rule']
    title: str = Field(..., max_length=255)
    content: str = Field(..., max_length=10000)
    tags: str = ""
    source: str = ""
    related_ids: list[int] = Field(default_factory=list)


class KBDocumentUpdate(BaseModel):
    title: str | None = None
    content: str | None = None
    tags: str | None = None
    source: str | None = None
    related_ids: list[int] | None = None
    enabled: int | None = None


class KBDocumentOut(BaseModel):
    id: int
    kb_type: str
    title: str
    content: str = ""
    tags: str = ""
    source: str = ""
    related_ids: list = Field(default_factory=list)
    enabled: int = 1
    created_by: str = ""
    created_at: datetime | None = None


class KBAliasIn(BaseModel):
    term: str = Field(..., max_length=128)
    standard: str = Field(..., max_length=128)
    kb_type: str = "alias"
    score: float = 1.0
    remark: str = ""
    enabled: bool = True


class KBAliasOut(BaseModel):
    id: int
    term: str
    standard: str
    kb_type: str = "alias"
    score: float = 1.0
    remark: str = ""
    enabled: int = 1


class KBSqlExampleIn(BaseModel):
    question: str = Field(..., max_length=500)
    sql: str = Field(..., max_length=10000)
    tables_used: str = ""
    tags: str = ""
    enabled: bool = True


class KBSqlExampleOut(BaseModel):
    id: int
    question: str
    sql: str
    tables_used: str = ""
    tags: str = ""
    good_feedback: int = 0
    enabled: int = 1


class RAGContext(BaseModel):
    """注入 NL2SQL 提示词的知识片段（Spec §5.3/5.4）。"""
    glossaries: list[str] = Field(default_factory=list)
    aliases: list[dict] = Field(default_factory=list)       # {term, standard}
    sql_examples: list[dict] = Field(default_factory=list)  # {question, sql}