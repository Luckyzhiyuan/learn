"""检索 / NL2SQL / 口径问答 路由（Spec §3.6）。"""
import asyncio
import uuid
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.exceptions import not_found, sql_not_readonly
from app.core.response import ok
from app.deps import get_db_session
from app.db.models import QaConversation, MetaTable, MetaDatabase
from app.schemas.biz import SearchReq, NL2SqlGenReq, NL2SqlExecuteReq, NL2SqlFeedbackReq, QaAskReq
from app.services import search as search_svc, nl2sql as nl2sql_svc
from app.services.sql_parser import is_read_only

router = APIRouter(prefix="/api/v1", tags=["search|nl2sql|qa"])


@router.post("/search")
def search(body: SearchReq):
    return ok(search_svc.search(body.keyword, body.entity_type,
                                body.page, body.page_size))


@router.post("/nl2sql/generate")
def nl2sql_generate(body: NL2SqlGenReq):
    result = nl2sql_svc.generate(body.question, body.db_hint, body.n, body.use_kb)
    return ok(result)


@router.post("/nl2sql/execute")
def nl2sql_execute(body: NL2SqlExecuteReq, db: Session = Depends(get_db_session)):
    if not is_read_only(body.sql):
        sql_not_readonly("SQL 非只读，已拦截")
    # 开发环境：无法连 Spark，返回占位结果
    return ok({
        "result": {"columns": ["统计值"], "rows": [["123456"]], "row_count": 1, "cost_seconds": 1.2},
        "query_key": f"qk-{uuid.uuid4().hex[:8]}",
    })


@router.post("/nl2sql/{cid}/feedback")
def nl2sql_feedback(cid: int, body: NL2SqlFeedbackReq, db: Session = Depends(get_db_session)):
    conv = db.query(QaConversation).filter(QaConversation.id == cid).first()
    if conv:
        conv.feedback = body.feedback
        db.commit()
    return ok({"id": cid, "feedback": body.feedback})


@router.post("/qa/ask")
def qa_ask(body: QaAskReq, db: Session = Depends(get_db_session)):
    # 简化口径问答：检索最相关表并基于元数据生成回答
    hits = search_svc.search_sql(body.question, "table", 1, 3)["list"]
    sources = []
    answer = "未能定位口径来源。"
    if hits:
        h = hits[0]
        t = db.query(MetaTable).filter(MetaTable.id == h["table_id"]).first()
        db2 = db.query(MetaDatabase).filter(MetaDatabase.id == t.db_id).first() if t else None
        full = f"{db2.db_name}.{t.table_name}" if (t and db2) else h["name"]
        answer = f"{h['comment'] or h['name']} 口径：本表为 {h['comment'] or '相关'}+ 统计口径来源表，明细见字段注释。"
        sources.append({"type": "table", "id": h["table_id"], "name": h["name"],
                        "reason": "口径定义来源表(基于元数据检索)"})
    return ok({"answer": answer, "sources": sources, "llm_used": "qwen3-8b"})