"""知识库管理路由（Spec §5.5）。"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.exceptions import not_found
from app.core.response import ok
from app.deps import get_db_session
from app.db.models import QaConversation, KBSqlExample
from app.schemas.kb import (
    KBDocumentCreate, KBDocumentUpdate, KBAliasIn, KBSqlExampleIn,
)
from app.services import kb_service, rag

router = APIRouter(prefix="/api/v1/kb", tags=["knowledge-base"])


@router.post("/documents")
def create_document(body: KBDocumentCreate, db: Session = Depends(get_db_session)):
    doc = kb_service.create_document(body, user="1207799", db=db)
    return ok({"id": doc.id, "kb_type": doc.kb_type, "title": doc.title})


@router.get("/documents")
def list_documents(page: int = 1, page_size: int = 20, kb_type: str = "",
                   tag: str = "", keyword: str = "", db: Session = Depends(get_db_session)):
    return ok(kb_service.list_documents(page, page_size, kb_type, tag, keyword, db=db))


@router.put("/documents/{doc_id}")
def update_document(doc_id: int, body: KBDocumentUpdate, db: Session = Depends(get_db_session)):
    doc = kb_service.update_document(doc_id, body, db=db)
    if not doc:
        not_found("知识文档不存在")
    return ok({"id": doc.id, "updated": True})


@router.delete("/documents/{doc_id}")
def delete_document(doc_id: int, db: Session = Depends(get_db_session)):
    if not kb_service.soft_delete_document(doc_id, db=db):
        not_found("知识文档不存在")
    return ok({"id": doc_id, "deleted": True})


@router.post("/aliases")
def create_alias(body: KBAliasIn, db: Session = Depends(get_db_session)):
    a = kb_service.create_alias(body, db=db)
    return ok({"id": a.id, "term": a.term, "standard": a.standard})


@router.get("/aliases")
def list_aliases(page: int = 1, page_size: int = 20, keyword: str = "",
                 db: Session = Depends(get_db_session)):
    return ok(kb_service.list_aliases(page, page_size, keyword, db=db))


@router.post("/sql-examples")
def create_sql_example(body: KBSqlExampleIn, db: Session = Depends(get_db_session)):
    e = kb_service.create_sql_example(body, db=db)
    return ok({"id": e.id, "question": e.question, "sql": e.sql})


@router.post("/sql-examples/import")
def import_from_conversation(body: dict, db: Session = Depends(get_db_session)):
    """把优质会话沉淀为示例 SQL（Spec §5.6）。"""
    cid = body.get("conversation_id")
    conv = db.query(QaConversation).filter(QaConversation.id == cid).first() if cid else None
    if not conv or not conv.generated_sql:
        not_found("会话不存在或无生成 SQL")
    e = KBSqlExample(question=conv.question, sql=conv.generated_sql, enabled=1)
    db.add(e)
    db.commit()
    db.refresh(e)
    return ok({"id": e.id, "imported": True, "question": e.question})


@router.get("/embedding/rebuild")
def rebuild_embedding_index():
    return ok(rag.rebuild_index())