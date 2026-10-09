"""知识库 CRUD 与多路召回检索（Spec §5.3/5.5 §5.6）。

多路召回：
1. 同义词检索：问题命中 kb_alias.term
2. 关键词检索：命中 kb_document.tags/content、kb_sql_example.question
3. 元数据检索：命中 meta_column/meta_table 注释
重排融合后按 score 截取，注入 NL2SQL 提示词。
"""
import re

from app.schemas.kb import RAGContext
from app.db.models import KBDocument, KBAlias, KBSqlExample
from app.db.session import get_session


# ---- CRUD: 文档 ----
def create_document(data, user: str, db=None) -> KBDocument:
    own = db is None
    db = db or get_session()
    try:
        doc = KBDocument(
            kb_type=data.kb_type, title=data.title, content=data.content,
            tags=data.tags, source=data.source,
            related_ids=data.related_ids or None, created_by=user, enabled=1)
        db.add(doc)
        db.commit()
        db.refresh(doc)
        return doc
    finally:
        if own:
            db.close()


def list_documents(page=1, page_size=20, kb_type="", tag="", keyword="", db=None):
    db = db or get_session()
    own = db is None
    try:
        q = db.query(KBDocument)
        if kb_type:
            q = q.filter(KBDocument.kb_type == kb_type)
        if tag:
            q = q.filter(KBDocument.tags.like(f"%{tag}%"))
        if keyword:
            q = q.filter(KBDocument.title.like(f"%{keyword}%"))
        total = q.count()
        rows = q.order_by(KBDocument.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
        return {"list": [d.to_dict() if hasattr(d, "to_dict") else {"id": d.id, "kb_type": d.kb_type,
                "title": d.title, "content": d.content, "tags": d.tags, "source": d.source,
                "related_ids": d.related_ids, "enabled": d.enabled,
                "created_by": d.created_by, "created_at": d.created_at.isoformat() if d.created_at else None}
                for d in rows],
                "total": total, "page": page, "page_size": page_size}
    finally:
        if own:
            db.close()


def update_document(doc_id: int, data, db=None) -> KBDocument:
    own = db is None
    db = db or get_session()
    try:
        doc = db.query(KBDocument).filter(KBDocument.id == doc_id).first()
        if not doc:
            return None
        for f in ("title", "content", "tags", "source"):
            v = getattr(data, f, None)
            if v is not None:
                setattr(doc, f, v)
        if data.related_ids is not None:
            doc.related_ids = data.related_ids or None
        if data.enabled is not None:
            doc.enabled = int(data.enabled)
        db.commit()
        db.refresh(doc)
        return doc
    finally:
        if own:
            db.close()


def soft_delete_document(doc_id: int, db=None) -> bool:
    own = db is None
    db = db or get_session()
    try:
        doc = db.query(KBDocument).filter(KBDocument.id == doc_id).first()
        if not doc:
            return False
        doc.enabled = 0
        db.commit()
        return True
    finally:
        if own:
            db.close()


# ---- CRUD: 同义词 ----
def create_alias(data, db=None) -> KBAlias:
    own = db is None
    db = db or get_session()
    try:
        a = KBAlias(term=data.term, standard=data.standard, kb_type=data.kb_type,
                    score=data.score, remark=data.remark, enabled=int(data.enabled))
        db.add(a)
        db.commit()
        db.refresh(a)
        return a
    finally:
        if own:
            db.close()


def list_aliases(page=1, page_size=20, keyword="", db=None):
    own = db is None
    db = db or get_session()
    try:
        q = db.query(KBAlias)
        if keyword:
            q = q.filter(KBAlias.term.like(f"%{keyword}%"))
        total = q.count()
        rows = q.order_by(KBAlias.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
        return {"list": [{"id": a.id, "term": a.term, "standard": a.standard,
                          "kb_type": a.kb_type, "score": a.score, "remark": a.remark,
                          "enabled": a.enabled} for a in rows],
                "total": total, "page": page, "page_size": page_size}
    finally:
        if own:
            db.close()


# ---- CRUD: 示例 SQL ----
def create_sql_example(data, db=None) -> KBSqlExample:
    own = db is None
    db = db or get_session()
    try:
        e = KBSqlExample(question=data.question, sql=data.sql,
                         tables_used=data.tables_used, tags=data.tags, enabled=int(data.enabled))
        db.add(e)
        db.commit()
        db.refresh(e)
        return e
    finally:
        if own:
            db.close()


def _keywords(question: str):
    return re.sub(r"[^\u4e00-\u9fa5A-Za-z0-9]+", " ", question).split()


def recall(question: str, limit: int = 3) -> RAGContext:
    """多路召回，返回注入所需的知识片段。检索失败只返回空上下文（不影响主流程，40014）。"""
    db = get_session()
    try:
        ctx = RAGContext()
        keys = _keywords(question)
        all_text = question + " " + " ".join(keys)

        # 1) 同义词召回：term 出现于问题，或 term 是问题子串
        for a in db.query(KBAlias).filter(KBAlias.enabled == 1).all():
            if a.term and (a.term in all_text or any(t in a.term for t in keys if len(t) >= 2)):
                if len(ctx.aliases) < limit:
                    ctx.aliases.append({"term": a.term, "standard": a.standard})

        # 2) 口径/规则文档召回：tag 或标题命中关键字
        for d in db.query(KBDocument).filter(KBDocument.enabled == 1).all():
            if len(ctx.glossaries) >= limit:
                break
            if any(k and (k in d.tags or k in d.title or k in d.content) for k in keys):
                ctx.glossaries.append(f"{d.title}：{d.content[:200]}")

        # 3) 示例 SQL 召回（few-shot）
        for e in db.query(KBSqlExample).filter(KBSqlExample.enabled == 1).all():
            if len(ctx.sql_examples) >= limit:
                break
            if any(k and (k in e.question or k in e.tags) for k in keys):
                ctx.sql_examples.append({"question": e.question, "sql": e.sql})

        # 4) 元数据注释召回→补充为口径
        if not ctx.glossaries:
            from app.db.models import MetaColumn
            for c in db.query(MetaColumn).filter(MetaColumn.comment != "").limit(200).all():
                if len(ctx.glossaries) >= limit:
                    break
                if any(k and (k in c.comment or k in c.column_name) for k in keys):
                    ctx.glossaries.append(f"{c.column_name}：{c.comment}")
        return ctx
    except Exception:
        return RAGContext()
    finally:
        db.close()