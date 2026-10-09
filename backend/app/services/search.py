"""检索服务：ES 优先 + SQL 回退（Spec §7.5 / F5.3, F5.1）。"""
import json
import os

from app.config import settings
from app.db.session import get_session
from app.db.models import MetaDatabase, MetaTable, MetaColumn


ES_CONN_OVERRIDE = os.environ.get("SMART_DW_ES_HEALTHY") == "1"


def _es_client():
    from elasticsearch import Elasticsearch
    return Elasticsearch(settings.es_url)


def es_available() -> bool:
    if ES_CONN_OVERRIDE:
        return True
    try:
        return _es_client().ping()
    except Exception:
        return False


def search_sql(keyword: str, entity_type: str = "table,column", page: int = 1, page_size: int = 20) -> dict:
    """SQL 回退检索（LIKE 模糊 + 匹配度排序）。"""
    db = get_session()
    kw = f"%{keyword}%"
    types = set(x.strip() for x in entity_type.split(",") if x.strip()) or {"table", "column"}
    hits = []

    if "table" in types or "database" in types:
        ts = db.query(MetaTable).filter(
            MetaTable.table_name.like(kw) | MetaTable.comment.like(kw)
        ).limit(page_size * 4).all()
        for t in ts:
            db2 = db.query(MetaDatabase).filter(MetaDatabase.id == t.db_id).first()
            hits.append({
                "entity_type": "table", "name": t.table_name, "comment": t.comment,
                "db_name": db2.db_name if db2 else "", "table_id": t.id,
                "score": 1.0 if keyword.lower() in t.table_name.lower() else 0.5,
            })
    if "column" in types:
        cs = db.query(MetaColumn).filter(
            MetaColumn.column_name.like(kw) | MetaColumn.comment.like(kw)
        ).limit(page_size * 4).all()
        for c in cs:
            t = db.query(MetaTable).filter(MetaTable.id == c.table_id).first()
            d = db.query(MetaDatabase).filter(MetaDatabase.id == t.db_id).first() if t else None
            hits.append({
                "entity_type": "column", "name": c.column_name, "comment": c.comment,
                "db_name": d.db_name if d else "", "table_id": t.id if t else None,
                "column_id": c.id,
                "score": 1.0 if keyword.lower() in c.column_name.lower() else 0.4,
            })
    hits.sort(key=lambda h: h["score"], reverse=True)
    total = len(hits)
    start = (page - 1) * page_size
    db.close()
    return {"list": hits[start:start + page_size], "total": total,
            "page": page, "page_size": page_size}


def search(keyword: str, entity_type: str = "table,column", page: int = 1, page_size: int = 20) -> dict:
    if es_available():
        return search_es(keyword, entity_type, page, page_size)
    return search_sql(keyword, entity_type, page, page_size)


def search_es(keyword: str, entity_type: str, page: int = 1, page_size: int = 20) -> dict:
    client = _es_client()
    types = set(x.strip() for x in entity_type.split(",") if x.strip()) or {"table", "column"}
    body = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"name": {"query": keyword, "boost": 3}}},
                    {"match": {"name_ngram": {"query": keyword, "boost": 2}}},
                    {"match": {"comment": {"query": keyword, "boost": 1}}},
                ]
            }
        },
        "from": (page - 1) * page_size, "size": page_size,
    }
    if types != {"table", "column"}:
        body["query"]["bool"]["filter"] = {"terms": {"entity_type": list(types)}}
    try:
        resp = client.search(index=settings.es_index_meta, body=body)
    except Exception:
        return search_sql(keyword, entity_type, page, page_size)
    hits = [{
        "entity_type": h["_source"].get("entity_type", "table"),
        "name": h["_source"].get("name", ""), "comment": h["_source"].get("comment", ""),
        "db_name": h["_source"].get("db_name", ""), "table_id": h["_source"].get("table_id"),
        "column_id": h["_source"].get("column_id"), "score": h["_score"] or 0,
    } for h in resp["hits"]["hits"]]
    return {"list": hits, "total": resp["hits"]["total"]["value"], "page": page, "page_size": page_size}