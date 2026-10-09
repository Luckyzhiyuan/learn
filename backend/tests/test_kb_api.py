"""知识库接口层测试：documents / aliases / sql-examples / rebuild / nl2sql(use_kb)。"""
from fastapi.testclient import TestClient

from app.main import app


def _client():
    # 每次新建以复用 conftest 同一临时库
    with TestClient(app) as c:
        return c


def test_kb_documents_crud():
    c = _client()
    # 创建
    r = c.post("/api/v1/kb/documents", json={
        "kb_type": "glossary", "title": "测试GMV口径", "content": "测试body", "tags": "gmv"})
    assert r.status_code == 200 and r.json()["code"] == 0
    did = r.json()["data"]["id"]
    # 列表（含 seed）
    r = c.get("/api/v1/kb/documents", params={"page": 1, "page_size": 10})
    assert r.json()["data"]["total"] >= 1
    # 更新
    r = c.put(f"/api/v1/kb/documents/{did}", json={"title": "改后口径"})
    assert r.json()["data"]["updated"] is True
    # 删除（软删）
    r = c.delete(f"/api/v1/kb/documents/{did}")
    assert r.json()["data"]["deleted"] is True


def test_kb_aliases_and_sql_examples():
    c = _client()
    r = c.post("/api/v1/kb/aliases", json={"term": "去重人数", "standard": "COUNT(DISTINCT userkey)"})
    assert r.status_code == 200 and r.json()["code"] == 0
    r = c.get("/api/v1/kb/aliases", params={"keyword": "去重"})
    assert r.json()["data"]["total"] >= 1

    r = c.post("/api/v1/kb/sql-examples",
               json={"question": "查GMV", "sql": "SELECT SUM(gmv) FROM t", "tables_used": "mid_hotel.t"})
    assert r.json()["code"] == 0 and r.json()["data"]["id"]


def test_embedding_rebuild_does_not_crash():
    c = _client()
    r = c.get("/api/v1/kb/embedding/rebuild")
    assert r.status_code == 200
    assert r.json()["data"]["status"] in ("DONE", "SKIPPED")


def test_nl2sql_generate_use_kb_flag():
    c = _client()
    on = c.post("/api/v1/nl2sql/generate", json={"question": "统计昨天UV", "use_kb": True})
    off = c.post("/api/v1/nl2sql/generate", json={"question": "统计昨天UV", "use_kb": False})
    assert on.status_code == 200 and off.status_code == 200
    assert on.json()["data"]["candidates"] and off.json()["data"]["candidates"]
    # 均需产出只读 SQL
    assert on.json()["data"]["candidates"][0]["is_read_only"] is True