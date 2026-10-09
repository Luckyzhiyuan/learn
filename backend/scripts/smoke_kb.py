"""新增 KB/RAG 与 LLM 配置接口冒烟测试。"""
from fastapi.testclient import TestClient
from app.main import app

with TestClient(app) as client:
    def show(name, r):
        ok = r.status_code < 500
        print(f"[{'PASS' if ok else 'FAIL'}] {name} -> {r.status_code} {str(r.json()).replace(chr(10),' ')[:200]}")

    # 字段校验（build/import/lifespan 正常即无需 create_all 另行验证）
    show("GET llm/config", client.get("/api/v1/llm/config"))
    show("GET kb/documents (seed)", client.get("/api/v1/kb/documents"))
    show("POST kb/documents", client.post("/api/v1/kb/documents",
         json={"kb_type": "glossary", "title": "测试口径", "content": "测试内容", "tags": "test"}))
    show("POST kb/aliases", client.post("/api/v1/kb/aliases",
         json={"term": "去重人数", "standard": "COUNT(DISTINCT userkey)"}))
    show("GET kb/aliases", client.get("/api/v1/kb/aliases"))
    show("POST kb/sql-examples", client.post("/api/v1/kb/sql-examples",
         json={"question": "测试示例", "sql": "SELECT 1"}), )
    show("GET kb/embedding/rebuild", client.get("/api/v1/kb/embedding/rebuild"))
    show("POST nl2sql/generate(use_kb)", client.post("/api/v1/nl2sql/generate",
         json={"question": "统计昨天国内酒店UV", "use_kb": True}))
    show("POST nl2sql/generate(no_kb)", client.post("/api/v1/nl2sql/generate",
         json={"question": "统计昨天国内酒店UV", "use_kb": False}))

print("done")