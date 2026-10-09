"""端到端冒烟测试：启动 App（含建表/seed），逐个打 API。"""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def run_all():
    def show(name, resp):
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        print(f"== {name} [{resp.status_code}]")
        print(str(body)[:320])
        return body

    with client:
        show("health", client.get("/api/v1/health"))
        show("login", client.post("/api/v1/auth/login", json={"employee_no": "1207799"}))
        show("databases", client.get("/api/v1/databases"))
        show("tables", client.get("/api/v1/databases/mid_hotel/tables"))
        show("table_detail", client.get("/api/v1/tables/1"))
        show("columns", client.get("/api/v1/tables/1/columns"))
        show("lineage", client.get("/api/v1/tables/1/lineage"))
        show("metadata_sync", client.post("/api/v1/metadata/sync", json={"mode": "all", "source": "file"}))

        show("search", client.post("/api/v1/search", json={"keyword": "酒店"}))

        model_body = {"db_name": "ads_business", "table_name": "ads_hotel_daily_report",
                      "table_comment": "酒店日报", "partition_by": "day",
                      "columns": [{"name": "report_date", "type": "string", "comment": "日期", "is_primary": False},
                                  {"name": "hotel_id", "type": "bigint", "comment": "酒店id"},
                                  {"name": "total_gmv", "type": "double", "comment": "gmv"}]}
        show("modeling_valid", client.post("/api/v1/modeling/validate", json=model_body))
        show("modeling_gen", client.post("/api/v1/modeling/generate", json=model_body))

        sql = "SELECT order_id, gmv FROM mid_hotel.dwd_hotel_order_detail WHERE dt='20241008'"
        show("sql_parse", client.post("/api/v1/sql/parse", json={"sql": sql}))
        show("sql_optimize", client.post("/api/v1/sql/optimize", json={"sql": "SELECT * FROM mid_hotel.dwd_hotel_order_detail"}))
        show("sql_rewrite", client.post("/api/v1/sql/rewrite", json={"sql": "SELECT * FROM x"}))
        show("sql_evaluate", client.post("/api/v1/sql/evaluate", json={"sql": sql}))
        show("sql_block_write", client.post("/api/v1/sql/evaluate", json={"sql": "INSERT INTO x VALUES(1)"}))

        show("quality_rules", client.get("/api/v1/quality/rules"))
        show("quality_run", client.post("/api/v1/quality/rules/1/run"))
        show("quality_reports", client.get("/api/v1/quality/reports"))
        show("nl2sql_gen", client.post("/api/v1/nl2sql/generate", json={"question": "酒店昨日GMV", "n": 1}))
        show("qa_ask", client.post("/api/v1/qa/ask", json={"question": "酒店GMV口径"}))
        show("notify", client.get("/api/v1/notify/messages"))
        show("llm_config", client.get("/api/v1/llm/config"))
        show("tasks", client.get("/api/v1/tasks"))

    print("\nALL SMOKE TESTS DONE")


if __name__ == "__main__":
    run_all()