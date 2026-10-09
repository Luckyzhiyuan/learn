"""FastAPI 入口：装配路由、统一错误处理、启动时建表+Seed+建 ES 索引。"""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from app.config import settings
from app.core.exceptions import BizError
from app.db.session import create_all
from app.routers import Routers


def init_es_index():
    """ES 可用时创建索引（异常静默，回退 SQL 检索）。"""
    try:
        from app.services import search
        if not search.es_available():
            return False
        client = search._es_client()
        if not client.indices.exists(index=settings.es_index_meta):
            client.indices.create(index=settings.es_index_meta, body={
                "settings": {"analysis": {"analyzer": {"ngram": {
                    "type": "custom",
                    "tokenizer": "ngram", "filter": ["lowercase"]}}}},
                "mappings": {"properties": {
                    "name": {"type": "keyword"},
                    "name_ngram": {"type": "text", "analyzer": "ngram", "search_analyzer": "standard"},
                    "comment": {"type": "text"},
                    "db_name": {"type": "keyword"},
                    "entity_type": {"type": "keyword"},
                    "table_id": {"type": "long"}, "column_id": {"type": "long"},
                }}})
        if not client.indices.exists(index=settings.es_index_kb):
            client.indices.create(index=settings.es_index_kb, body={
                "mappings": {"properties": {
                    "entity_type": {"type": "keyword"},
                    "title": {"type": "keyword"},
                    "content": {"type": "text"},
                    "tags": {"type": "text"},
                    "doc_id": {"type": "long"},
                }}})
        return True
    except Exception:
        return False


def seed_demo_data():
    """开发环境种子数据：若无任何库/表则注入示例。"""
    from app.db.session import get_session
    from app.db.models import MetaDatabase, MetaTable, MetaColumn, MetaPartition
    db = get_session()
    try:
        if db.query(MetaDatabase).count() > 0:
            return
        d = MetaDatabase(db_name="mid_hotel", layer="mid", owner="1207799",
                         remark="酒旅中间层")
        db.add(d)
        d2 = MetaDatabase(db_name="ads_business", layer="ads", owner="1207799",
                          remark="应用层")
        db.add(d2)
        db.flush()

        t = MetaTable(db_id=d.id, table_name="dwd_hotel_order_detail",
                      comment="酒店订单明细(事实)", is_partitioned=1, table_size_bytes=int(1.2e12),
                      row_count=int(8e8), file_count=1200, little_file_count=230, owner="1207799")
        db.add(t)
        db.flush()
        for col in ["order_id", "hotel_id", "userkey", "city_id", "gmv", "room_night",
                    "order_status", "dt"]:
            db.add(MetaColumn(table_id=t.id, column_name=col,
                              column_type="bigint" if col in ("gmv", "room_night", "hotel_id", "userkey") else "string",
                              comment=f"{col}字段", is_partition=int(col == "dt"), ordinal=1))
        db.add(MetaPartition(table_id=t.id, partition_key="dt", partition_value="20241008"))

        t2 = MetaTable(db_id=d.id, table_name="dwd_hotel_room_inventory",
                       comment="酒店房间库存(事实)", is_partitioned=1,
                       row_count=int(3e8), file_count=900, owner="1207799")
        db.add(t2)
        db.flush()
        for col in ["hotel_id", "room_type_id", "inventory", "dt"]:
            db.add(MetaColumn(table_id=t2.id, column_name=col,
                              column_type="string", comment=f"{col}字段",
                              is_partition=int(col == "dt")))

        a1 = MetaTable(db_id=d2.id, table_name="ads_hotel_daily_gmv",
                       comment="酒店每日GMV指标汇总", is_partitioned=1,
                       row_count=int(2e6), file_count=30, owner="1207799")
        db.add(a1)
        db.flush()
        for col in ["report_date", "hotel_id", "city_id", "total_gmv", "order_cnt", "pv", "uv"]:
            db.add(MetaColumn(table_id=a1.id, column_name=col, column_type="string",
                              comment=f"{col}字段", is_partition=int(col == "report_date")))

        # 血缘：mid -> ads
        from app.db.models import MetaLineage
        db.add(MetaLineage(src_table_id=t.id, dst_table_id=a1.id, edge_tag="sql_parse",
                           job_type="spark_sql", owner="1207799", schedule_status="SUCCESS"))
        db.commit()

        # 种子质量规则
        from app.db.models import QualityRule
        db.add(QualityRule(rule_name="订单量波动", rule_type="volatility", table_id=a1.id,
                           params={"window": 7}, threshold=0.2, enabled=1))
        db.add(QualityRule(rule_name="rowid非空", rule_type="not_null", table_id=t.id,
                           column_id=1, threshold=0.0, enabled=1))
        db.commit()
    finally:
        db.close()


def seed_kb_data():
    """开发环境知识库种子数据：口径/同义词/示例SQL（Spec §5.2）。"""
    from app.db.session import get_session
    from app.db.models import KBDocument, KBAlias, KBSqlExample
    db = get_session()
    try:
        if db.query(KBDocument).count() > 0:
            return
        db.add(KBDocument(kb_type="glossary", title="酒店UV口径",
                          content="UV=去重用户数，使用 userkey 字段 COUNT(DISTINCT userkey)",
                          tags="uv,去重,用户", created_by="1207799"))
        db.add(KBDocument(kb_type="glossary", title="国内酒店GMV口径",
                          content="GMV=国内酒店各端已支付订单金额之和，来源 dwd_hotel_order_detail.gmv",
                          tags="gmv,金额,订单,酒店", created_by="1207799"))
        db.add(KBAlias(term="UV", standard="COUNT(DISTINCT userkey)", remark="去重用户数"))
        db.add(KBAlias(term="昨日", standard="dt = DATE_SUB(CURRENT_DATE,1)", remark="日期词"))
        db.add(KBAlias(term="GMV", standard="SUM(gmv)", remark="交易额"))
        db.add(KBSqlExample(question="统计昨天国内酒店UV",
                            sql="SELECT COUNT(DISTINCT userkey) AS uv FROM mid_hotel.dwd_hotel_order_detail "
                                "WHERE dt=DATE_FORMAT(DATE_SUB(CURRENT_DATE,1),'yyyyMMdd')",
                            tables_used="mid_hotel.dwd_hotel_order_detail", tags="uv,酒店,昨日"))
        db.commit()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_all()
    seed_demo_data()
    seed_kb_data()
    init_es_index()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan, docs_url="/docs", redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


@app.exception_handler(BizError)
async def biz_error_handler(request: Request, exc: BizError):
    return JSONResponse(status_code=exc.status_code,
                        content={"code": exc.code, "message": exc.biz, "data": None})


@app.get("/api/v1/health")
def health():
    return {"code": 0, "message": "ok", "data": {"service": settings.app_name}}


FRONTEND_INDEX = Path(__file__).resolve().parent.parent.parent / "frontend" / "index.html"


@app.get("/", include_in_schema=False)
def index():
    """根路径直接返回前端单页（同源访问，免跨域）。"""
    if FRONTEND_INDEX.exists():
        return FileResponse(FRONTEND_INDEX)
    return JSONResponse({"code": 0, "message": "ok", "data": {"docs": "/docs"}})


for r in Routers:
    app.include_router(r)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host=settings.app_host, port=settings.app_port, reload=settings.debug)