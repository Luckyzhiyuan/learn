"""建模 / SQL助手 路由。"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.response import ok
from app.deps import get_db_session
from app.schemas.modeling import ModelingReq
from app.schemas.sql_assistant import SqlRequest, OptimizeRequest
from app.services import modeling as msvc, optimizer, evaluator, sql_parser

router = APIRouter(prefix="/api/v1", tags=["modeling|sql"])


# ---------- 智能建模 ----------
@router.post("/modeling/validate")
def modeling_validate(body: ModelingReq):
    result = msvc.validate(body.db_name, body.table_comment,
                           [c.model_dump() for c in body.columns],
                           body.partition_by, body.storage_format)
    return ok(result)


@router.post("/modeling/generate")
def modeling_generate(body: ModelingReq):
    sql = msvc.generate(body.db_name, body.table_name, body.table_comment,
                        [c.model_dump() for c in body.columns],
                        body.partition_by, body.storage_format, body.compress)
    return ok({"sql": sql})


# ---------- SQL 助手 ----------
@router.post("/sql/parse")
def sql_parse(body: SqlRequest, db: Session = Depends(get_db_session)):
    parsed = sql_parser.parse_sql(body.sql)
    # 关联元数据
    tables_out = []
    sql_parser.resolve_table_meta(db, parsed["tables"], tables_out)
    feats = parsed["features"]
    feats["join_tables"] = [t.get("table_id") for t in tables_out if t.get("table_id")]
    return ok({"tables": tables_out, "columns": parsed["columns"], "features": feats})


@router.post("/sql/optimize")
def sql_optimize(body: OptimizeRequest):
    result = optimizer.optimize(body.sql)
    return ok(result)


@router.post("/sql/rewrite")
def sql_rewrite(body: SqlRequest):
    res = optimizer.optimize(body.sql)
    rewritten = body.sql
    changes = []
    for s in res["suggestions"]:
        if s["rule"] == "no_select_star":
            n = 0
            import re as _r
            rewritten = _r.sub(r"\bSELECT \*", "SELECT col /* 建议：列裁剪 */", rewritten, flags=_r.I)
            changes.append({"rule": "column_pruning", "desc": "裁剪未使用字段(SELECT *)"})
            n += 1
    diff = unified_diff(body.sql, rewritten)
    return ok({"original_sql": body.sql, "rewritten_sql": rewritten, "diff": diff, "changes": changes})


@router.post("/sql/evaluate")
def sql_evaluate(body: OptimizeRequest):
    res = optimizer.optimize(body.sql)
    return ok(evaluator.evaluate(body.sql, res))


def unified_diff(a: str, b: str) -> str:
    import difflib
    return "\n".join(difflib.unified_diff(a.splitlines(), b.splitlines(), "original", "rewritten", lineterm=""))