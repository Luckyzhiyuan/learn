"""NL2SQL 编排（Spec §4 / F5.1）。

流程：构建表结构 prompt -> 调用千问 -> 解析 JSON -> 只读校验 -> 结构化候选。
LLM 不可用/配置为空 -> 规则降级生成。
"""
import json
import re
import uuid

from app.core.exceptions import nl2sql_fail, sql_not_readonly
from app.services.sql_parser import is_read_only
from app.services import llm_gateway, search
from app.db.session import get_session
from app.db.models import MetaDatabase, MetaTable, MetaColumn


def _schema_prompt(db_hint: str | None = None) -> str:
    db = get_session()
    try:
        q = db.query(MetaDatabase)
        if db_hint:
            q = q.filter(MetaDatabase.db_name == db_hint)
        dbs = q.limit(5).all()
        lines = []
        for d in dbs:
            for t in d.tables[:8]:
                cols = ", ".join(
                    f"{c.column_name}({c.column_type},{'分区' if c.is_partition else ''})"
                    for c in t.columns[:20])
                lines.append(f"{d.db_name}.{t.table_name}[{t.comment or t.table_name}]: {cols}")
        return "\n".join(lines[:80])
    finally:
        db.close()


def _pick_table(question: str) -> str:
    db = get_session()
    try:
        kw = ""
        m = re.search(r"(\w+)", question)
        r = search.search_sql(question, "table", 1, 5)
        if r["list"]:
            hit = r["list"][0]
            kw = f"{hit['db_name']}.{hit['name']}"
            return kw
        t = db.query(MetaTable).first()
        if t:
            d = db.query(MetaDatabase).filter(MetaDatabase.id == t.db_id).first()
            return f"{d.db_name}.{t.table_name}"
        return ""
    finally:
        db.close()


def generate(question: str, db_hint: str | None = None, n: int = 1) -> dict:
    fallback_used = False
    sql_text = ""
    schema = _schema_prompt(db_hint)
    prompt = (
        "【表结构】\n" + (schema or "(暂无元数据，请基于问题自动推断)\n") +
        "\n【规则】\n1. 只允许SELECT，禁止DELETE/UPDATE/INSERT/DDL。\n"
        "2. 分区字段必须过滤。\n3. 输出格式: {\"sql\": \"...\", \"confidence\": 0.0-1.0}\n\n"
        f"【问题】{question}"
    )
    try:
        raw = __import__("asyncio").run(llm_gateway.chat(prompt))
        parsed = _extract_json(raw)
        sql_text = parsed.get("sql", "")
        confidence = float(parsed.get("confidence", 0.8))
    except Exception:
        fallback_used = True
        sql_text, confidence = _fallback_sql(question)
        if not sql_text:
            raise nl2sql_fail("LLM 不可用且规则降级失败")

    if not is_read_only(sql_text):
        # 降级 / 拦截：非只读丢弃
        fallback_used = True
        sql_text, confidence = _fallback_sql(question)

    candidates = [_build_candidate(sql_text, confidence)]
    return {"candidates": candidates,
            "llm_used": "qwen3-8b",
            "fallback_used": fallback_used}


def _build_candidate(sql: str, confidence: float) -> dict:
    tables = []
    m = re.search(r"\bFROM\s+([\w.]+)", sql, re.I)
    if m:
        tables.append({"id": None, "table": m.group(1)})
    return {"sql": sql, "sql_id": uuid.uuid4().hex[:12],
            "confidence": round(confidence, 2), "tables_used": tables,
            "is_read_only": is_read_only(sql)}


def _extract_json(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.S)
        if m:
            return json.loads(m.group(0))
    return {}


def _fallback_sql(question: str) -> tuple[str, float]:
    table = _pick_table(question)
    if not table:
        return "", 0.0
    agg = "COUNT(*)"
    if "uv" in question or "去重" in question or "去查看" in question:
        agg = "COUNT(DISTINCT userkey)"
    elif "gmv" in question or "金额" in question or "sum" in question.lower():
        agg = "SUM(gmv)"
    sql = f"SELECT {agg} AS value FROM {table} WHERE dt = DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), 'yyyyMMdd')"
    return sql, 0.6