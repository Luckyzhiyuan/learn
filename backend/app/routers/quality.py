"""数据质量路由（Spec §3.5）。"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.exceptions import not_found
from app.core.response import ok
from app.deps import get_db_session
from app.db.models import QualityRule, QualityRuleResult, MetaTable
from app.schemas.biz import QualityRuleIn
from app.services import quality as qsvc

router = APIRouter(prefix="/api/v1/quality", tags=["quality"])


@router.get("/rules")
def list_rules(table_id: int | None = None, db: Session = Depends(get_db_session)):
    q = db.query(QualityRule)
    if table_id:
        q = q.filter(QualityRule.table_id == table_id)
    rows = q.all()
    return ok({"list": [{
        "id": r.id, "rule_name": r.rule_name, "rule_type": r.rule_type, "table_id": r.table_id,
        "column_id": r.column_id, "params": r.params, "sql": r.sql, "threshold": r.threshold,
        "enabled": r.enabled, "created_by": r.created_by, "created_at": r.created_at} for r in rows],
        "total": len(rows), "page": 1, "page_size": 100})


@router.post("/rules")
def create_rule(body: QualityRuleIn, db: Session = Depends(get_db_session)):
    r = QualityRule(rule_name=body.rule_name, rule_type=body.rule_type, table_id=body.table_id,
                    column_id=body.column_id, params=body.params, sql=body.sql,
                    threshold=body.threshold, enabled=int(body.enabled), created_by="1207799")
    db.add(r)
    db.commit()
    db.refresh(r)
    return ok({"id": r.id, "rule_name": r.rule_name})


@router.post("/rules/{rid}/run")
def run_rule(rid: int, body: dict | None = None, db: Session = Depends(get_db_session)):
    if not db.query(QualityRule).filter(QualityRule.id == rid).first():
        not_found("规则不存在")
    user = (body or {}).get("user") or "1207799"
    import uuid
    job_id = f"qjob-{uuid.uuid4().hex[:8]}"
    result = qsvc.run_rule(rid, user)
    return ok({"job_id": job_id, "rule_id": rid, "status": "DONE", **result})


@router.get("/reports")
def reports(table_id: int | None = None, rule_id: int | None = None,
            db: Session = Depends(get_db_session)):
    q = db.query(QualityRuleResult)
    if table_id or rule_id:
        q = q.join(QualityRule, QualityRuleResult.rule_id == QualityRule.id)
        if table_id:
            q = q.filter(QualityRule.table_id == table_id)
        if rule_id:
            q = q.filter(QualityRuleResult.rule_id == rule_id)
    q = q.order_by(QualityRuleResult.execute_time.desc()).limit(100)
    out = []
    for r in q.all():
        rule = db.query(QualityRule).filter(QualityRule.id == r.rule_id).first()
        tname = db.query(MetaTable).filter(MetaTable.id == rule.table_id).first().table_name if rule else ""
        out.append({"id": r.id, "rule_id": r.rule_id, "rule_name": rule.rule_name if rule else "",
                    "table_name": tname, "execute_time": r.execute_time, "pass": r.pass_,
                    "actual_value": r.actual_value, "detail": r.detail, "notified": r.notified})
    return ok({"list": out, "total": len(out), "page": 1, "page_size": 100})