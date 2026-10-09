"""Celery 异步任务（Spec §3.7 / §8.3）。

dev 模式下任务同步执行（无需 Redis）；prod 模式走队列。
"""
import uuid

from app.config import settings

if settings.task_mode == "dev":
    # dev：同步执行模式，直接调用
    def run_metadata_sync(payload: dict):
        from app.services import metadata_ingest
        source = payload.get("source", "file")
        scope = payload.get("scope", [])
        if source == "file":
            return metadata_ingest.sync_from_files(scope, "file")
        return metadata_ingest.sync_from_hive(scope)

    def run_lineage_build(payload: dict):
        from app.db.session import get_session
        from app.services import lineage
        db = get_session()
        try:
            return {"edges": lineage.build_from_sql_task(
                db, payload.get("sql", ""), payload.get("task_id"),
                payload.get("job_type", "spark_sql"), payload.get("owner", ""),
                payload.get("schedule_status", "SUCCESS"))}
        finally:
            db.close()

    def run_quality(payload: dict):
        from app.services import quality
        return quality.run_rule(payload["rule_id"], payload.get("user", ""))

else:
    # prod：真实 Celery
    from celery import Celery

    celery_app = Celery(
        "smart_dw",
        broker=settings.celery_broker_url,
        backend=settings.celery_result_backend,
    )
    celery_app.conf.update(task_serializer="json", result_serializer="json",
                           accept_content=["json"], timezone="Asia/Shanghai")

    TASKS = {}

    from celery import shared_task

    @shared_task
    def _metadata_sync(payload):
        return run_metadata_sync(payload)

    @shared_task
    def _lineage_build(payload):
        return run_lineage_build(payload)

    @shared_task
    def _quality(payload):
        return run_quality(payload)


# 统一暴露可测单元：
def submit(task_name: str, payload: dict) -> str:
    job_id = f"{task_name}-{uuid.uuid4().hex[:12]}"
    if settings.task_mode == "dev":
        handler = {"metadata_sync": run_metadata_sync, "lineage_build": run_lineage_build,
                   "quality": run_quality}.get(task_name)
        if handler:
            handler(payload)
        return job_id
    # prod 走 Celery
    AsyncTask = {"metadata_sync": _metadata_sync, "lineage_build": _lineage_build,
                 "quality": _quality}.get(task_name)
    if AsyncTask:
        AsyncTask.delay(payload)
    return job_id