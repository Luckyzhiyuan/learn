"""LLM 网关配置路由（Spec §3.9）。"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.exceptions import not_found
from app.core.response import ok
from app.core.security import encrypt_secret, mask_api_key, decrypt_secret
from app.deps import get_db_session
from app.db.models import LlmConfig
from app.schemas.biz import LlmConfigIn, LlmConfigOut

router = APIRouter(prefix="/api/v1/llm", tags=["llm"])


def _default_config(db) -> LlmConfig:
    c = db.query(LlmConfig).order_by(LlmConfig.id.desc()).first()
    if not c:
        c = LlmConfig(provider="qwen", model_name="qwen3-8b",
                      api_url="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
                      api_key_enc="")
        db.add(c)
        db.commit()
        db.refresh(c)
    return c


def _masked(db, c: LlmConfig) -> str:
    if not c.api_key_enc:
        return ""
    try:
        return mask_api_key(decrypt_secret(c.api_key_enc))
    except Exception:
        return "****"


@router.get("/config")
def get_config(db: Session = Depends(get_db_session)):
    c = _default_config(db)
    return ok(LlmConfigOut(
        provider=c.provider, model_name=c.model_name, api_url=c.api_url,
        api_key_masked=_masked(db, c),
        enabled=bool(c.enabled), fallback=bool(c.fallback), temperature=c.temperature,
        max_tokens=c.max_tokens, timeout_sec=c.timeout_sec).model_dump())


@router.put("/config")
def put_config(body: LlmConfigIn, db: Session = Depends(get_db_session)):
    c = _default_config(db)
    c.provider = body.provider
    c.model_name = body.model_name
    c.api_url = body.api_url
    c.enabled = int(body.enabled)
    c.fallback = int(body.fallback)
    c.temperature = body.temperature
    c.max_tokens = body.max_tokens
    c.timeout_sec = body.timeout_sec
    if body.api_key:
        c.api_key_enc = encrypt_secret(body.api_key)
    db.commit()
    return ok({"ok": True, "test_result": {"latency_ms": 0, "status": "config-saved"}})