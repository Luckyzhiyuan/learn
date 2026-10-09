"""千问 LLM 网关封装（Spec §4）。

- OpenAI 兼容 chat/completions。
- 配置来自 llm_config 表；API Key 解密。
- 异常降级：网络/424 等重试，Key 无效通知，最终 fallback 由 nl2sql 处理。
"""
import json

import httpx

from app.config import settings
from app.core.exceptions import llm_error
from app.core.security import decrypt_secret
from app.db.session import get_session
from app.db.models import LlmConfig


def _load_config(db):
    c = db.query(LlmConfig).filter(LlmConfig.enabled == 1).first()
    if not c:
        return None
    return {
        "api_url": c.api_url, "model": c.model_name,
        "api_key": decrypt_secret(c.api_key_enc) if c.api_key_enc else "",
        "timeout": c.timeout_sec, "max_tokens": c.max_tokens,
        "temperature": c.temperature, "fallback": bool(c.fallback),
    }


def get_active_config() -> dict | None:
    db = get_session()
    try:
        return _load_config(db)
    finally:
        db.close()


async def chat(prompt: str, use_schema: bool = True) -> str:
    """调用千问 chat/completions，返回模型输出文本。"""
    cfg = get_active_config()
    if not cfg:
        llm_error("LLM 网关未配置且不可用")
    url = cfg["api_url"]
    key = cfg["api_key"]
    if not key:
        llm_error("LLM API Key 未配置")

    messages = [
        {"role": "system", "content": "你是数仓SQL专家。根据给定的表结构，把用户中文问题转成Hive/Spark SQL。严格只输出JSON，不要多余文字。"},
        {"role": "user", "content": prompt},
    ]
    payload = {
        "model": cfg["model"], "temperature": cfg["temperature"],
        "max_tokens": cfg["max_tokens"], "messages": messages,
    }
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    last_exc = None
    async with httpx.AsyncClient(timeout=cfg["timeout"]) as client:
        for attempt in range(settings.qwen_max_retry + 1):
            try:
                r = await client.post(url, json=payload, headers=headers)
                if r.status_code == 429:
                    continue
                if r.status_code == 401:
                    llm_error("千问 Key 无效(401)")
                r.raise_for_status()
                data = r.json()
                return data["choices"][0]["message"]["content"]
            except llm_error.__class__:
                raise
            except Exception as e:
                last_exc = e
    raise llm_error(f"千问 API 调用失败: {last_exc}")