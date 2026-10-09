"""认证与安全：JWT 签发/校验、只读校验用的密码工具。"""
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import os

from jose import jwt
from passlib.context import CryptContext

from app.config import settings

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(p: str) -> str:
    return pwd_ctx.hash(p)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)


def create_access_token(subject: str, extra: dict | None = None) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_expire_minutes)
    payload = {"sub": subject, "exp": expire}
    if extra:
        payload.update(extra)
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])


# ---- AES-256-GCM 用于加密 LLM API Key（Spec §4.6） ----
def _aes_key() -> bytes:
    return hashlib.sha256(settings.llm_key_enc_master.encode()).digest()


def encrypt_secret(plain: str) -> str:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    key = _aes_key()
    nonce = os.urandom(12)
    ct = AESGCM(key).encrypt(nonce, plain.encode(), None)
    return base64.b64encode(nonce + ct).decode()


def decrypt_secret(blob: str) -> str:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    key = _aes_key()
    raw = base64.b64decode(blob)
    nonce, ct = raw[:12], raw[12:]
    return AESGCM(key).decrypt(nonce, ct, None).decode()


def mask_api_key(key: str) -> str:
    """sk- 之后仅保留末 3 位（Spec §4.6）。"""
    if len(key) <= 4:
        return "****"
    return key[:3] + "****" + key[-3:]