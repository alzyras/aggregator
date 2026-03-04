from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from django.conf import settings


@dataclass
class EncryptionResult:
    payload: str
    encrypted: bool


class EncryptionError(Exception):
    pass


def _get_fernet() -> Fernet | None:
    key = getattr(settings, "ENCRYPTION_KEY", None)
    if not key:
        return None
    if isinstance(key, str):
        key_bytes = key.encode("utf-8")
    else:
        key_bytes = key
    return Fernet(key_bytes)


def encrypt_payload(data: dict[str, Any]) -> EncryptionResult:
    raw = json.dumps(data)
    fernet = _get_fernet()
    if not fernet:
        return EncryptionResult(payload=raw, encrypted=False)
    token = fernet.encrypt(raw.encode("utf-8")).decode("utf-8")
    return EncryptionResult(payload=token, encrypted=True)


def decrypt_payload(payload: str) -> dict[str, Any]:
    fernet = _get_fernet()
    if not fernet:
        return json.loads(payload)
    try:
        decrypted = fernet.decrypt(payload.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise EncryptionError("Invalid encryption token or key.") from exc
    return json.loads(decrypted)
