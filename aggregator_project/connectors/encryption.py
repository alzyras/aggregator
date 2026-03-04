from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


class EncryptionError(Exception):
    pass


def _get_fernet() -> Fernet:
    key = getattr(settings, "ENCRYPTION_KEY", None)
    if not key:
        raise ImproperlyConfigured("ENCRYPTION_KEY must be set to encrypt tokens.")
    if isinstance(key, str):
        key_bytes = key.encode("utf-8")
    else:
        key_bytes = key
    return Fernet(key_bytes)


def encrypt_value(value: str) -> bytes:
    fernet = _get_fernet()
    return fernet.encrypt(value.encode("utf-8"))


def decrypt_value(value: bytes) -> str:
    fernet = _get_fernet()
    try:
        decrypted = fernet.decrypt(value)
    except InvalidToken as exc:
        raise EncryptionError("Invalid encryption token or key.") from exc
    return decrypted.decode("utf-8")
