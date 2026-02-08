from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {
        "client_id": os.getenv("GOOGLE_FIT_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_FIT_CLIENT_SECRET"),
        "refresh_token": os.getenv("GOOGLE_FIT_REFRESH_TOKEN"),
        "access_token": os.getenv("GOOGLE_FIT_ACCESS_TOKEN"),
    }


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    refresh_token = credentials.get("refresh_token")
    client_id = credentials.get("client_id")
    client_secret = credentials.get("client_secret")

    if refresh_token and client_id and client_secret:
        return True, "OAuth refresh token present."
    return False, "Missing refresh token or client credentials."
