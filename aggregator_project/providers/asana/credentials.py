from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {
        "access_token": os.getenv("ASANA_ACCESS_TOKEN")
        or os.getenv("ASANA_PERSONAL_ACCESS_TOKEN")
    }


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    token = credentials.get("access_token")
    if not token:
        return False, "Missing access token."
    return True, "Credentials present."
