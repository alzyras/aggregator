from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {
        "user_id": os.getenv("HABITICA_USER_ID"),
        "api_token": os.getenv("HABITICA_API_TOKEN"),
    }


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    if not credentials.get("user_id"):
        return False, "Missing user id."
    if not credentials.get("api_token"):
        return False, "Missing API token."
    return True, "Credentials present."
