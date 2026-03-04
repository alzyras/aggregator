from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {"api_token": os.getenv("TODOIST_API_TOKEN")}


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    token = credentials.get("api_token")
    if not token:
        return False, "Missing API token."
    return True, "Credentials present."
