from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {"api_token": os.getenv("CLICKUP_API_TOKEN")}


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    if not credentials.get("api_token"):
        return False, "Missing ClickUp API token."
    return True, "Credentials present."
