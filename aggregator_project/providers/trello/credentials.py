from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {
        "api_key": os.getenv("TRELLO_API_KEY"),
        "api_token": os.getenv("TRELLO_API_TOKEN"),
    }


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    if not credentials.get("api_key"):
        return False, "Missing Trello API key."
    if not credentials.get("api_token"):
        return False, "Missing Trello API token."
    return True, "Credentials present."
