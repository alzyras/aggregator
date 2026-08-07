from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {"api_key": os.getenv("LINEAR_API_KEY")}


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    if not credentials.get("api_key"):
        return False, "Missing Linear API key."
    return True, "Credentials present."
