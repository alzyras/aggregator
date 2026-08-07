from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    repositories = [
        value.strip()
        for value in os.getenv("GITHUB_ISSUES_REPOSITORIES", "").split(",")
        if value.strip()
    ]
    return {
        "api_token": os.getenv("GITHUB_ISSUES_TOKEN"),
        "repositories": repositories,
    }


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    if not credentials.get("api_token"):
        return False, "Missing GitHub personal access token."
    return True, "Credentials present."
