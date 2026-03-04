from __future__ import annotations

import os
from typing import Any


def env_credentials() -> dict[str, Any]:
    return {
        "access_token": os.getenv("ASANA_ACCESS_TOKEN")
        or os.getenv("ASANA_PERSONAL_ACCESS_TOKEN"),
        "workspace_gids": os.getenv("ASANA_WORKSPACE_GID"),
    }


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    token = credentials.get("access_token")
    workspace_gid = credentials.get("workspace_gid") or credentials.get("workspace_id")
    workspace_gids = credentials.get("workspace_gids")
    if not token:
        return False, "Missing access token."
    if not workspace_gid and not workspace_gids:
        return False, "Missing workspace GID."
    return True, "Credentials present."
