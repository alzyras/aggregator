from __future__ import annotations

from typing import Any


def env_credentials() -> dict[str, Any]:
    return {}


def validate_credentials(credentials: dict[str, Any]) -> tuple[bool, str]:
    base_url = (credentials.get("base_url") or "").strip()
    auth_method = credentials.get("auth_method")
    if not base_url:
        return False, "Missing Jira base URL."
    if auth_method not in {"cloud_api_token", "personal_access_token", "oauth2"}:
        return False, "Unsupported Jira auth method."

    if auth_method == "cloud_api_token":
        if not credentials.get("email"):
            return False, "Email is required for Jira Cloud API token auth."
        if not credentials.get("api_token"):
            return False, "API token is required."
    elif auth_method == "personal_access_token":
        if not credentials.get("pat_token"):
            return False, "PAT token is required."
    elif auth_method == "oauth2":
        if not credentials.get("client_id"):
            return False, "Client ID is required for OAuth2."
        if not credentials.get("client_secret"):
            return False, "Client secret is required for OAuth2."
        if not credentials.get("refresh_token"):
            return False, "Refresh token is required for OAuth2."

    return True, "Credentials present."

