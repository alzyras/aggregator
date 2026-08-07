from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount


MASKED_TOKEN = "*********************"
DEFAULT_SETTINGS: dict[str, Any] = {
    "repositories": [],
    "include_closed": True,
    "include_pull_requests": False,
    "emit_task_created": True,
    "emit_task_updated": True,
    "emit_task_completed": True,
    "emit_task_state": False,
}


def get_github_settings(scopes: dict[str, Any] | list[Any] | None) -> dict[str, Any]:
    merged = {**DEFAULT_SETTINGS, "repositories": []}
    if not isinstance(scopes, dict):
        return merged
    stored = scopes.get("github")
    if not isinstance(stored, dict):
        stored = scopes
    for key in DEFAULT_SETTINGS:
        if key in stored:
            merged[key] = stored[key]
    merged["repositories"] = [
        str(value).strip().strip("/")
        for value in merged.get("repositories") or []
        if str(value).strip()
    ]
    return merged


def extract_github_settings(cleaned_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "repositories": list(cleaned_data.get("repositories") or []),
        "include_closed": bool(cleaned_data.get("include_closed")),
        "include_pull_requests": bool(cleaned_data.get("include_pull_requests")),
        "emit_task_created": bool(cleaned_data.get("emit_task_created")),
        "emit_task_updated": bool(cleaned_data.get("emit_task_updated")),
        "emit_task_completed": bool(cleaned_data.get("emit_task_completed")),
        "emit_task_state": bool(cleaned_data.get("emit_task_state")),
    }


def github_form_initial(account) -> dict[str, Any]:
    settings = get_github_settings(account.scopes)
    settings["repositories"] = "\n".join(settings["repositories"])
    if account.encrypted_access_token:
        settings["api_token"] = MASKED_TOKEN
    return settings


def resolve_masked_credentials(account, cleaned_data: dict[str, Any]) -> dict[str, Any]:
    if cleaned_data.get("api_token") == MASKED_TOKEN:
        return {**cleaned_data, "api_token": account.get_access_token()}
    return cleaned_data


def apply_credentials(account, cleaned_data: dict[str, Any]) -> None:
    token = cleaned_data.get("api_token")
    if token and token != MASKED_TOKEN:
        account.set_access_token(token)
    account.set_refresh_token(None)
    account.auth_type = ConnectorAccount.AUTH_API_TOKEN
    account.external_account_id = None
    account.token_expires_at = None
    account.scopes = {"github": extract_github_settings(cleaned_data)}


def source_url(raw: dict[str, Any]) -> str | None:
    value = raw.get("html_url")
    return str(value) if value else None
