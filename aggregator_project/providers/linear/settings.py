from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount


MASKED_KEY = "*********************"
DEFAULT_SETTINGS: dict[str, Any] = {
    "team_keys": [],
    "only_assigned_to_me": True,
    "include_completed": True,
    "include_canceled": False,
    "include_archived": False,
    "emit_task_created": True,
    "emit_task_updated": True,
    "emit_task_completed": True,
    "emit_task_state": False,
}


def get_linear_settings(scopes: dict[str, Any] | list[Any] | None) -> dict[str, Any]:
    merged = {**DEFAULT_SETTINGS, "team_keys": []}
    if not isinstance(scopes, dict):
        return merged
    stored = scopes.get("linear")
    if not isinstance(stored, dict):
        stored = scopes
    for key in DEFAULT_SETTINGS:
        if key in stored:
            merged[key] = stored[key]
    merged["team_keys"] = [
        str(value).strip()
        for value in merged.get("team_keys") or []
        if str(value).strip()
    ]
    return merged


def extract_linear_settings(cleaned_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "team_keys": list(cleaned_data.get("team_keys") or []),
        "only_assigned_to_me": bool(cleaned_data.get("only_assigned_to_me")),
        "include_completed": bool(cleaned_data.get("include_completed")),
        "include_canceled": bool(cleaned_data.get("include_canceled")),
        "include_archived": bool(cleaned_data.get("include_archived")),
        "emit_task_created": bool(cleaned_data.get("emit_task_created")),
        "emit_task_updated": bool(cleaned_data.get("emit_task_updated")),
        "emit_task_completed": bool(cleaned_data.get("emit_task_completed")),
        "emit_task_state": bool(cleaned_data.get("emit_task_state")),
    }


def linear_form_initial(account) -> dict[str, Any]:
    settings = get_linear_settings(account.scopes)
    settings["team_keys"] = ", ".join(settings["team_keys"])
    if account.encrypted_access_token:
        settings["api_key"] = MASKED_KEY
    return settings


def resolve_masked_credentials(account, cleaned_data: dict[str, Any]) -> dict[str, Any]:
    if cleaned_data.get("api_key") == MASKED_KEY:
        return {**cleaned_data, "api_key": account.get_access_token()}
    return cleaned_data


def apply_credentials(account, cleaned_data: dict[str, Any]) -> None:
    token = cleaned_data.get("api_key")
    if token and token != MASKED_KEY:
        account.set_access_token(token)
    account.set_refresh_token(None)
    account.auth_type = ConnectorAccount.AUTH_API_TOKEN
    account.external_account_id = None
    account.token_expires_at = None
    account.scopes = {"linear": extract_linear_settings(cleaned_data)}


def source_url(raw: dict[str, Any]) -> str | None:
    value = raw.get("url")
    return str(value) if value else None
