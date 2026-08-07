from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount


MASKED_TOKEN = "*********************"
DEFAULT_SETTINGS: dict[str, Any] = {
    "list_ids": [],
    "include_closed": True,
    "todo_status": "to do",
    "in_progress_status": "in progress",
    "done_status": "complete",
    "emit_task_created": True,
    "emit_task_updated": True,
    "emit_task_completed": True,
    "emit_task_state": False,
}


def get_clickup_settings(scopes: dict[str, Any] | list[Any] | None) -> dict[str, Any]:
    merged = {**DEFAULT_SETTINGS, "list_ids": []}
    if not isinstance(scopes, dict):
        return merged
    stored = scopes.get("clickup")
    if not isinstance(stored, dict):
        stored = scopes
    for key in DEFAULT_SETTINGS:
        if key in stored:
            merged[key] = stored[key]
    merged["list_ids"] = [
        str(value).strip()
        for value in merged.get("list_ids") or []
        if str(value).strip()
    ]
    for key in ("todo_status", "in_progress_status", "done_status"):
        merged[key] = str(merged.get(key) or "").strip()
    return merged


def extract_clickup_settings(cleaned_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "list_ids": list(cleaned_data.get("list_ids") or []),
        "include_closed": bool(cleaned_data.get("include_closed")),
        "todo_status": str(cleaned_data.get("todo_status") or "").strip(),
        "in_progress_status": str(cleaned_data.get("in_progress_status") or "").strip(),
        "done_status": str(cleaned_data.get("done_status") or "").strip(),
        "emit_task_created": bool(cleaned_data.get("emit_task_created")),
        "emit_task_updated": bool(cleaned_data.get("emit_task_updated")),
        "emit_task_completed": bool(cleaned_data.get("emit_task_completed")),
        "emit_task_state": bool(cleaned_data.get("emit_task_state")),
    }


def clickup_form_initial(account) -> dict[str, Any]:
    settings = get_clickup_settings(account.scopes)
    settings["list_ids"] = ", ".join(settings["list_ids"])
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
    account.scopes = {"clickup": extract_clickup_settings(cleaned_data)}


def source_url(raw: dict[str, Any]) -> str | None:
    value = raw.get("url")
    return str(value) if value else None
