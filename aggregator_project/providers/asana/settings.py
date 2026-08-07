from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount

DEFAULT_SETTINGS: dict[str, bool] = {
    "sync_tasks": True,
    "sync_subtasks": True,
    "include_completed": True,
    "include_archived": False,
    "emit_task_created": True,
    "emit_task_updated": True,
    "emit_task_completed": True,
    "emit_task_reopened": True,
    "emit_task_deleted": True,
    "task_state_created": False,
    "task_state_updated": False,
    "task_state_completed": False,
}

DEFAULT_WORKSPACES: list[str] = []
MASKED_TOKEN = "*********************"


def get_asana_settings(scopes: dict[str, Any] | list[Any] | None) -> dict[str, bool]:
    if not isinstance(scopes, dict):
        return DEFAULT_SETTINGS.copy()
    stored = scopes.get("asana")
    if not isinstance(stored, dict):
        stored = scopes
    merged = DEFAULT_SETTINGS.copy()
    for key, value in stored.items():
        if key in merged:
            merged[key] = bool(value)
    return merged


def get_asana_workspace_gids(scopes: dict[str, Any] | list[Any] | None) -> list[str]:
    if not isinstance(scopes, dict):
        return DEFAULT_WORKSPACES.copy()
    stored = scopes.get("asana_workspaces")
    if isinstance(stored, list):
        return [str(value) for value in stored if str(value).strip()]
    if isinstance(stored, str):
        return [value.strip() for value in stored.split(",") if value.strip()]
    return DEFAULT_WORKSPACES.copy()


def extract_asana_settings(cleaned_data: dict[str, Any]) -> dict[str, bool]:
    return {
        "sync_tasks": bool(cleaned_data.get("sync_tasks")),
        "sync_subtasks": bool(cleaned_data.get("sync_subtasks")),
        "include_completed": bool(cleaned_data.get("include_completed")),
        "include_archived": bool(cleaned_data.get("include_archived")),
        "emit_task_created": bool(cleaned_data.get("emit_task_created")),
        "emit_task_updated": bool(cleaned_data.get("emit_task_updated")),
        "emit_task_completed": bool(cleaned_data.get("emit_task_completed")),
        "emit_task_reopened": bool(cleaned_data.get("emit_task_reopened")),
        "emit_task_deleted": bool(cleaned_data.get("emit_task_deleted")),
        "task_state_created": bool(cleaned_data.get("task_state_created")),
        "task_state_updated": bool(cleaned_data.get("task_state_updated")),
        "task_state_completed": bool(cleaned_data.get("task_state_completed")),
    }


def extract_asana_workspaces(cleaned_data: dict[str, Any]) -> list[str]:
    raw = cleaned_data.get("workspace_gids") or []
    if isinstance(raw, list):
        return [str(value).strip() for value in raw if str(value).strip()]
    if isinstance(raw, str):
        return [value.strip() for value in raw.split(",") if value.strip()]
    return []


def apply_asana_settings(account, cleaned_data: dict[str, Any]) -> None:
    settings = extract_asana_settings(cleaned_data)
    workspaces = extract_asana_workspaces(cleaned_data)
    account.scopes = {"asana": settings, "asana_workspaces": workspaces}


def asana_form_initial(account) -> dict[str, Any]:
    settings = get_asana_settings(getattr(account, "scopes", None))
    workspace_gids = get_asana_workspace_gids(getattr(account, "scopes", None))
    if not workspace_gids and getattr(account, "external_account_id", None):
        workspace_gids = [str(account.external_account_id)]
    if workspace_gids:
        settings["workspace_gids"] = ",".join(workspace_gids)
    if getattr(account, "encrypted_access_token", None):
        settings["access_token"] = MASKED_TOKEN
    return settings


def is_masked_token(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == MASKED_TOKEN


def resolve_masked_credentials(account, cleaned_data: dict[str, Any]) -> dict[str, Any]:
    if is_masked_token(cleaned_data.get("access_token")):
        return {**cleaned_data, "access_token": account.get_access_token()}
    return cleaned_data


def apply_credentials(account, cleaned_data: dict[str, Any]) -> None:
    token = cleaned_data.get("access_token") or cleaned_data.get("api_token")
    if token and not is_masked_token(token):
        account.set_access_token(token)
    account.set_refresh_token(None)
    workspace_gids = extract_asana_workspaces(cleaned_data)
    account.external_account_id = workspace_gids[0] if workspace_gids else None
    account.auth_type = ConnectorAccount.AUTH_API_TOKEN
    account.token_expires_at = None
    apply_asana_settings(account, cleaned_data)


def source_url(raw: dict[str, Any]) -> str | None:
    value = raw.get("permalink_url")
    return str(value) if value else None
