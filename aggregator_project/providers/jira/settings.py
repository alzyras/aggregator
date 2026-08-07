from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount

MASKED_SECRET = "*********************"

DEFAULT_CONFIG: dict[str, Any] = {
    "deployment_type": "cloud",
    "auth_method": "cloud_api_token",
    "base_url": "",
    "email": "",
    "project_keys": [],
    "jql_filter": "ORDER BY updated DESC",
    "issue_types": [],
    "include_status_categories": ["todo", "in_progress", "done"],
    "exclude_done_before_days": None,
    "timezone": "UTC",
    "include_comments": False,
    "include_worklogs": False,
    "include_changelog": True,
    "include_sprints": False,
    "include_attachments_metadata": False,
    "include_linked_issues": False,
    "emit_task_created": True,
    "emit_task_updated": True,
    "emit_task_completed": True,
    "emit_task_reopened": True,
    "emit_task_deleted": False,
    "emit_task_state": False,
    "emit_worklog_metrics": True,
    "full_sync": False,
    "initial_backfill_days": 365,
    "incremental_lookback_minutes": 30,
    "page_size": 100,
}


def get_jira_config(config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(config, dict):
        return DEFAULT_CONFIG.copy()
    stored = config.get("jira")
    if not isinstance(stored, dict):
        return DEFAULT_CONFIG.copy()
    merged = DEFAULT_CONFIG.copy()
    for key, value in stored.items():
        if key in merged:
            merged[key] = value
    return merged


def extract_jira_config(cleaned_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "deployment_type": cleaned_data.get("deployment_type") or "cloud",
        "auth_method": cleaned_data.get("auth_method") or "cloud_api_token",
        "base_url": cleaned_data.get("base_url") or "",
        "email": cleaned_data.get("email") or "",
        "project_keys": cleaned_data.get("project_keys") or [],
        "jql_filter": cleaned_data.get("jql_filter") or "",
        "issue_types": cleaned_data.get("issue_types") or [],
        "include_status_categories": cleaned_data.get("include_status_categories")
        or ["todo", "in_progress", "done"],
        "exclude_done_before_days": cleaned_data.get("exclude_done_before_days"),
        "timezone": cleaned_data.get("timezone") or "UTC",
        "include_comments": bool(cleaned_data.get("include_comments")),
        "include_worklogs": bool(cleaned_data.get("include_worklogs")),
        "include_changelog": bool(cleaned_data.get("include_changelog")),
        "include_sprints": bool(cleaned_data.get("include_sprints")),
        "include_attachments_metadata": bool(
            cleaned_data.get("include_attachments_metadata")
        ),
        "include_linked_issues": bool(cleaned_data.get("include_linked_issues")),
        "emit_task_created": bool(cleaned_data.get("emit_task_created")),
        "emit_task_updated": bool(cleaned_data.get("emit_task_updated")),
        "emit_task_completed": bool(cleaned_data.get("emit_task_completed")),
        "emit_task_reopened": bool(cleaned_data.get("emit_task_reopened")),
        "emit_task_deleted": bool(cleaned_data.get("emit_task_deleted")),
        "emit_task_state": bool(cleaned_data.get("emit_task_state")),
        "emit_worklog_metrics": bool(cleaned_data.get("emit_worklog_metrics")),
        "full_sync": bool(cleaned_data.get("full_sync")),
        "initial_backfill_days": int(cleaned_data.get("initial_backfill_days") or 365),
        "incremental_lookback_minutes": int(
            cleaned_data.get("incremental_lookback_minutes") or 30
        ),
        "page_size": int(cleaned_data.get("page_size") or 100),
    }


def apply_jira_settings(account, cleaned_data: dict[str, Any]) -> None:
    jira_config = extract_jira_config(cleaned_data)
    account.config = {"jira": jira_config}
    account.external_account_id = jira_config.get("base_url") or None
    account.auth_type = (
        ConnectorAccount.AUTH_OAUTH
        if jira_config.get("auth_method") == "oauth2"
        else ConnectorAccount.AUTH_API_TOKEN
    )


def jira_form_initial(account) -> dict[str, Any]:
    initial = get_jira_config(getattr(account, "config", None))
    auth_method = initial.get("auth_method")

    if auth_method == "cloud_api_token" and account.encrypted_access_token:
        initial["api_token"] = MASKED_SECRET
    elif auth_method == "personal_access_token" and account.encrypted_access_token:
        initial["pat_token"] = MASKED_SECRET
    elif auth_method == "oauth2":
        if account.encrypted_access_token:
            initial["client_secret"] = MASKED_SECRET
        if account.encrypted_refresh_token:
            initial["refresh_token"] = MASKED_SECRET

    return initial


def is_masked_secret(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == MASKED_SECRET


def resolve_masked_credentials(account, cleaned_data: dict[str, Any]) -> dict[str, Any]:
    stored_auth_method = get_jira_config(account.config).get("auth_method")
    auth_method = cleaned_data.get("auth_method") or stored_auth_method
    resolved = dict(cleaned_data)

    if auth_method == "cloud_api_token" and is_masked_secret(resolved.get("api_token")):
        resolved["api_token"] = account.get_access_token()
    if auth_method == "personal_access_token" and is_masked_secret(resolved.get("pat_token")):
        resolved["pat_token"] = account.get_access_token()
    if auth_method == "oauth2":
        if is_masked_secret(resolved.get("client_secret")):
            resolved["client_secret"] = account.get_access_token()
        if is_masked_secret(resolved.get("refresh_token")):
            resolved["refresh_token"] = account.get_refresh_token()
    return resolved


def apply_credentials(account, cleaned_data: dict[str, Any]) -> None:
    auth_method = cleaned_data.get("auth_method")
    if auth_method == "cloud_api_token":
        token = cleaned_data.get("api_token")
        if token and not is_masked_secret(token):
            account.set_access_token(token)
        account.set_refresh_token(None)
    elif auth_method == "personal_access_token":
        token = cleaned_data.get("pat_token")
        if token and not is_masked_secret(token):
            account.set_access_token(token)
        account.set_refresh_token(None)
    elif auth_method == "oauth2":
        client_secret = cleaned_data.get("client_secret")
        refresh_token = cleaned_data.get("refresh_token")
        if client_secret and not is_masked_secret(client_secret):
            account.set_access_token(client_secret)
        if refresh_token and not is_masked_secret(refresh_token):
            account.set_refresh_token(refresh_token)

    apply_jira_settings(account, cleaned_data)
    account.token_expires_at = None


def source_url(raw: dict[str, Any]) -> str | None:
    issue_key = raw.get("key")
    api_url = raw.get("self")
    if not issue_key or not api_url:
        return None
    base_url = str(api_url).split("/rest/api/", 1)[0].rstrip("/")
    if not base_url:
        return None
    return f"{base_url}/browse/{issue_key}"
