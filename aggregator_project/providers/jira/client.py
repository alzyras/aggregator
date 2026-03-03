from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests
from requests.auth import HTTPBasicAuth

from connectors.models import ConnectorAccount
from providers.jira.settings import get_jira_config

logger = logging.getLogger(__name__)


class JiraClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.account = account
        self.config = get_jira_config(account.config)
        self.base_url = (self.config.get("base_url") or "").rstrip("/")
        self.timeout = 30
        self.max_attempts = 4
        self.session = requests.Session()

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        if not self.base_url:
            return []

        effective_since = self._effective_since(since)
        issues: list[dict[str, Any]] = []
        start_at = 0
        page_size = max(10, min(int(self.config.get("page_size") or 100), 100))

        while True:
            payload = self._search_issues(
                start_at=start_at,
                page_size=page_size,
                jql=self._build_jql(effective_since),
            )
            page_issues = payload.get("issues") or []
            for issue in page_issues:
                issue["__jira_config"] = self.config
                self._enrich_issue(issue)
            issues.extend(page_issues)
            total = int(payload.get("total") or 0)
            start_at += len(page_issues)
            if not page_issues or start_at >= total:
                break
        return issues

    def _effective_since(self, since: datetime | None) -> datetime | None:
        lookback_minutes = int(self.config.get("incremental_lookback_minutes") or 0)
        lookback = timedelta(minutes=max(0, lookback_minutes))
        if since:
            return since - lookback

        if self.config.get("full_sync"):
            return None

        backfill_days = int(self.config.get("initial_backfill_days") or 365)
        return datetime.utcnow().astimezone() - timedelta(days=max(1, backfill_days))

    def _fields(self) -> list[str]:
        fields = [
            "summary",
            "description",
            "created",
            "updated",
            "resolutiondate",
            "status",
            "issuetype",
            "project",
            "labels",
            "assignee",
            "reporter",
            "creator",
            "priority",
        ]
        if self.config.get("include_comments"):
            fields.append("comment")
        if self.config.get("include_worklogs"):
            fields.append("worklog")
        if self.config.get("include_attachments_metadata"):
            fields.append("attachment")
        if self.config.get("include_linked_issues"):
            fields.append("issuelinks")
        return fields

    def _expand_fields(self) -> list[str]:
        expands: list[str] = []
        if self.config.get("include_changelog"):
            expands.append("changelog")
        if self.config.get("include_sprints"):
            expands.append("names")
        return expands

    def _build_jql(self, since: datetime | None) -> str:
        clauses: list[str] = []
        custom_jql = (self.config.get("jql_filter") or "").strip()
        custom_base, custom_order = self._split_order_by(custom_jql)
        if custom_base:
            clauses.append(f"({custom_base})")

        project_keys = self.config.get("project_keys") or []
        if project_keys:
            quoted = ",".join(f'"{key}"' for key in project_keys)
            clauses.append(f"project in ({quoted})")

        issue_types = self.config.get("issue_types") or []
        if issue_types:
            quoted = ",".join(f'"{issue_type}"' for issue_type in issue_types)
            clauses.append(f"issuetype in ({quoted})")

        categories = self.config.get("include_status_categories") or []
        full_category_set = {"todo", "in_progress", "done"}
        selected = {str(item) for item in categories}
        if selected and selected != full_category_set:
            category_map = {
                "todo": "To Do",
                "in_progress": "In Progress",
                "done": "Done",
            }
            selected_names = [
                category_map[key] for key in ("todo", "in_progress", "done") if key in selected
            ]
            quoted = ",".join(f'"{value}"' for value in selected_names)
            clauses.append(f"statusCategory in ({quoted})")

        exclude_done_before_days = self.config.get("exclude_done_before_days")
        if exclude_done_before_days is not None:
            clauses.append(
                f"(statusCategory != Done OR updated >= -{int(exclude_done_before_days)}d)"
            )

        if since:
            clauses.append(f'updated >= "{self._format_jql_datetime(since)}"')

        if not clauses:
            return custom_order or "ORDER BY updated ASC"

        query = " AND ".join(clauses)
        return f"{query} {custom_order or 'ORDER BY updated ASC'}".strip()

    def _split_order_by(self, jql: str) -> tuple[str, str]:
        if not jql:
            return "", ""
        match = re.search(r"\border\s+by\b", jql, flags=re.IGNORECASE)
        if not match:
            return jql, ""
        return jql[: match.start()].strip(), jql[match.start() :].strip()

    def _format_jql_datetime(self, value: datetime) -> str:
        tz_name = self.config.get("timezone") or "UTC"
        tz = ZoneInfo(tz_name)
        aware = value
        if value.tzinfo is None:
            aware = value.replace(tzinfo=ZoneInfo("UTC"))
        return aware.astimezone(tz).strftime("%Y-%m-%d %H:%M")

    def _search_issues(self, *, start_at: int, page_size: int, jql: str) -> dict[str, Any]:
        params = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": page_size,
            "fields": ",".join(self._fields()),
            "expand": ",".join(self._expand_fields()),
        }
        last_error: RuntimeError | None = None
        for path in self._search_paths():
            try:
                return self._request("GET", path, params=params)
            except RuntimeError as exc:
                message = str(exc).lower()
                recoverable = (
                    "jira api error 404" in message
                    or "jira api error 405" in message
                    or "jira api error 410" in message
                    or "requested api has been removed" in message
                )
                if recoverable:
                    last_error = exc
                    continue
                raise
        if last_error:
            raise last_error
        raise RuntimeError("Jira search endpoint not available.")

    def _search_paths(self) -> list[str]:
        deployment = self.config.get("deployment_type") or "cloud"
        if deployment == "cloud":
            return ["/rest/api/3/search/jql", "/rest/api/3/search", "/rest/api/2/search"]
        # Keep server/DC-first ordering, but allow cloud endpoints as compatibility fallback.
        return ["/rest/api/2/search", "/rest/api/3/search/jql", "/rest/api/3/search"]

    def _enrich_issue(self, issue: dict[str, Any]) -> None:
        issue_key = issue.get("key")
        if not issue_key:
            return

        fields = issue.setdefault("fields", {})
        if self.config.get("include_comments") and "comment" not in fields:
            comments_payload = self._request("GET", f"{self._issue_path(issue_key)}/comment")
            issue["_expanded_comments"] = comments_payload.get("comments") or []

        if self.config.get("include_worklogs") and "worklog" not in fields:
            worklogs_payload = self._request("GET", f"{self._issue_path(issue_key)}/worklog")
            issue["_expanded_worklogs"] = worklogs_payload.get("worklogs") or []

    def _issue_path(self, issue_key: str) -> str:
        deployment = self.config.get("deployment_type") or "cloud"
        api_version = "3" if deployment == "cloud" else "2"
        return f"/rest/api/{api_version}/issue/{issue_key}"

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        headers.update(kwargs.pop("headers", {}))

        auth_method = self.config.get("auth_method")
        auth: HTTPBasicAuth | None = None
        if auth_method == "cloud_api_token":
            email = self.config.get("email")
            token = self.account.get_access_token()
            auth = HTTPBasicAuth(email, token)
        elif auth_method in {"personal_access_token", "oauth2"}:
            token = self.account.get_access_token()
            headers["Authorization"] = f"Bearer {token}"

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    auth=auth,
                    timeout=self.timeout,
                    **kwargs,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_attempts:
                    raise RuntimeError(f"Jira request failed: {exc}") from exc
                time.sleep(min(2 ** attempt, 8))
                continue

            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_attempts:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 8)
                logger.warning("jira_retry", extra={"url": url, "status": response.status_code, "attempt": attempt})
                time.sleep(delay)
                continue

            if response.status_code >= 400:
                raise RuntimeError(
                    f"Jira API error {response.status_code}: {response.text[:200]}"
                )

            return response.json()

        raise RuntimeError("Jira request exhausted retries.")
