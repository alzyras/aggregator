from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from connectors.models import ConnectorAccount
from ingestion.normalizers.utils import parse_timestamp
from providers.todoist.normalizer import is_completed_task
from providers.todoist.settings import get_todoist_settings

BASE_URL = "https://api.todoist.com/api/v1"
PAGE_SIZE = 200
COMPLETED_LOOKBACK_DAYS = 89


class TodoistClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)
        self.settings = get_todoist_settings(account.scopes)

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        return {"api_token": account.get_access_token()}

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        api_token = self.credentials.get("api_token")
        if not api_token:
            return []
        if not self.settings.get("sync_tasks"):
            return []
        headers = {"Authorization": f"Bearer {api_token}"}
        project_map = self._project_map(headers)
        section_map = self._section_map(headers)
        tasks = self._paginate(
            f"{BASE_URL}/tasks", headers=headers, result_key="results"
        )
        if self.settings.get("include_completed"):
            tasks = self._merge_tasks(tasks, self._completed_tasks(headers, since))

        filtered: list[dict[str, Any]] = []
        for task in tasks:
            task["__todoist_settings"] = self.settings
            task["__todoist_planner_context"] = {
                "project_name": project_map.get(str(task.get("project_id") or ""))
                or "",
                "section_name": section_map.get(str(task.get("section_id") or ""))
                or "",
            }
            if not self.settings.get("include_archived") and task.get("is_archived"):
                continue
            if not self.settings.get("include_completed") and is_completed_task(task):
                continue
            if since:
                ts_candidates = [
                    parse_timestamp(task.get("added_at") or task.get("created_at")),
                    parse_timestamp(
                        task.get("updated_at")
                        or task.get("sync_updated_at")
                        or task.get("date_updated")
                    ),
                    parse_timestamp(task.get("completed_at")),
                ]
                if not any(ts and ts > since for ts in ts_candidates):
                    continue
            filtered.append(task)
        return filtered

    def _project_map(self, headers: dict[str, str]) -> dict[str, str]:
        projects = self._paginate(
            f"{BASE_URL}/projects",
            headers=headers,
            result_key="results",
        )
        return {
            str(project.get("id")): str(project.get("name") or "")
            for project in projects
            if project.get("id") is not None and project.get("name")
        }

    def _section_map(self, headers: dict[str, str]) -> dict[str, str]:
        sections = self._paginate(
            f"{BASE_URL}/sections",
            headers=headers,
            result_key="results",
        )
        return {
            str(section.get("id")): str(section.get("name") or "")
            for section in sections
            if section.get("id") is not None and section.get("name")
        }

    def _completed_tasks(
        self,
        headers: dict[str, str],
        since: datetime | None,
    ) -> list[dict[str, Any]]:
        until = datetime.now(timezone.utc)
        earliest = until - timedelta(days=COMPLETED_LOOKBACK_DAYS)
        completed_since = max(self._aware(since), earliest) if since else earliest
        tasks = self._paginate(
            f"{BASE_URL}/tasks/completed/by_completion_date",
            headers=headers,
            params={
                "since": self._rfc3339(completed_since),
                "until": self._rfc3339(until),
            },
            result_key="items",
        )
        for task in tasks:
            task["checked"] = True
        return tasks

    def _paginate(
        self,
        url: str,
        *,
        headers: dict[str, str],
        result_key: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        query = {**(params or {}), "limit": PAGE_SIZE}
        results: list[dict[str, Any]] = []
        while True:
            response = requests.get(url, headers=headers, params=query, timeout=30)
            response.raise_for_status()
            payload = response.json() or {}
            if isinstance(payload, list):
                page = payload
                next_cursor = None
            else:
                page = payload.get(result_key) or []
                next_cursor = payload.get("next_cursor")
            results.extend(item for item in page if isinstance(item, dict))
            if not next_cursor:
                return results
            query["cursor"] = next_cursor

    def _merge_tasks(
        self,
        active: list[dict[str, Any]],
        completed: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        without_id: list[dict[str, Any]] = []
        for task in [*active, *completed]:
            task_id = task.get("id")
            if task_id is None:
                without_id.append(task)
                continue
            key = str(task_id)
            current = merged.get(key)
            if current is None or is_completed_task(task):
                merged[key] = task
        return [*merged.values(), *without_id]

    def _aware(self, value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    def _rfc3339(self, value: datetime) -> str:
        return self._aware(value).isoformat().replace("+00:00", "Z")
