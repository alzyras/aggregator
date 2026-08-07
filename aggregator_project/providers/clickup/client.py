from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.models import ConnectorAccount
from ingestion.normalizers.utils import parse_timestamp
from providers.clickup.settings import get_clickup_settings


BASE_URL = "https://api.clickup.com/api/v2"


class ClickUpClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.token = account.get_access_token()
        self.settings = get_clickup_settings(account.scopes)
        self.session = requests.Session()

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": self.token, "Content-Type": "application/json"}

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        if not self.token:
            return []
        tasks: list[dict[str, Any]] = []
        for list_id in self.settings.get("list_ids") or []:
            tasks.extend(self._fetch_list(str(list_id), since=since))
        return tasks

    def _fetch_list(self, list_id: str, *, since: datetime | None) -> list[dict[str, Any]]:
        page = 0
        tasks: list[dict[str, Any]] = []
        while True:
            response = self.session.get(
                f"{BASE_URL}/list/{list_id}/task",
                headers=self.headers,
                params={
                    "include_closed": str(bool(self.settings.get("include_closed"))).lower(),
                    "include_markdown_description": "true",
                    "order_by": "updated",
                    "reverse": "true",
                    "page": page,
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json() or {}
            raw_tasks = payload.get("tasks") if isinstance(payload, dict) else None
            if not isinstance(raw_tasks, list):
                raise ValueError("ClickUp tasks response did not contain a task list.")
            for task in raw_tasks:
                if not isinstance(task, dict):
                    continue
                updated_at = _clickup_timestamp(task.get("date_updated"))
                if since and updated_at and updated_at <= since:
                    continue
                task["__clickup_settings"] = self.settings
                task["__clickup_planner_context"] = _planner_context(task, list_id)
                tasks.append(task)
            if payload.get("last_page") is True or not raw_tasks:
                break
            page += 1
        return tasks


def _clickup_timestamp(value: Any):
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    return parse_timestamp(value)


def _planner_context(task: dict[str, Any], list_id: str) -> dict[str, str]:
    list_data = task.get("list") or {}
    folder_data = task.get("folder") or {}
    space_data = task.get("space") or {}
    return {
        "list_name": str(list_data.get("name") or list_id),
        "folder_name": str(folder_data.get("name") or ""),
        "space_name": str(space_data.get("name") or ""),
        "priority": str((task.get("priority") or {}).get("priority") or ""),
    }
