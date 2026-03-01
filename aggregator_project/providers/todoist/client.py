from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.models import ConnectorAccount
from providers.todoist.settings import get_todoist_settings

BASE_URL = "https://api.todoist.com/rest/v2"


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
        params: dict[str, Any] = {}
        response = requests.get(f"{BASE_URL}/tasks", headers=headers, params=params, timeout=30)
        response.raise_for_status()
        tasks: list[dict[str, Any]] = response.json() or []

        filtered: list[dict[str, Any]] = []
        for task in tasks:
            task["__todoist_settings"] = self.settings
            if not self.settings.get("include_archived") and task.get("is_archived"):
                continue
            if not self.settings.get("include_completed") and task.get("completed"):
                continue
            filtered.append(task)
        return filtered
