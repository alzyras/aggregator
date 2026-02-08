from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.models import ConnectorAccount

BASE_URL = "https://habitica.com/api/v3"
X_CLIENT_HEADER = "aggregator"


class HabiticaClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)
        self._user_profile: dict[str, Any] | None = None

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        return {
            "user_id": account.external_account_id,
            "api_token": account.get_access_token(),
        }

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        user_id = self.credentials.get("user_id")
        api_token = self.credentials.get("api_token")
        if not user_id or not api_token:
            return []

        actor = self.get_user_profile()
        habits = self._fetch_tasks(user_id, api_token, "habits")
        dailies = self._fetch_tasks(user_id, api_token, "dailys")
        todos = self._fetch_tasks(user_id, api_token, "todos")
        completed_todos = self._fetch_tasks(user_id, api_token, "completedTodos")

        tasks = self._merge_tasks(todos, completed_todos)
        tasks.extend(habits)
        tasks.extend(dailies)

        for task in tasks:
            task["actor"] = actor
        return tasks

    def get_user_profile(self) -> dict[str, Any]:
        if self._user_profile is not None:
            return self._user_profile
        user_id = self.credentials.get("user_id")
        api_token = self.credentials.get("api_token")
        if not user_id or not api_token:
            self._user_profile = {}
            return self._user_profile
        headers = {
            "x-api-user": user_id,
            "x-api-key": api_token,
            "x-client": X_CLIENT_HEADER,
        }
        response = requests.get(f"{BASE_URL}/user", headers=headers, timeout=30)
        response.raise_for_status()
        self._user_profile = response.json().get("data", {})
        return self._user_profile

    def _fetch_tasks(self, user_id: str, api_token: str, task_type: str) -> list[dict[str, Any]]:
        headers = {
            "x-api-user": user_id,
            "x-api-key": api_token,
            "x-client": X_CLIENT_HEADER,
        }
        response = requests.get(
            f"{BASE_URL}/tasks/user",
            headers=headers,
            params={"type": task_type},
            timeout=30,
        )
        response.raise_for_status()
        return response.json().get("data", [])

    def _merge_tasks(
        self, todos: list[dict[str, Any]], completed_todos: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for task in todos + completed_todos:
            task_id = task.get("id") or task.get("_id")
            if not task_id:
                continue
            existing = merged.get(task_id)
            if not existing:
                merged[task_id] = task
                continue
            if task.get("completed") and not existing.get("completed"):
                merged[task_id] = task
        return list(merged.values())
