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

        habits = self._fetch_tasks(user_id, api_token, "habits")
        dailies = self._fetch_tasks(user_id, api_token, "dailys")
        todos = self._fetch_tasks(user_id, api_token, "todos")
        completed_todos = self._fetch_tasks(user_id, api_token, "completedTodos")

        records: list[dict[str, Any]] = []
        records.extend(self._expand_habits(habits))
        records.extend(self._expand_dailies(dailies))
        records.extend(self._expand_todos(todos))
        records.extend(self._expand_todos(completed_todos))
        return records

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

    def _expand_habits(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for task in tasks:
            history = task.get("history") or []
            for entry in history:
                if entry.get("date") is None:
                    continue
                records.append(
                    {
                        "task": task,
                        "occurrence": entry,
                        "task_type": "habit",
                    }
                )
        return records

    def _expand_dailies(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for task in tasks:
            history = task.get("history") or []
            if history:
                for entry in history:
                    if entry.get("date") is None:
                        continue
                    if entry.get("completed") is False:
                        continue
                    records.append(
                        {
                            "task": task,
                            "occurrence": entry,
                            "task_type": "daily",
                        }
                    )
                continue

            if task.get("completed") and task.get("dateCompleted"):
                records.append(
                    {
                        "task": task,
                        "occurrence": {
                            "date": task.get("dateCompleted"),
                            "value": task.get("value"),
                            "completed": True,
                        },
                        "task_type": "daily",
                    }
                )
        return records

    def _expand_todos(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for task in tasks:
            if not task.get("completed"):
                continue
            if not task.get("dateCompleted"):
                continue
            records.append(
                {
                    "task": task,
                    "occurrence": {
                        "date": task.get("dateCompleted"),
                        "value": task.get("value"),
                        "completed": True,
                    },
                    "task_type": "todo",
                }
            )
        return records
