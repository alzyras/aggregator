from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.models import ConnectorAccount
from providers.habitica.settings import get_habitica_settings
from ingestion.normalizers.utils import parse_timestamp

BASE_URL = "https://habitica.com/api/v3"


class HabiticaClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)
        self.settings = get_habitica_settings(account.scopes)
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

        if not any(
            (
                self.settings.get("sync_habits"),
                self.settings.get("sync_dailies"),
                self.settings.get("sync_todos"),
            )
        ):
            return []

        actor = self.get_user_actor_summary()
        habits = (
            self._fetch_tasks(user_id, api_token, "habits")
            if self.settings.get("sync_habits")
            else []
        )
        dailies = (
            self._fetch_tasks(user_id, api_token, "dailys")
            if self.settings.get("sync_dailies")
            else []
        )
        todos = (
            self._fetch_tasks(user_id, api_token, "todos")
            if self.settings.get("sync_todos")
            else []
        )
        completed_todos = (
            self._fetch_tasks(user_id, api_token, "completedTodos")
            if self.settings.get("sync_todos")
            else []
        )

        tasks = self._merge_tasks(todos, completed_todos)
        tasks.extend(habits)
        tasks.extend(dailies)

        for task in tasks:
            if actor:
                task["actor"] = actor
            task["_habitica_settings"] = self.settings
        if since:
            filtered: list[dict[str, Any]] = []
            for task in tasks:
                timestamps = [
                    parse_timestamp(task.get("updatedAt")),
                    parse_timestamp(task.get("dateCreated")),
                    parse_timestamp(task.get("dateCompleted")),
                ]
                # history entries
                for entry in task.get("history") or []:
                    ts = parse_timestamp(entry.get("date"))
                    if ts:
                        timestamps.append(ts)
                if any(ts and ts > since for ts in timestamps):
                    filtered.append(task)
            return filtered
        return tasks

    def get_user_profile(self) -> dict[str, Any]:
        if self._user_profile is not None:
            return self._user_profile
        user_id = self.credentials.get("user_id")
        api_token = self.credentials.get("api_token")
        if not user_id or not api_token:
            self._user_profile = {}
            return self._user_profile
        headers = self._headers(user_id, api_token)
        response = requests.get(f"{BASE_URL}/user", headers=headers, timeout=30)
        response.raise_for_status()
        self._user_profile = response.json().get("data", {})
        return self._user_profile

    def get_user_actor_summary(self) -> dict[str, Any]:
        profile = self.get_user_profile()
        if not profile:
            return {}
        actor_id = profile.get("id") or profile.get("_id") or profile.get("userId")
        profile_info = profile.get("profile") or {}
        auth_info = profile.get("auth") or {}
        local_auth = auth_info.get("local") or {}
        actor = {
            "id": actor_id,
            "display_name": profile_info.get("name") or profile.get("name"),
            "username": local_auth.get("username"),
        }
        return {key: value for key, value in actor.items() if value not in (None, "")}

    def _fetch_tasks(self, user_id: str, api_token: str, task_type: str) -> list[dict[str, Any]]:
        headers = self._headers(user_id, api_token)
        response = requests.get(
            f"{BASE_URL}/tasks/user",
            headers=headers,
            params={"type": task_type},
            timeout=30,
        )
        response.raise_for_status()
        return response.json().get("data", [])

    def _headers(self, user_id: str, api_token: str) -> dict[str, str]:
        return {
            "x-api-user": user_id,
            "x-api-key": api_token,
            "x-client": f"aggregator-{user_id}",
        }

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
