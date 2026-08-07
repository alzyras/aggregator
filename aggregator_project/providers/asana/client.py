from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.models import ConnectorAccount
from providers.asana.settings import get_asana_settings, get_asana_workspace_gids
from ingestion.normalizers.utils import parse_timestamp

BASE_URL = "https://app.asana.com/api/1.0"


class AsanaClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)
        self.settings = get_asana_settings(account.scopes)

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        workspace_gids = get_asana_workspace_gids(account.scopes)
        if not workspace_gids and account.external_account_id:
            workspace_gids = [str(account.external_account_id)]
        return {
            "access_token": account.get_access_token(),
            "workspace_gids": workspace_gids,
        }

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        access_token = self.credentials.get("access_token")
        workspace_gids = self.credentials.get("workspace_gids") or []
        if not access_token or not workspace_gids:
            return []
        if not self.settings.get("sync_tasks"):
            return []

        tasks: list[dict[str, Any]] = []
        for workspace_gid in workspace_gids:
            workspace = self._fetch_workspace(access_token, workspace_gid)
            projects = self._fetch_projects(access_token, workspace_gid)
            for project in projects:
                project_gid = project.get("gid")
                if not project_gid:
                    continue
                tasks.extend(
                    self._fetch_project_tasks(
                        access_token,
                        project_gid,
                        since,
                        workspace_name=str(workspace.get("name") or ""),
                        project_name=str(project.get("name") or ""),
                    )
                )

        filtered = []
        for task in tasks:
            task["__asana_settings"] = self.settings
            if not self.settings.get("include_archived"):
                if task.get("archived") or task.get("resource_subtype") == "archived":
                    continue
            if not self.settings.get("include_completed") and task.get("completed") is True:
                continue
            if not self.settings.get("sync_subtasks") and task.get("resource_subtype") == "subtask":
                continue
            if since:
                ts_candidates = [
                    parse_timestamp(task.get("modified_at")),
                    parse_timestamp(task.get("completed_at")),
                    parse_timestamp(task.get("created_at")),
                ]
                if not any(ts and ts > since for ts in ts_candidates):
                    continue
            filtered.append(task)

        return filtered

    def _fetch_workspace(self, access_token: str, workspace_gid: str) -> dict[str, Any]:
        payload = requests.get(
            f"{BASE_URL}/workspaces/{workspace_gid}",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"opt_fields": "gid,name"},
            timeout=30,
        )
        payload.raise_for_status()
        return payload.json().get("data") or {}

    def _fetch_projects(self, access_token: str, workspace_gid: str) -> list[dict[str, Any]]:
        params = {
            "opt_fields": "gid,name,created_at,modified_at,archived",
        }
        return self._paginate(
            f"{BASE_URL}/workspaces/{workspace_gid}/projects",
            access_token,
            params,
        )

    def _fetch_project_tasks(
        self,
        access_token: str,
        project_gid: str,
        since: datetime | None,
        *,
        workspace_name: str,
        project_name: str,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "opt_fields": ",".join(
                [
                    "gid",
                    "name",
                    "notes",
                    "created_at",
                    "modified_at",
                    "completed",
                    "completed_at",
                    "due_at",
                    "start_at",
                    "assignee.gid",
                    "assignee.name",
                    "created_by.gid",
                    "created_by.name",
                    "completed_by.gid",
                    "completed_by.name",
                    "last_modified_by.gid",
                    "last_modified_by.name",
                    "followers.gid",
                    "memberships.project.gid",
                    "resource_type",
                    "resource_subtype",
                    "archived",
                ]
            ),
            "limit": 100,
            "include_subtasks": "true" if self.settings.get("sync_subtasks") else "false",
        }
        if since is not None:
            params["modified_since"] = since.isoformat()
        tasks = self._paginate(
            f"{BASE_URL}/projects/{project_gid}/tasks",
            access_token,
            params,
        )
        for task in tasks:
            task["__asana_planner_context"] = {
                "workspace_name": workspace_name,
                "project_name": project_name,
            }
        return tasks

    def _paginate(
        self,
        url: str,
        access_token: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        headers = {"Authorization": f"Bearer {access_token}"}
        results: list[dict[str, Any]] = []
        offset: str | None = None

        while True:
            if offset:
                params["offset"] = offset
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            results.extend(payload.get("data", []))
            next_page = payload.get("next_page") or {}
            offset = next_page.get("offset")
            if not offset:
                break
        return results
