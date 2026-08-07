from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests

from connectors.models import ConnectorAccount
from ingestion.normalizers.utils import parse_timestamp
from providers.github_issues.identity import repository_name
from providers.github_issues.settings import get_github_settings


BASE_URL = "https://api.github.com"
API_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2026-03-10",
}


class GitHubIssuesClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.token = account.get_access_token()
        self.settings = get_github_settings(account.scopes)
        self.session = requests.Session()

    @property
    def headers(self) -> dict[str, str]:
        return {**API_HEADERS, "Authorization": f"Bearer {self.token}"}

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        if not self.token:
            return []
        repositories = self.settings.get("repositories") or []
        issues: list[dict[str, Any]] = []
        if repositories:
            for repository in repositories:
                issues.extend(
                    self._fetch_endpoint(
                        f"{BASE_URL}/repos/{repository}/issues",
                        since=since,
                        repository=repository,
                    )
                )
        else:
            issues.extend(
                self._fetch_endpoint(
                    f"{BASE_URL}/issues",
                    since=since,
                    repository="",
                    extra_params={"filter": "assigned"},
                )
            )
        return issues

    def _fetch_endpoint(
        self,
        url: str,
        *,
        since: datetime | None,
        repository: str,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "state": "all" if self.settings.get("include_closed") else "open",
            "sort": "updated",
            "direction": "desc",
            "per_page": 100,
        }
        if since:
            params["since"] = _github_timestamp(since)
        params.update(extra_params or {})

        results: list[dict[str, Any]] = []
        next_url: str | None = url
        next_params: dict[str, Any] | None = params
        while next_url:
            response = self.session.get(
                next_url,
                headers=self.headers,
                params=next_params,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json() or []
            if not isinstance(payload, list):
                raise ValueError("GitHub issues response was not a list.")
            for issue in payload:
                if not isinstance(issue, dict):
                    continue
                if issue.get("pull_request") and not self.settings.get(
                    "include_pull_requests"
                ):
                    continue
                if issue.get("state") == "closed" and not self.settings.get(
                    "include_closed"
                ):
                    continue
                if since:
                    updated_at = parse_timestamp(issue.get("updated_at"))
                    if updated_at and updated_at <= since:
                        continue
                repo_name = repository_name(issue, repository)
                if not repo_name:
                    continue
                issue["__github_repository"] = repo_name
                issue["__github_settings"] = self.settings
                issue["__github_planner_context"] = _planner_context(issue, repo_name)
                results.append(issue)
            links = getattr(response, "links", {}) or {}
            next_url = (links.get("next") or {}).get("url")
            next_params = None
        return results


def _github_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _planner_context(issue: dict[str, Any], repository: str) -> dict[str, Any]:
    labels = []
    for label in issue.get("labels") or []:
        name = label.get("name") if isinstance(label, dict) else label
        if name:
            labels.append(str(name))
    assignees = [
        str(assignee.get("login"))
        for assignee in issue.get("assignees") or []
        if isinstance(assignee, dict) and assignee.get("login")
    ]
    milestone = issue.get("milestone") or {}
    return {
        "repository": repository,
        "labels": labels,
        "assignees": assignees,
        "milestone": milestone.get("title") if isinstance(milestone, dict) else "",
    }
