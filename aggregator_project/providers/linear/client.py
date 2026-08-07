from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from connectors.models import ConnectorAccount
from ingestion.normalizers.utils import parse_timestamp
from providers.linear.api import LinearAPI
from providers.linear.settings import get_linear_settings


ISSUE_FIELDS = """
    id
    identifier
    title
    description
    url
    createdAt
    updatedAt
    completedAt
    canceledAt
    archivedAt
    dueDate
    priority
    priorityLabel
    state { id name type }
    team { id key name }
    project { id name }
    cycle { id name number }
    assignee { id name email }
    labels { nodes { id name color } }
"""

ASSIGNED_ISSUES_QUERY = f"""
query AssignedIssues($first: Int!, $after: String, $filter: IssueFilter, $includeArchived: Boolean!) {{
  viewer {{
    assignedIssues(first: $first, after: $after, filter: $filter, includeArchived: $includeArchived, orderBy: updatedAt) {{
      nodes {{ {ISSUE_FIELDS} }}
      pageInfo {{ hasNextPage endCursor }}
    }}
  }}
}}
"""

ALL_ISSUES_QUERY = f"""
query AllIssues($first: Int!, $after: String, $filter: IssueFilter, $includeArchived: Boolean!) {{
  issues(first: $first, after: $after, filter: $filter, includeArchived: $includeArchived, orderBy: updatedAt) {{
    nodes {{ {ISSUE_FIELDS} }}
    pageInfo {{ hasNextPage endCursor }}
  }}
}}
"""


class LinearClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.token = account.get_access_token()
        self.settings = get_linear_settings(account.scopes)
        self.api = LinearAPI(self.token)

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        if not self.token:
            return []
        query = (
            ASSIGNED_ISSUES_QUERY
            if self.settings.get("only_assigned_to_me")
            else ALL_ISSUES_QUERY
        )
        after: str | None = None
        issues: list[dict[str, Any]] = []
        while True:
            variables: dict[str, Any] = {
                "first": 100,
                "after": after,
                "filter": (
                    {"updatedAt": {"gt": _linear_timestamp(since)}} if since else None
                ),
                "includeArchived": bool(self.settings.get("include_archived")),
            }
            data = self.api.request(query, variables)
            if self.settings.get("only_assigned_to_me"):
                connection = (data.get("viewer") or {}).get("assignedIssues") or {}
            else:
                connection = data.get("issues") or {}
            for issue in connection.get("nodes") or []:
                if not isinstance(issue, dict) or not self._include(issue, since):
                    continue
                issue["__linear_settings"] = self.settings
                issue["__linear_planner_context"] = _planner_context(issue)
                issues.append(issue)
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")
            if not after:
                break
        return issues

    def _include(self, issue: dict[str, Any], since: datetime | None) -> bool:
        team_keys = {value.lower() for value in self.settings.get("team_keys") or []}
        team_key = str((issue.get("team") or {}).get("key") or "").lower()
        if team_keys and team_key not in team_keys:
            return False
        state_type = str((issue.get("state") or {}).get("type") or "").lower()
        if state_type == "completed" and not self.settings.get("include_completed"):
            return False
        if state_type == "canceled" and not self.settings.get("include_canceled"):
            return False
        if issue.get("archivedAt") and not self.settings.get("include_archived"):
            return False
        if since:
            updated_at = parse_timestamp(issue.get("updatedAt"))
            if updated_at and updated_at <= since:
                return False
        return True


def _linear_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _planner_context(issue: dict[str, Any]) -> dict[str, Any]:
    labels = [
        str(label.get("name"))
        for label in ((issue.get("labels") or {}).get("nodes") or [])
        if isinstance(label, dict) and label.get("name")
    ]
    return {
        "identifier": issue.get("identifier") or "",
        "team": (issue.get("team") or {}).get("key") or "",
        "project": (issue.get("project") or {}).get("name") or "",
        "cycle": (issue.get("cycle") or {}).get("name") or "",
        "priority": issue.get("priorityLabel") or "",
        "labels": labels,
    }
