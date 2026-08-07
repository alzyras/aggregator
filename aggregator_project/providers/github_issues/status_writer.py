from __future__ import annotations

import requests

from ingestion.providers import (
    STATUS_WRITEBACK_SUCCESS,
    DescriptionWritebackResult,
    StatusWritebackResult,
)
from planner.models import PlannerItemState
from providers.github_issues.client import API_HEADERS, BASE_URL
from providers.github_issues.identity import parse_issue_identity


class GitHubIssuesStatusWriter:
    def __init__(self, account) -> None:
        self.token = account.get_access_token()

    @property
    def headers(self) -> dict[str, str]:
        return {**API_HEADERS, "Authorization": f"Bearer {self.token}"}

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        if not self.token:
            raise RuntimeError("Missing GitHub personal access token.")
        repository, number = parse_issue_identity(source_entity_id)
        completed = planner_status == PlannerItemState.PLANNER_STATUS_DONE
        payload = {
            "state": "closed" if completed else "open",
            "state_reason": "completed" if completed else "reopened",
        }
        response = requests.patch(
            f"{BASE_URL}/repos/{repository}/issues/{number}",
            headers=self.headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or {}
        actual_completed = data.get("state", payload["state"]) == "closed"
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status="closed" if actual_completed else "open",
            external_completed=actual_completed,
            message="Saved to GitHub.",
            raw=data,
        )

    def update_description(
        self,
        *,
        source_entity_id: str,
        description: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> DescriptionWritebackResult:
        if not self.token:
            raise RuntimeError("Missing GitHub personal access token.")
        repository, number = parse_issue_identity(source_entity_id)
        response = requests.patch(
            f"{BASE_URL}/repos/{repository}/issues/{number}",
            headers=self.headers,
            json={"body": description},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or {}
        return DescriptionWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            description=str(
                data.get("body") if data.get("body") is not None else description
            ),
            message="Description saved to GitHub.",
            raw=data,
        )
