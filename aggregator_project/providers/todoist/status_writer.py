from __future__ import annotations

import requests
from ingestion.providers import (
    STATUS_WRITEBACK_SUCCESS,
    DescriptionWritebackResult,
    StatusWritebackResult,
)
from planner.models import PlannerItemState

from providers.todoist.client import TodoistClient

WRITE_BASE_URL = "https://api.todoist.com/api/v1"


class TodoistStatusWriter:
    def __init__(self, account) -> None:
        self.client = TodoistClient(account)

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        token = self.client.credentials.get("api_token")
        if not token:
            raise RuntimeError("Missing Todoist API token.")

        completed = planner_status == PlannerItemState.PLANNER_STATUS_DONE
        action = "close" if completed else "reopen"
        response = requests.post(
            f"{WRITE_BASE_URL}/tasks/{source_entity_id}/{action}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        response.raise_for_status()
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status="completed" if completed else "open",
            external_completed=completed,
            message="Saved to Todoist.",
        )

    def update_description(
        self,
        *,
        source_entity_id: str,
        description: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> DescriptionWritebackResult:
        token = self.client.credentials.get("api_token")
        if not token:
            raise RuntimeError("Missing Todoist API token.")

        response = requests.post(
            f"{WRITE_BASE_URL}/tasks/{source_entity_id}",
            headers={"Authorization": f"Bearer {token}"},
            json={"description": description},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() if response.content else {}
        data = data or {}
        return DescriptionWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            description=str(
                data.get("description")
                if data.get("description") is not None
                else description
            ),
            message="Description saved to Todoist.",
            raw=data,
        )
