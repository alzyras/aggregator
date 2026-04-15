from __future__ import annotations

import requests
from ingestion.providers import STATUS_WRITEBACK_SUCCESS, StatusWritebackResult
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
