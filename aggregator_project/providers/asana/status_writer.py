from __future__ import annotations

import requests
from ingestion.providers import STATUS_WRITEBACK_SUCCESS, StatusWritebackResult
from planner.models import PlannerItemState

from providers.asana.client import BASE_URL, AsanaClient


class AsanaStatusWriter:
    def __init__(self, account) -> None:
        self.client = AsanaClient(account)

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        completed = planner_status == PlannerItemState.PLANNER_STATUS_DONE
        token = self.client.credentials.get("access_token")
        if not token:
            raise RuntimeError("Missing Asana access token.")

        response = requests.put(
            f"{BASE_URL}/tasks/{source_entity_id}",
            headers={"Authorization": f"Bearer {token}"},
            json={"data": {"completed": completed}},
            timeout=30,
        )
        response.raise_for_status()
        data = (response.json() or {}).get("data") or {}
        actual_completed = bool(data.get("completed", completed))
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status="completed" if actual_completed else "open",
            external_completed=actual_completed,
            message="Saved to Asana.",
            raw=data,
        )
