from __future__ import annotations

import requests
from ingestion.providers import (
    STATUS_WRITEBACK_SUCCESS,
    STATUS_WRITEBACK_UNSUPPORTED,
    StatusWritebackResult,
)
from planner.models import PlannerItemState

from providers.habitica.client import BASE_URL, HabiticaClient


class HabiticaStatusWriter:
    def __init__(self, account) -> None:
        self.client = HabiticaClient(account)

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        if source_entity_type != "todo":
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_UNSUPPORTED,
                message="Habitica writeback is only supported for todos.",
            )

        user_id = self.client.credentials.get("user_id")
        api_token = self.client.credentials.get("api_token")
        if not user_id or not api_token:
            raise RuntimeError("Missing Habitica credentials.")

        completed = planner_status == PlannerItemState.PLANNER_STATUS_DONE
        response = requests.put(
            f"{BASE_URL}/tasks/{source_entity_id}",
            headers=self.client._headers(user_id, api_token),
            json={"completed": completed},
            timeout=30,
        )
        response.raise_for_status()
        data = (response.json() or {}).get("data") or {}
        actual_completed = bool(data.get("completed", completed))
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status="completed" if actual_completed else "open",
            external_completed=actual_completed,
            message="Saved to Habitica.",
            raw=data,
        )
