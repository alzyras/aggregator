from __future__ import annotations

import requests
from ingestion.providers import (
    DescriptionWritebackResult,
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

    def update_description(
        self,
        *,
        source_entity_id: str,
        description: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> DescriptionWritebackResult:
        user_id = self.client.credentials.get("user_id")
        api_token = self.client.credentials.get("api_token")
        if not user_id or not api_token:
            raise RuntimeError("Missing Habitica credentials.")

        response = requests.put(
            f"{BASE_URL}/tasks/{source_entity_id}",
            headers=self.client._headers(user_id, api_token),
            json={"notes": description},
            timeout=30,
        )
        response.raise_for_status()
        data = (response.json() or {}).get("data") or {}
        return DescriptionWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            description=str(data.get("notes") if data.get("notes") is not None else description),
            message="Description saved to Habitica.",
            raw=data,
        )
