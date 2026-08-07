from __future__ import annotations

import requests

from ingestion.providers import (
    STATUS_WRITEBACK_SUCCESS,
    STATUS_WRITEBACK_UNSUPPORTED,
    DescriptionWritebackResult,
    StatusWritebackResult,
)
from planner.models import PlannerItemState
from providers.clickup.client import BASE_URL
from providers.clickup.settings import get_clickup_settings


class ClickUpStatusWriter:
    def __init__(self, account) -> None:
        self.token = account.get_access_token()
        self.settings = get_clickup_settings(account.scopes)

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": self.token, "Content-Type": "application/json"}

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        if not self.token:
            raise RuntimeError("Missing ClickUp API token.")
        target = _target_status(self.settings, planner_status)
        if not target:
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_UNSUPPORTED,
                message="Configure a ClickUp status name for this Planner state.",
            )
        response = requests.put(
            f"{BASE_URL}/task/{source_entity_id}",
            headers=self.headers,
            json={"status": target},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or {}
        status_data = data.get("status") or {}
        if not isinstance(status_data, dict):
            status_data = {}
        status_name = str(status_data.get("status") or target)
        completed = str(status_data.get("type") or "").lower() == "closed"
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status=status_name,
            external_completed=completed,
            message="Saved to ClickUp.",
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
            raise RuntimeError("Missing ClickUp API token.")
        response = requests.put(
            f"{BASE_URL}/task/{source_entity_id}",
            headers=self.headers,
            json={"description": description},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or {}
        saved = data.get("markdown_description") or data.get("description") or description
        return DescriptionWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            description=str(saved),
            message="Description saved to ClickUp.",
            raw=data,
        )


def _target_status(settings: dict, planner_status: str) -> str:
    if planner_status == PlannerItemState.PLANNER_STATUS_DONE:
        return str(settings.get("done_status") or "").strip()
    if planner_status == PlannerItemState.PLANNER_STATUS_DOING:
        return str(settings.get("in_progress_status") or "").strip()
    return str(settings.get("todo_status") or "").strip()
