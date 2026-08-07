from __future__ import annotations

import requests

from ingestion.providers import (
    STATUS_WRITEBACK_SUCCESS,
    STATUS_WRITEBACK_UNSUPPORTED,
    DescriptionWritebackResult,
    StatusWritebackResult,
)
from planner.models import PlannerItemState
from providers.trello.client import BASE_URL
from providers.trello.settings import get_trello_settings


class TrelloStatusWriter:
    def __init__(self, account) -> None:
        self.api_token = account.get_access_token()
        self.api_key = account.get_refresh_token() or ""
        self.settings = get_trello_settings(account.scopes)

    @property
    def auth_params(self) -> dict[str, str]:
        return {"key": self.api_key, "token": self.api_token}

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        if not self.api_key or not self.api_token:
            raise RuntimeError("Missing Trello API key or token.")
        if planner_status == PlannerItemState.PLANNER_STATUS_DONE:
            return self._update_card(
                source_entity_id,
                {"closed": "true"},
                source_status="closed",
                completed=True,
            )

        target_name = _target_list_name(self.settings, planner_status)
        if not target_name:
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_UNSUPPORTED,
                message="Configure a Trello list name for this Planner state.",
            )
        card = self._card(source_entity_id)
        board_id = str(card.get("idBoard") or "")
        if not board_id:
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_UNSUPPORTED,
                message="Trello did not return a board for this card.",
            )
        target_list = self._find_list(board_id, target_name)
        if not target_list:
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_UNSUPPORTED,
                message=f"No open Trello list named {target_name!r} exists on this board.",
            )
        return self._update_card(
            source_entity_id,
            {"idList": str(target_list["id"]), "closed": "false"},
            source_status=str(target_list.get("name") or target_name),
            completed=False,
        )

    def update_description(
        self,
        *,
        source_entity_id: str,
        description: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> DescriptionWritebackResult:
        if not self.api_key or not self.api_token:
            raise RuntimeError("Missing Trello API key or token.")
        response = requests.put(
            f"{BASE_URL}/cards/{source_entity_id}",
            params={**self.auth_params, "desc": description},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or {}
        return DescriptionWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            description=str(data.get("desc") if data.get("desc") is not None else description),
            message="Description saved to Trello.",
            raw=data,
        )

    def _card(self, card_id: str) -> dict:
        response = requests.get(
            f"{BASE_URL}/cards/{card_id}",
            params={**self.auth_params, "fields": "idBoard,idList,closed"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or {}
        return data if isinstance(data, dict) else {}

    def _find_list(self, board_id: str, name: str) -> dict | None:
        response = requests.get(
            f"{BASE_URL}/boards/{board_id}/lists",
            params={**self.auth_params, "fields": "id,name", "filter": "open"},
            timeout=30,
        )
        response.raise_for_status()
        values = response.json() or []
        wanted = name.casefold().strip()
        for value in values:
            if isinstance(value, dict) and str(value.get("name") or "").casefold().strip() == wanted:
                return value
        return None

    def _update_card(
        self,
        card_id: str,
        values: dict[str, str],
        *,
        source_status: str,
        completed: bool,
    ) -> StatusWritebackResult:
        response = requests.put(
            f"{BASE_URL}/cards/{card_id}",
            params={**self.auth_params, **values},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json() or {}
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status=source_status,
            external_completed=completed,
            message="Saved to Trello.",
            raw=data,
        )


def _target_list_name(settings: dict, planner_status: str) -> str:
    if planner_status == PlannerItemState.PLANNER_STATUS_DOING:
        return str(settings.get("in_progress_list_name") or "").strip()
    return str(settings.get("todo_list_name") or "").strip()
