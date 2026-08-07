from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests

from connectors.models import ConnectorAccount
from ingestion.normalizers.utils import parse_timestamp
from providers.trello.settings import get_trello_settings


BASE_URL = "https://api.trello.com/1"
CARD_FIELDS = "id,idBoard,idList,name,desc,closed,dateLastActivity,due,url,labels"


class TrelloClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.api_token = account.get_access_token()
        self.api_key = account.get_refresh_token() or ""
        self.settings = get_trello_settings(account.scopes)
        self.session = requests.Session()

    @property
    def auth_params(self) -> dict[str, str]:
        return {"key": self.api_key, "token": self.api_token}

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        if not self.api_key or not self.api_token:
            return []
        cards: list[dict[str, Any]] = []
        for board_id in self.settings.get("board_ids") or []:
            cards.extend(self._fetch_board(str(board_id), since=since))
        return cards

    def _fetch_board(self, board_id: str, *, since: datetime | None) -> list[dict[str, Any]]:
        lists = self._lists_for_board(board_id)
        response = self.session.get(
            f"{BASE_URL}/boards/{board_id}/cards",
            params={
                **self.auth_params,
                "fields": CARD_FIELDS,
                "filter": "all" if self.settings.get("include_closed") else "open",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json() or []
        if not isinstance(payload, list):
            raise ValueError("Trello cards response was not a list.")
        results: list[dict[str, Any]] = []
        for card in payload:
            if not isinstance(card, dict):
                continue
            updated_at = parse_timestamp(card.get("dateLastActivity"))
            if since and updated_at and updated_at <= since:
                continue
            list_name = lists.get(str(card.get("idList") or ""), "")
            card["__trello_settings"] = self.settings
            card["__trello_created_at"] = _trello_created_at(card.get("id"))
            card["__trello_planner_context"] = {
                "board_id": board_id,
                "list_name": list_name,
                "labels": [
                    str(label.get("name") or "")
                    for label in card.get("labels") or []
                    if isinstance(label, dict) and label.get("name")
                ],
            }
            results.append(card)
        return results

    def _lists_for_board(self, board_id: str) -> dict[str, str]:
        response = self.session.get(
            f"{BASE_URL}/boards/{board_id}/lists",
            params={**self.auth_params, "fields": "id,name,closed", "filter": "all"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json() or []
        if not isinstance(payload, list):
            raise ValueError("Trello lists response was not a list.")
        return {
            str(entry.get("id")): str(entry.get("name") or "")
            for entry in payload
            if isinstance(entry, dict) and entry.get("id")
        }


def _trello_created_at(card_id: Any) -> str | None:
    if not isinstance(card_id, str) or len(card_id) < 8:
        return None
    try:
        seconds = int(card_id[:8], 16)
    except ValueError:
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")
