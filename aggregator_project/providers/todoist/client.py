from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.services import get_active_account
from providers.todoist.credentials import env_credentials


class TodoistClient:
    def __init__(self) -> None:
        self.credentials = self._load_credentials()

    def _load_credentials(self) -> dict[str, Any]:
        account = get_active_account("todoist")
        if account:
            return account.get_credentials()
        return env_credentials()

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        """
        TODO: Implement Todoist API calls.
        For now, return an empty list to keep the pipeline runnable.
        """
        _ = requests
        return []
