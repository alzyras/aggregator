from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.models import ConnectorAccount


class HabiticaClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        return {
            "user_id": account.external_account_id,
            "api_token": account.get_access_token(),
        }

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        """
        TODO: Implement Habitica API calls.
        For now, return an empty list to keep the pipeline runnable.
        """
        _ = requests
        return []
