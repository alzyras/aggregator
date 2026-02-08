from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.models import ConnectorAccount


class GoogleFitClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        credentials = {"access_token": account.get_access_token()}
        refresh_token = account.get_refresh_token()
        if refresh_token:
            credentials["refresh_token"] = refresh_token
        return credentials

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        """
        TODO: Implement Google Fit OAuth flow and API calls.
        For now, return an empty list to keep the pipeline runnable.
        """
        _ = requests
        return []
