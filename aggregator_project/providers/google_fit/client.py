from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.services import get_active_account
from providers.google_fit.credentials import env_credentials


class GoogleFitClient:
    def __init__(self) -> None:
        self.credentials = self._load_credentials()

    def _load_credentials(self) -> dict[str, Any]:
        account = get_active_account("google_fit")
        if account:
            return account.get_credentials()
        return env_credentials()

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        """
        TODO: Implement Google Fit OAuth flow and API calls.
        For now, return an empty list to keep the pipeline runnable.
        """
        _ = requests
        return []
