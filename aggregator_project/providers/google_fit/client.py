from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

from connectors.models import ConnectorAccount


logger = logging.getLogger(__name__)


class GoogleFitClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        credentials: dict[str, Any] = {
            "access_token": account.get_access_token(),
        }
        refresh_token = account.get_refresh_token()
        if refresh_token:
            credentials["refresh_token"] = refresh_token
        if isinstance(account.scopes, dict):
            credentials["client_id"] = account.scopes.get("client_id")
            credentials["client_secret"] = account.scopes.get("client_secret")
        return credentials

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        logger.warning("Google Fit fetch is not implemented for the Django connector yet.")
        _ = since
        return []
