from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

import pandas as pd

from aggregator.plugins.google_fit.services import GoogleFitService
from connectors.models import ConnectorAccount


logger = logging.getLogger(__name__)


class _LegacySettings:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str) -> None:
        self.google_fit = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }


class _LegacyState:
    def __init__(self, days_to_fetch: int) -> None:
        self._days_to_fetch = days_to_fetch

    def get_data_fetch_range_days(self) -> int:
        return self._days_to_fetch

    def is_full_load_completed(self) -> bool:
        return True

    def mark_full_load_completed(self) -> None:
        return None


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
        logger.warning("Old plugin entrypoint was called")
        client_id = self.credentials.get("client_id")
        client_secret = self.credentials.get("client_secret")
        refresh_token = self.credentials.get("refresh_token")
        if not client_id or not client_secret or not refresh_token:
            return []

        days_to_fetch = self._days_to_fetch(since)
        settings = _LegacySettings(client_id, client_secret, refresh_token)
        service = GoogleFitService.__new__(GoogleFitService)
        service.settings = settings
        service.state = _LegacyState(days_to_fetch)
        service.repository = None

        payload = service.fetch_data()
        return self._flatten_payload(payload)

    def _flatten_payload(self, payload: dict[str, pd.DataFrame] | None) -> list[dict[str, Any]]:
        if not payload:
            return []

        records: list[dict[str, Any]] = []
        steps_df = payload.get("google_fit_steps")
        if isinstance(steps_df, pd.DataFrame) and not steps_df.empty:
            for row in steps_df.to_dict(orient="records"):
                records.append(
                    {
                        "record_type": "steps",
                        "data_type": "steps",
                        "unit": "count",
                        "value": row.get("steps"),
                        "timestamp": row.get("timestamp"),
                        "user_id": row.get("user_id"),
                        "id": row.get("id"),
                    }
                )

        heart_df = payload.get("google_fit_heart")
        if isinstance(heart_df, pd.DataFrame) and not heart_df.empty:
            for row in heart_df.to_dict(orient="records"):
                records.append(
                    {
                        "record_type": "heart_rate",
                        "data_type": "heart_rate",
                        "unit": "bpm",
                        "value": row.get("heart_rate"),
                        "timestamp": row.get("timestamp"),
                        "user_id": row.get("user_id"),
                        "id": row.get("id"),
                    }
                )

        general_df = payload.get("google_fit_general")
        if isinstance(general_df, pd.DataFrame) and not general_df.empty:
            for row in general_df.to_dict(orient="records"):
                row["record_type"] = "general"
                records.append(row)

        return records

    def _days_to_fetch(self, since: datetime | None) -> int:
        if since is None:
            return 1825
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - since
        days = max(1, int(delta.total_seconds() // 86400))
        return days
