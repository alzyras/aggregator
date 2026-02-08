from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

import pandas as pd

from connectors.models import ConnectorAccount
from aggregator.plugins.asana.get_done_tasks_df import get_asana_completed_tasks_df


logger = logging.getLogger(__name__)


class AsanaClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        return {
            "access_token": account.get_access_token(),
            "workspace_gid": account.external_account_id,
        }

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        logger.warning("Old plugin entrypoint was called")
        access_token = self.credentials.get("access_token")
        workspace_gid = self.credentials.get("workspace_gid")
        if not access_token or not workspace_gid:
            return []

        days_to_fetch = self._days_to_fetch(since)
        df = get_asana_completed_tasks_df(access_token, workspace_gid, days_to_fetch)
        if df is None or df.empty:
            return []
        if isinstance(df, pd.DataFrame):
            return df.to_dict(orient="records")
        return []

    def _days_to_fetch(self, since: datetime | None) -> int:
        if since is None:
            return 548
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - since
        days = max(1, int(delta.total_seconds() // 86400))
        return days
