from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

import pandas as pd

from aggregator.plugins.habitica.get_habits_dailies_df import fetch_all_data
from aggregator.plugins.habitica.get_todos_df import (
    create_dataframe,
    fetch_tags,
    get_completed_todos,
)
from connectors.models import ConnectorAccount


logger = logging.getLogger(__name__)


class HabiticaClient:
    def __init__(self, account: ConnectorAccount) -> None:
        self.credentials = self._load_credentials(account)

    def _load_credentials(self, account: ConnectorAccount) -> dict[str, Any]:
        return {
            "user_id": account.external_account_id,
            "api_token": account.get_access_token(),
        }

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        logger.warning("Old plugin entrypoint was called")
        user_id = self.credentials.get("user_id")
        api_token = self.credentials.get("api_token")
        if not user_id or not api_token:
            return []

        tag_dict = fetch_tags(user_id, api_token)
        df_habits_dailies = fetch_all_data(user_id, api_token)
        completed_todos = get_completed_todos(user_id, api_token, tag_dict)
        df_todos = create_dataframe(completed_todos) if completed_todos else pd.DataFrame()

        frames = [df for df in [df_habits_dailies, df_todos] if df is not None]
        if not frames:
            return []

        df = pd.concat(frames, ignore_index=True)
        if df.empty:
            return []
        return df.to_dict(orient="records")
