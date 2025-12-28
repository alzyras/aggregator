import logging
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import requests

from aggregator.core.apps import PluginService
from aggregator.infrastructure.filesystem import PluginState
from aggregator.settings import settings

from .repositories import TogglRepository

logger = logging.getLogger(__name__)


class TogglService(PluginService):
    name = "toggl"

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        self.state = PluginState(self.name)
        self.repository = TogglRepository()

    def setup(self) -> bool:
        self.repository.ensure_schema()
        return True

    def fetch_data(self) -> pd.DataFrame:
        api_token = self.settings.toggl.get("api_token")
        workspace_id = self.settings.toggl.get("workspace_id")

        if not api_token or not workspace_id:
            logger.error("Missing Toggl API credentials (TOGGL_API_TOKEN / TOGGL_WORKSPACE_ID)")
            return pd.DataFrame()

        df = self._fetch_time_entries(api_token, workspace_id)

        if not df.empty and not self.state.is_full_load_completed():
            self.state.mark_full_load_completed()

        return df

    def write_data(self, payload: pd.DataFrame) -> Tuple[int, int]:
        return self.repository.write_entries(payload)

    def _fetch_time_entries(self, api_token: str, workspace_id: str) -> pd.DataFrame:
        """Fetch all available time entries from Toggl Reports API v2 (paginated, max 1-year intervals)."""
        url = "https://api.track.toggl.com/reports/api/v2/details"
        user_agent = "toggl-plugin"

        days_to_fetch = self.state.get_data_fetch_range_days()
        today = datetime.utcnow()
        start_date = today - timedelta(days=days_to_fetch)
        all_entries = []

        while start_date < today:
            end_date = min(start_date + timedelta(days=365), today)
            since = start_date.strftime("%Y-%m-%d")
            until = end_date.strftime("%Y-%m-%d")

            logger.info("Fetching Toggl entries from %s to %s", since, until)

            page = 1
            while True:
                params = {
                    "workspace_id": workspace_id,
                    "user_agent": user_agent,
                    "since": since,
                    "until": until,
                    "page": page,
                    "per_page": 50,
                }

                response = requests.get(url, auth=(api_token, "api_token"), params=params)

                if response.status_code != 200:
                    logger.error(
                        "Failed to fetch page %s: %s %s",
                        page,
                        response.status_code,
                        response.text,
                    )
                    break

                data = response.json()
                entries = data.get("data", [])

                if not entries:
                    break

                all_entries.extend(entries)
                logger.info("Retrieved page %s with %s entries", page, len(entries))

                if len(entries) < params["per_page"]:
                    break

                page += 1

            start_date = end_date + timedelta(days=1)

        if not all_entries:
            logger.warning("No time entries returned from Toggl Reports API.")
            return pd.DataFrame()

        df = pd.DataFrame(all_entries)
        df = df.rename(
            columns={
                "id": "id",
                "uid": "user_id",
                "user": "user_name",
                "pid": "project_id",
                "project": "project_name",
                "wid": "client_id",
                "client": "client_name",
                "description": "description",
                "start": "start_time",
                "end": "end_time",
                "dur": "duration_minutes",
                "tags": "tags",
                "billable": "billable",
                "created_with": "created_at",
            }
        )

        if "duration_minutes" in df:
            df["duration_minutes"] = df["duration_minutes"] / (1000 * 60.0)

        if "tags" in df:
            df["tags"] = df["tags"].apply(lambda x: ",".join(x) if isinstance(x, list) else "")

        required_cols = [
            "id",
            "user_id",
            "user_name",
            "project_id",
            "project_name",
            "client_id",
            "client_name",
            "description",
            "start_time",
            "end_time",
            "duration_minutes",
            "tags",
            "billable",
            "created_at",
        ]
        for col in required_cols:
            if col not in df:
                df[col] = None

        if "duration_minutes" in df:
            df = df[df["duration_minutes"] >= 0]

        return df[required_cols]
