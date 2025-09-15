import os
import pandas as pd
import logging
from typing import Tuple
import requests
from datetime import datetime, timedelta

from aggregator.plugin_interface import PluginInterface
from aggregator.plugin_config import PluginConfig
from aggregator.plugins.toggl.df_to_mysql import (
    write_toggl_dataframe_to_mysql_batch,
    execute_sql_file,
)

# Create a logger for this module
logger = logging.getLogger(__name__)


class TogglPlugin(PluginInterface):
    """Toggl plugin implementation using Reports API v2 for last 3 years of history with 1-year batching."""

    @property
    def name(self) -> str:
        """Return the name of the plugin."""
        return "toggl"

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from the Toggl API and return as a DataFrame."""
        logger.info("Fetching data from Toggl")

        api_token = os.environ.get("TOGGL_API_TOKEN")
        workspace_id = os.environ.get("TOGGL_WORKSPACE_ID")

        if not api_token or not workspace_id:
            logger.error("Missing Toggl API credentials (TOGGL_API_TOKEN / TOGGL_WORKSPACE_ID)")
            return pd.DataFrame()

        try:
            df = self._fetch_time_entries(api_token, workspace_id)
            
            # Mark full load as completed after successful fetch
            plugin_config = PluginConfig(self.name)
            if not plugin_config.is_full_load_completed():
                plugin_config.mark_full_load_completed()
            
            return df
        except Exception as e:
            logger.error(f"Error fetching data from Toggl: {e}", exc_info=True)
            return pd.DataFrame()

    def _fetch_time_entries(self, api_token: str, workspace_id: str) -> pd.DataFrame:
        """Fetch all available time entries from Toggl Reports API v2 (paginated, max 1-year intervals)."""
        url = "https://api.track.toggl.com/reports/api/v2/details"
        user_agent = "toggl-plugin"  # any string is fine

        # Get data fetch range from plugin config
        plugin_config = PluginConfig(self.name)
        days_to_fetch = plugin_config.get_data_fetch_range_days()
        
        today = datetime.utcnow()
        start_date = today - timedelta(days=days_to_fetch)
        all_entries = []

        while start_date < today:
            end_date = min(start_date + timedelta(days=365), today)
            since = start_date.strftime("%Y-%m-%d")
            until = end_date.strftime("%Y-%m-%d")

            logger.info(f"🟡 Fetching Toggl entries from {since} to {until} (paginated)...")

            page = 1
            while True:
                params = {
                    "workspace_id": workspace_id,
                    "user_agent": user_agent,
                    "since": since,
                    "until": until,
                    "page": page,
                    "per_page": 50,  # max allowed per request
                }

                response = requests.get(url, auth=(api_token, "api_token"), params=params)

                if response.status_code != 200:
                    logger.error(
                        f"❌ Failed to fetch page {page}: {response.status_code} {response.text}"
                    )
                    break

                data = response.json()
                entries = data.get("data", [])

                if not entries:
                    break

                all_entries.extend(entries)
                logger.info(f"✅ Retrieved page {page} with {len(entries)} entries")

                if len(entries) < params["per_page"]:
                    break

                page += 1

            start_date = end_date + timedelta(days=1)  # move to next 1-year interval

        if not all_entries:
            logger.warning("⚠️ No time entries returned from Toggl Reports API.")
            return pd.DataFrame()

        df = pd.DataFrame(all_entries)
        logger.info(f"✅ Retrieved total {len(df)} time entries from Toggl Reports API")

        # Normalize dataframe (map fields to your MySQL schema)
        df = df.rename(columns={
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
        })

        # Convert duration (ms → minutes)
        if "duration_minutes" in df:
            df["duration_minutes"] = df["duration_minutes"] / (1000 * 60.0)

        # Convert list columns to strings
        if "tags" in df:
            df["tags"] = df["tags"].apply(lambda x: ",".join(x) if isinstance(x, list) else "")

        required_cols = [
            "id", "user_id", "user_name", "project_id", "project_name", "client_id", "client_name",
            "description", "start_time", "end_time", "duration_minutes", "tags", "billable", "created_at"
        ]
        for col in required_cols:
            if col not in df:
                df[col] = None

        # Skip running entries (negative duration)
        if "duration_minutes" in df:
            df = df[df["duration_minutes"] >= 0]

        df = df[required_cols]
        return df

    def write_to_database(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Write DataFrame to database and return (inserted_count, duplicate_count)."""
        if df is not None and not df.empty:
            logger.info(f"Writing {len(df)} records to database")
            inserted_count, duplicate_count = write_toggl_dataframe_to_mysql_batch(df, "toggl_items")
            return inserted_count, duplicate_count
        return 0, 0

    def setup_database(self) -> bool:
        """Set up the database schema for this plugin."""
        logger.info("Setting up toggl database schema")
        execute_sql_file("aggregator/plugins/toggl/sql/toggl_entries.sql")
        return True
