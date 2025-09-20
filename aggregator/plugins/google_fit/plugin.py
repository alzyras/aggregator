import os
import json
import logging
import requests
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta, timezone
import uuid
import time

from aggregator.plugin_interface import PluginInterface
from aggregator.plugin_config import PluginConfig
from aggregator.plugins.google_fit.df_to_mysql import (
    write_samsung_dataframe_to_mysql_batch,
    execute_sql_file,
)

logger = logging.getLogger(__name__)

# Plugin configuration
PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(PLUGIN_DIR, "data", "google_fit_tokens.json")
TOKEN_URL = "https://oauth2.googleapis.com/token"
REDIRECT_URI = "http://localhost:8080/oauth2callback"

# Minimal scopes for required data
SCOPES = [
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.heart_rate.read", 
    "https://www.googleapis.com/auth/fitness.body.read",
]

# Data sources - only the essential ones
DATA_SOURCES = {
    "steps": "derived:com.google.step_count.delta:com.google.android.gms:merge_step_deltas",
    "heart_rate": "derived:com.google.heart_rate.bpm:com.google.android.gms:merge_heart_rate_bpm",
    "weight": "derived:com.google.weight:com.google.android.gms:merge_weight",
    "height": "derived:com.google.height:com.google.android.gms:merge_height",
    "body_fat": "derived:com.google.body.fat.percentage:com.google.android.gms:merged",
}

# Chunk size for API requests
CHUNK_DAYS = 90


class GoogleFitPlugin(PluginInterface):
    """Ultra-minimal Google Fit plugin for essential health data."""

    @property
    def name(self) -> str:
        return "google_fit"

    def setup_database(self) -> bool:
        """Set up the database tables."""
        sql_files = [
            "aggregator/plugins/google_fit/sql/steps.sql",
            "aggregator/plugins/google_fit/sql/heart.sql",
            "aggregator/plugins/google_fit/sql/general.sql"
        ]
        try:
            for sql_file in sql_files:
                execute_sql_file(sql_file)
            logger.info("Google Fit database schema initialized")
            return True
        except Exception as e:
            logger.error(f"Error setting up Google Fit database: {e}", exc_info=True)
            return False

    def write_to_database(self, data) -> tuple[int, int]:
        """Write data to database."""
        total_inserted, total_duplicates = 0, 0
        try:
            if isinstance(data, dict):
                for table, df in data.items():
                    if df is None or df.empty:
                        continue
                    # Only process steps, heart, and general data
                    if table in ["google_fit_steps", "google_fit_heart", "google_fit_general"]:
                        ins, dup = write_samsung_dataframe_to_mysql_batch(df, table)
                        logger.info(f"Wrote {ins} new rows ({dup} duplicates) to {table}")
                        total_inserted += ins
                        total_duplicates += dup
                return total_inserted, total_duplicates
            else:
                logger.error("write_to_database called with non-dict payload for google_fit")
                return 0, 0
        except Exception as e:
            logger.error(f"Error writing google_fit data: {e}", exc_info=True)
            return total_inserted, total_duplicates

    def fetch_data(self) -> dict:
        """Fetch essential health data from Google Fit."""
        client_id = os.environ.get("GOOGLE_FIT_CLIENT_ID")
        client_secret = os.environ.get("GOOGLE_FIT_CLIENT_SECRET")
        if not client_id or not client_secret:
            logger.error("Missing Google Fit API credentials")
            return {}

        try:
            tokens = self._get_tokens(client_id, client_secret)
            if not tokens:
                return {}

            access_token = tokens["access_token"]

            # Get user ID
            headers = {"Authorization": f"Bearer {access_token}"}
            user_info_url = "https://www.googleapis.com/fitness/v1/users/me"
            try:
                user_response = requests.get(user_info_url, headers=headers)
                user_response.raise_for_status()
                user_id = user_response.json().get("id", "unknown_user")
            except Exception:
                user_id = "unknown_user"

            # Fetch data
            steps_df = self._fetch_steps_data(access_token, user_id)
            hr_df = self._fetch_heart_rate_data(access_token, user_id)
            general_df = self._fetch_general_data(access_token, user_id)

            # Mark full load as completed after successful fetch
            plugin_config = PluginConfig(self.name)
            if not plugin_config.is_full_load_completed():
                plugin_config.mark_full_load_completed()

            return {
                "google_fit_steps": steps_df,
                "google_fit_heart": hr_df,
                "google_fit_general": general_df,
            }

        except Exception as e:
            logger.error(f"Error in fetch_data: {e}", exc_info=True)
            return {}

    def _build_auth_url(self, client_id: str) -> str:
        """Build the OAuth authorization URL."""
        params = {
            "client_id": client_id,
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "scope": " ".join(SCOPES),
            "access_type": "offline",
            "prompt": "consent",
        }
        return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)

    def _exchange_code_for_tokens(self, code: str, client_id: str, client_secret: str) -> dict:
        """Exchange authorization code for tokens."""
        data = {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
        }
        r = requests.post(TOKEN_URL, data=data)
        r.raise_for_status()
        tokens = r.json()
        # Ensure the directory exists before saving the tokens
        os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            json.dump(tokens, f, indent=2)
        return tokens

    def _refresh_access_token(self, refresh_token: str, client_id: str, client_secret: str) -> dict:
        """Refresh the access token."""
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        r = requests.post(TOKEN_URL, data=data)
        r.raise_for_status()
        tokens = r.json()
        tokens["refresh_token"] = refresh_token
        # Ensure the directory exists before saving the tokens
        os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            json.dump(tokens, f, indent=2)
        return tokens

    def _get_tokens(self, client_id: str, client_secret: str) -> dict:
        """Get valid tokens."""
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                tokens = json.load(f)
            if "refresh_token" in tokens:
                return self._refresh_access_token(tokens["refresh_token"], client_id, client_secret)

        url = self._build_auth_url(client_id)
        logger.info(f"Go to this URL and authorize access:\n{url}")
        code = input("Paste the 'code' from the URL: ").strip()
        return self._exchange_code_for_tokens(code, client_id, client_secret)

    def _chunked_time_ranges(self, start: datetime, end: datetime):
        """Split time range into chunks."""
        ranges = []
        current_start = start
        while current_start < end:
            current_end = min(current_start + timedelta(days=CHUNK_DAYS), end)
            ranges.append((int(current_start.timestamp() * 1000), int(current_end.timestamp() * 1000)))
            current_start = current_end
        return ranges

    def _fetch_data_for_source(self, access_token: str, data_source: str, bucket_millis: int, user_id: str) -> pd.DataFrame:
        """Fetch data for a specific source."""
        # Get data fetch range from plugin config
        plugin_config = PluginConfig(self.name)
        days_to_fetch = plugin_config.get_data_fetch_range_days()
        
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days_to_fetch)

        headers = {"Authorization": f"Bearer {access_token}"}
        all_records = []

        for start_ms, end_ms in self._chunked_time_ranges(start_dt, end_dt):
            body = {
                "aggregateBy": [{"dataSourceId": data_source}],
                "bucketByTime": {"durationMillis": bucket_millis},
                "startTimeMillis": start_ms,
                "endTimeMillis": end_ms
            }

            url = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
            r = requests.post(url, headers=headers, json=body)
            if r.status_code == 403:
                logger.warning(f"Permission denied for {data_source}. Skipping...")
                break
            elif r.status_code != 200:
                logger.error(f"Google Fit API error {r.status_code}: {r.text}")
                continue
            
            data = r.json()
            
            for bucket in data.get("bucket", []):
                for dataset in bucket.get("dataset", []):
                    for point in dataset.get("point", []):
                        start_ts = int(point["startTimeNanos"]) / 1_000_000_000
                        for value in point.get("value", []):
                            record = {
                                "id": str(uuid.uuid4()),
                                "user_id": user_id,
                                "timestamp": datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None),
                            }
                            if "intVal" in value:
                                record["value"] = value["intVal"]
                            elif "fpVal" in value:
                                record["value"] = round(value["fpVal"], 2)
                            all_records.append(record)
            
            # Small delay to avoid quota errors
            time.sleep(0.1)

        return pd.DataFrame(all_records)

    def _fetch_steps_data(self, access_token: str, user_id: str) -> pd.DataFrame:
        """Fetch steps data."""
        logger.info("Fetching steps data...")
        df = self._fetch_data_for_source(access_token, DATA_SOURCES["steps"], 86400000, user_id)
        
        if not df.empty:
            # Transform to steps format with only essential columns
            df = df.rename(columns={"value": "steps"})
            # Convert timestamp to date only (remove time component)
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
            # Group by user_id and date, sum the steps
            df = df.groupby(["user_id", "timestamp"]).agg({
                "steps": "sum",
                "id": "first"  # Keep one ID for the group
            }).reset_index()
            # Convert timestamp back to datetime for database storage
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            logger.info(f"Fetched {len(df)} steps records")
        else:
            logger.info("No steps data found")
            
        return df

    def _fetch_heart_rate_data(self, access_token: str, user_id: str) -> pd.DataFrame:
        """Fetch heart rate data."""
        logger.info("Fetching heart rate data...")
        df = self._fetch_data_for_source(access_token, DATA_SOURCES["heart_rate"], 3600000, user_id)
        
        if not df.empty:
            # Transform to heart rate format with only essential columns
            df = df.rename(columns={"value": "heart_rate"})
            # Remove duplicates by keeping the average heart rate per hour
            df["timestamp_hour"] = pd.to_datetime(df["timestamp"]).dt.floor("h")
            df = df.groupby(["user_id", "timestamp_hour"]).agg({
                "heart_rate": "mean",
                "id": "first"  # Keep one ID for the group
            }).reset_index()
            # Rename timestamp_hour back to timestamp
            df = df.rename(columns={"timestamp_hour": "timestamp"})
            # Convert heart_rate to rounded value
            df["heart_rate"] = df["heart_rate"].round(2)
            logger.info(f"Fetched {len(df)} heart rate records")
        else:
            logger.info("No heart rate data found")
            
        return df

    def _fetch_general_data(self, access_token: str, user_id: str) -> pd.DataFrame:
        """Fetch general health data (weight, height, body fat, etc.)."""
        logger.info("Fetching general health data...")
        
        # Data types to fetch
        general_data_types = {
            "weight": ("weight", "kg"),
            "height": ("height", "cm"), 
            "body_fat": ("body_fat_percentage", "%")
        }
        
        all_records = []
        
        for data_type_key, (data_type_name, unit) in general_data_types.items():
            logger.info(f"Fetching {data_type_key} data...")
            df = self._fetch_data_for_source(access_token, DATA_SOURCES[data_type_key], 86400000, user_id)
            
            if not df.empty:
                # Transform to general format
                df = df.rename(columns={"value": "value"})
                # Convert timestamp to date only
                df["date"] = pd.to_datetime(df["timestamp"]).dt.date
                # Add data type and unit information
                df["data_type"] = data_type_name
                df["unit"] = unit
                df["source"] = data_type_key
                # Group by user_id and date, take average value
                df = df.groupby(["user_id", "date"]).agg({
                    "value": "mean",
                    "id": "first",  # Keep one ID for the group
                    "data_type": "first",
                    "unit": "first",
                    "source": "first"
                }).reset_index()
                # Convert date back to datetime for database storage
                df["date"] = pd.to_datetime(df["date"])
                # Convert value to rounded value
                df["value"] = df["value"].round(2)
                
                all_records.append(df)
                logger.info(f"Fetched {len(df)} {data_type_key} records")
            else:
                logger.info(f"No {data_type_key} data found")
        
        # Combine all general data
        if all_records:
            combined_df = pd.concat(all_records, ignore_index=True)
            logger.info(f"Total general health records: {len(combined_df)}")
            return combined_df
        else:
            logger.info("No general health data found")
            return pd.DataFrame()