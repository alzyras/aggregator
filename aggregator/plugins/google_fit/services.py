import json
import logging
import time
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Tuple

import pandas as pd
import requests

from aggregator.core.apps import PluginService
from aggregator.infrastructure.filesystem import PluginState
from aggregator.settings import settings

from .repositories import GoogleFitRepository

logger = logging.getLogger(__name__)

PLUGIN_DIR = Path(__file__).parent
TOKEN_FILE = PLUGIN_DIR / "data" / "google_fit_tokens.json"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REDIRECT_URI = "http://localhost:8080/oauth2callback"

SCOPES = [
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.heart_rate.read",
    "https://www.googleapis.com/auth/fitness.body.read",
]

DATA_SOURCES = {
    "steps": "derived:com.google.step_count.delta:com.google.android.gms:merge_step_deltas",
    "heart_rate": "derived:com.google.heart_rate.bpm:com.google.android.gms:merge_heart_rate_bpm",
    "weight": "derived:com.google.weight:com.google.android.gms:merge_weight",
    "height": "derived:com.google.height:com.google.android.gms:merge_height",
    "body_fat": "derived:com.google.body.fat.percentage:com.google.android.gms:merged",
}

CHUNK_DAYS = 90


class GoogleFitService(PluginService):
    name = "google_fit"

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        self.state = PluginState(self.name)
        self.repository = GoogleFitRepository()

    def setup(self) -> bool:
        self.repository.ensure_schema()
        return True

    def write_data(self, payload) -> Tuple[int, int]:
        total_inserted, total_duplicates = 0, 0
        if not isinstance(payload, dict):
            logger.error("google_fit write_data called with non-dict payload")
            return total_inserted, total_duplicates

        for table, df in payload.items():
            ins, dup = self.repository.write_dataframe(df, table)
            total_inserted += ins
            total_duplicates += dup
        return total_inserted, total_duplicates

    def fetch_data(self) -> dict:
        creds = self.settings.google_fit
        client_id = creds.get("client_id")
        client_secret = creds.get("client_secret")
        if not client_id or not client_secret:
            logger.error("Missing Google Fit API credentials")
            return {}

        try:
            tokens = self._get_tokens(client_id, client_secret)
            if not tokens:
                return {}

            access_token = tokens["access_token"]

            headers = {"Authorization": f"Bearer {access_token}"}
            user_info_url = "https://www.googleapis.com/fitness/v1/users/me"
            try:
                user_response = requests.get(user_info_url, headers=headers)
                user_response.raise_for_status()
                user_id = user_response.json().get("id", "unknown_user")
            except Exception:
                user_id = "unknown_user"

            steps_df = self._fetch_steps_data(access_token, user_id)
            hr_df = self._fetch_heart_rate_data(access_token, user_id)
            general_df = self._fetch_general_data(access_token, user_id)

            if (
                (steps_df is not None and not steps_df.empty)
                or (hr_df is not None and not hr_df.empty)
                or (general_df is not None and not general_df.empty)
            ) and not self.state.is_full_load_completed():
                self.state.mark_full_load_completed()

            return {
                "google_fit_steps": steps_df,
                "google_fit_heart": hr_df,
                "google_fit_general": general_df,
            }

        except Exception as exc:
            logger.error("Error in google_fit.fetch_data: %s", exc, exc_info=True)
            return {}

    def _build_auth_url(self, client_id: str) -> str:
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
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
        return tokens

    def _refresh_access_token(self, refresh_token: str, client_id: str, client_secret: str) -> dict:
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
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
        return tokens

    def _get_tokens(self, client_id: str, client_secret: str) -> dict:
        if TOKEN_FILE.exists():
            tokens = json.loads(TOKEN_FILE.read_text())
            if "refresh_token" in tokens:
                return self._refresh_access_token(tokens["refresh_token"], client_id, client_secret)

        refresh_token = self.settings.google_fit.get("refresh_token")
        if refresh_token:
            return self._refresh_access_token(refresh_token, client_id, client_secret)

        url = self._build_auth_url(client_id)
        logger.error(
            "No refresh token available for Google Fit. Authorize at %s and place the resulting tokens in %s.",
            url,
            TOKEN_FILE,
        )
        return {}

    def _chunked_time_ranges(self, start: datetime, end: datetime):
        ranges = []
        current_start = start
        while current_start < end:
            current_end = min(current_start + timedelta(days=CHUNK_DAYS), end)
            ranges.append(
                (int(current_start.timestamp() * 1000), int(current_end.timestamp() * 1000))
            )
            current_start = current_end
        return ranges

    def _fetch_data_for_source(self, access_token: str, data_source: str, bucket_millis: int, user_id: str) -> pd.DataFrame:
        days_to_fetch = self.state.get_data_fetch_range_days()

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days_to_fetch)

        headers = {"Authorization": f"Bearer {access_token}"}
        all_records = []

        for start_ms, end_ms in self._chunked_time_ranges(start_dt, end_dt):
            body = {
                "aggregateBy": [{"dataSourceId": data_source}],
                "bucketByTime": {"durationMillis": bucket_millis},
                "startTimeMillis": start_ms,
                "endTimeMillis": end_ms,
            }

            url = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
            r = requests.post(url, headers=headers, json=body)
            if r.status_code == 403:
                logger.warning("Permission denied for %s. Skipping...", data_source)
                break
            if r.status_code != 200:
                logger.error("Google Fit API error %s: %s", r.status_code, r.text)
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

            time.sleep(0.1)

        return pd.DataFrame(all_records)

    def _fetch_steps_data(self, access_token: str, user_id: str) -> pd.DataFrame:
        df = self._fetch_data_for_source(access_token, DATA_SOURCES["steps"], 86400000, user_id)

        if not df.empty:
            df = df.rename(columns={"value": "steps"})
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
            df = (
                df.groupby(["user_id", "timestamp"])
                .agg({"steps": "sum", "id": "first"})
                .reset_index()
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    def _fetch_heart_rate_data(self, access_token: str, user_id: str) -> pd.DataFrame:
        df = self._fetch_data_for_source(access_token, DATA_SOURCES["heart_rate"], 3600000, user_id)

        if not df.empty:
            df = df.rename(columns={"value": "heart_rate"})
            df["timestamp_hour"] = pd.to_datetime(df["timestamp"]).dt.floor("h")
            df = (
                df.groupby(["user_id", "timestamp_hour"])
                .agg({"heart_rate": "mean", "id": "first"})
                .reset_index()
            )
            df = df.rename(columns={"timestamp_hour": "timestamp"})
            df["heart_rate"] = df["heart_rate"].round(2)
        return df

    def _fetch_general_data(self, access_token: str, user_id: str) -> pd.DataFrame:
        general_data_types = {
            "weight": ("weight", "kg"),
            "height": ("height", "cm"),
            "body_fat": ("body_fat_percentage", "%"),
        }

        all_records = []

        for data_type_key, (data_type_name, unit) in general_data_types.items():
            df = self._fetch_data_for_source(access_token, DATA_SOURCES[data_type_key], 86400000, user_id)

            if df.empty:
                continue

            df = df.rename(columns={"value": "value"})
            df["date"] = pd.to_datetime(df["timestamp"]).dt.date
            df["data_type"] = data_type_name
            df["unit"] = unit
            df["source"] = data_type_key
            df = (
                df.groupby(["user_id", "date"])
                .agg(
                    {
                        "value": "mean",
                        "id": "first",
                        "data_type": "first",
                        "unit": "first",
                        "source": "first",
                    }
                )
                .reset_index()
            )
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = df["value"].round(2)

            df = df.rename(columns={"date": "timestamp"})
            df["metadata"] = None
            all_records.append(df[
                ["id", "user_id", "data_type", "timestamp", "value", "unit", "metadata"]
            ])

        if all_records:
            combined_df = pd.concat(all_records, ignore_index=True)
            return combined_df
        return pd.DataFrame()
