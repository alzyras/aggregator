import os
import json
import logging
import requests
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta, timezone
import uuid
import time

from uv_app.plugin_interface import PluginInterface
from uv_app.plugins.samsung_health.df_to_mysql import (
    write_samsung_dataframe_to_mysql_batch,
    execute_sql_file,
)

logger = logging.getLogger(__name__)

TOKEN_FILE = "google_fit_tokens.json"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REDIRECT_URI = "http://localhost:8080/oauth2callback"

SCOPES = [
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.heart_rate.read",
    "https://www.googleapis.com/auth/fitness.body.read",
    "https://www.googleapis.com/auth/fitness.sleep.read",
]

CHUNK_DAYS = 90  # Split large requests into 90-day chunks


class SamsungHealthPlugin(PluginInterface):
    """Samsung Health plugin implementation using Google Fit API."""

    @property
    def name(self) -> str:
        return "samsung_health"

    # ---------------- REQUIRED INTERFACE METHODS ---------------- #

    def setup_database(self) -> None:
        sql_files = [
            "uv_app/plugins/samsung_health/sql/steps.sql",
            "uv_app/plugins/samsung_health/sql/heart_rate.sql",
            "uv_app/plugins/samsung_health/sql/sleep.sql",
            "uv_app/plugins/samsung_health/sql/workouts.sql",
            "uv_app/plugins/samsung_health/sql/general.sql"
        ]
        try:
            for sql_file in sql_files:
                execute_sql_file(sql_file)
            logger.info("Samsung Health database schema initialized")
        except Exception as e:
            logger.error(f"Error setting up Samsung Health database: {e}", exc_info=True)

    def write_to_database(self, df: pd.DataFrame, table: str) -> tuple[int, int]:
        if df.empty:
            return (0, 0)
        try:
            inserted, duplicates = write_samsung_dataframe_to_mysql_batch(df, table)
            logger.info(f"Wrote {inserted} new rows ({duplicates} duplicates) to {table}")
            print(inserted, duplicates)
            return inserted, duplicates
        except Exception as e:
            logger.error(f"Error writing data to {table}: {e}", exc_info=True)
            return (0, 0)

    # ---------------- MAIN FETCH LOGIC ---------------- #

    def fetch_data(self) -> pd.DataFrame:
        client_id = os.environ.get("GOOGLE_FIT_CLIENT_ID")
        client_secret = os.environ.get("GOOGLE_FIT_CLIENT_SECRET")
        if not client_id or not client_secret:
            logger.error("Missing Google Fit API credentials")
            return pd.DataFrame()

        try:
            tokens = self._get_tokens(client_id, client_secret)
            if not tokens:
                return pd.DataFrame()

            access_token = tokens["access_token"]

            # Get user ID
            headers = {"Authorization": f"Bearer {access_token}"}
            user_info_url = "https://www.googleapis.com/fitness/v1/users/me"
            try:
                user_response = requests.get(user_info_url, headers=headers)
                user_response.raise_for_status()
                user_id = user_response.json().get("id", "unknown_user")
                logger.info(f"Authenticated user ID: {user_id}")
            except Exception as e:
                logger.error(f"Error getting user ID: {e}")
                user_id = "unknown_user"

            total_inserted, total_duplicates = 0, 0

            # Steps
            steps_df = self._fetch_steps_data(access_token, user_id)
            if not steps_df.empty:
                logger.info(f"Writing {len(steps_df)} steps records to database")
                ins, dup = self.write_to_database(steps_df, "samsung_health_steps")
                total_inserted += ins
                total_duplicates += dup
            else:
                logger.info("No steps data to write")

            # Heart rate
            hr_df = self._fetch_heart_rate_data(access_token, user_id)
            if not hr_df.empty:
                logger.info(f"Writing {len(hr_df)} heart rate records to database")
                ins, dup = self.write_to_database(hr_df, "samsung_health_heart_rate")
                total_inserted += ins
                total_duplicates += dup
            else:
                logger.info("No heart rate data to write")

            # Sleep
            sleep_df = self._fetch_sleep_data(access_token, user_id)
            if not sleep_df.empty:
                logger.info(f"Writing {len(sleep_df)} sleep records to database")
                ins, dup = self.write_to_database(sleep_df, "samsung_health_sleep")
                total_inserted += ins
                total_duplicates += dup
            else:
                logger.info("No sleep data to write")

            # Workouts
            wo_df = self._fetch_workout_data(access_token, user_id)
            if not wo_df.empty:
                logger.info(f"Writing {len(wo_df)} workout records to database")
                ins, dup = self.write_to_database(wo_df, "samsung_health_workouts")
                total_inserted += ins
                total_duplicates += dup
            else:
                logger.info("No workout data to write")

            # General health
            gen_df = self._fetch_general_health_data(access_token, user_id)
            if not gen_df.empty:
                logger.info(f"Writing {len(gen_df)} general health records to database")
                ins, dup = self.write_to_database(gen_df, "samsung_health_general")
                total_inserted += ins
                total_duplicates += dup
            else:
                logger.info("No general health data to write")

            return pd.DataFrame(
                [{
                    "total_inserted": total_inserted,
                    "total_duplicates": total_duplicates,
                    "timestamp": datetime.now(),
                }]
            )

        except Exception as e:
            logger.error(f"Error in fetch_data: {e}", exc_info=True)
            return pd.DataFrame()

    # ---------------- TOKEN HANDLING ---------------- #

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
        with open(TOKEN_FILE, "w") as f:
            json.dump(tokens, f, indent=2)
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
        with open(TOKEN_FILE, "w") as f:
            json.dump(tokens, f, indent=2)
        return tokens

    def _get_tokens(self, client_id: str, client_secret: str) -> dict:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                tokens = json.load(f)
            if "refresh_token" in tokens:
                return self._refresh_access_token(tokens["refresh_token"], client_id, client_secret)

        url = self._build_auth_url(client_id)
        logger.info(f"Go to this URL and authorize access:\n{url}")
        code = input("Paste the 'code' from the URL: ").strip()
        return self._exchange_code_for_tokens(code, client_id, client_secret)

    # ---------------- HELPER: SPLIT DATE RANGE ---------------- #

    def _chunked_time_ranges(self, start: datetime, end: datetime, chunk_days=CHUNK_DAYS):
        ranges = []
        current_start = start
        while current_start < end:
            current_end = min(current_start + timedelta(days=chunk_days), end)
            ranges.append((int(current_start.timestamp() * 1000), int(current_end.timestamp() * 1000)))
            current_start = current_end
        return ranges

    # ---------------- DATA FETCH HELPERS ---------------- #

    def _fetch_generic_data(self, access_token: str, data_source_or_type: str, is_source_id=True, name_map=None) -> pd.DataFrame:
        """Generic fetch function for steps, HR, sleep, workouts, weight."""
        headers = {"Authorization": f"Bearer {access_token}"}
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=548)  # 1.5 years

        all_records = []

        for start_ms, end_ms in self._chunked_time_ranges(start_dt, end_dt):
            if is_source_id:
                aggregate_by = [{"dataSourceId": data_source_or_type}]
            else:
                aggregate_by = [{"dataTypeName": data_source_or_type}]

            body = {
                "aggregateBy": aggregate_by,
                "bucketByTime": {"durationMillis": 86400000},
                "startTimeMillis": start_ms,
                "endTimeMillis": end_ms
            }

            url = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
            r = requests.post(url, headers=headers, json=body)
            if r.status_code == 403:
                logger.warning(f"Permission denied for data source {data_source_or_type}. Skipping...")
                continue
            elif r.status_code != 200:
                logger.error(f"Google Fit API error {r.status_code}: {r.text}")
                continue
            data = r.json()

            for bucket in data.get("bucket", []):
                for dataset in bucket.get("dataset", []):
                    for point in dataset.get("point", []):
                        start_ts = int(point["startTimeNanos"]) / 1_000_000_000
                        end_ts = int(point["endTimeNanos"]) / 1_000_000_000
                        for value in point.get("value", []):
                            record = {"id": str(uuid.uuid4())}
                            if "intVal" in value:
                                record["value"] = value["intVal"]
                            if "fpVal" in value:
                                record["value"] = value["fpVal"]
                            # Convert timestamps to standard format (naive datetime in UTC)
                            # Use 'timestamp' to match database schema (use start time as the main timestamp)
                            record["timestamp"] = datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None)
                            # Also keep start_time and end_time for compatibility with the plugin's logic
                            record["start_time"] = datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None)
                            record["end_time"] = datetime.fromtimestamp(end_ts, tz=timezone.utc).replace(tzinfo=None)
                            if name_map:
                                mapped_values = name_map(record.get("value"))
                                record.update(mapped_values)
                                # Remove the generic "value" column if we have specific columns
                                if mapped_values:  # If name_map returned something
                                    record.pop("value", None)  # Remove value column if it exists
                            all_records.append(record)
            time.sleep(0.2)  # small delay to avoid quota errors

        return pd.DataFrame(all_records)

    def _fetch_steps_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Fetch data with name mapping
        df = self._fetch_generic_data(
            access_token,
            "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps",
            is_source_id=True,
            name_map=lambda v: {"steps": v} if v is not None else {}
        )
        
        # Ensure we have the correct column structure for the steps table
        if not df.empty:
            # Rename start_time to timestamp to match database schema
            if "start_time" in df.columns and "timestamp" not in df.columns:
                df.rename(columns={"start_time": "timestamp"}, inplace=True)
            
            # Add missing columns that are expected by the database
            if "user_id" not in df.columns:
                df["user_id"] = user_id
            if "distance" not in df.columns:
                df["distance"] = 0.0
            if "calories" not in df.columns:
                df["calories"] = 0.0
            if "speed" not in df.columns:
                df["speed"] = 0.0
            if "heart_rate" not in df.columns:
                df["heart_rate"] = None
                
            # Select only the columns that exist in the database table
            db_columns = ["id", "user_id", "timestamp", "steps", "distance", "calories", "speed", "heart_rate"]
            # Only select columns that actually exist in the DataFrame
            existing_columns = [col for col in db_columns if col in df.columns]
            df = df[existing_columns]
        return df

    def _fetch_heart_rate_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Fetch data with name mapping
        df = self._fetch_generic_data(
            access_token,
            "com.google.heart_rate.bpm",
            is_source_id=False,
            name_map=lambda v: {"heart_rate": v} if v is not None else {}
        )
        
        # Ensure we have the correct column structure for the heart rate table
        if not df.empty:
            # Rename start_time to timestamp to match database schema
            if "start_time" in df.columns and "timestamp" not in df.columns:
                df.rename(columns={"start_time": "timestamp"}, inplace=True)
            
            # Add missing columns that are expected by the database
            if "user_id" not in df.columns:
                df["user_id"] = user_id
            if "heart_rate_zone" not in df.columns:
                df["heart_rate_zone"] = None
            if "measurement_type" not in df.columns:
                df["measurement_type"] = "bpm"
                
            # Select only the columns that exist in the database table
            db_columns = ["id", "user_id", "timestamp", "heart_rate", "heart_rate_zone", "measurement_type"]
            # Only select columns that actually exist in the DataFrame
            existing_columns = [col for col in db_columns if col in df.columns]
            df = df[existing_columns]
        return df

    def _fetch_sleep_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        try:
            # Fetch data with name mapping
            df = self._fetch_generic_data(
                access_token,
                "com.google.sleep.segment",
                is_source_id=False,
                name_map=lambda v: {"sleep_type": v} if v is not None else {}
            )
            
            # Ensure we have the correct column structure for the sleep table
            if not df.empty:
                # Add missing columns that are expected by the database
                if "user_id" not in df.columns:
                    df["user_id"] = user_id
                if "duration_minutes" not in df.columns:
                    df["duration_minutes"] = 0.0
                if "sleep_score" not in df.columns:
                    df["sleep_score"] = None
                if "deep_sleep_minutes" not in df.columns:
                    df["deep_sleep_minutes"] = 0.0
                if "light_sleep_minutes" not in df.columns:
                    df["light_sleep_minutes"] = 0.0
                if "rem_sleep_minutes" not in df.columns:
                    df["rem_sleep_minutes"] = 0.0
                if "awake_minutes" not in df.columns:
                    df["awake_minutes"] = 0.0
                if "sleep_efficiency" not in df.columns:
                    df["sleep_efficiency"] = None
                if "bed_time" not in df.columns:
                    df["bed_time"] = None
                if "wake_up_time" not in df.columns:
                    df["wake_up_time"] = None
                    
                # Select only the columns that exist in the database table
                db_columns = [
                    "id", "user_id", "start_time", "end_time", "duration_minutes", "sleep_score",
                    "deep_sleep_minutes", "light_sleep_minutes", "rem_sleep_minutes", "awake_minutes",
                    "sleep_efficiency", "bed_time", "wake_up_time"
                ]
                # Only select columns that actually exist in the DataFrame
                existing_columns = [col for col in db_columns if col in df.columns]
                df = df[existing_columns]
                    
            return df
        except Exception as e:
            logger.error(f"Error fetching sleep data: {e}")
            # Return empty DataFrame if sleep data can't be fetched
            return pd.DataFrame()

    def _fetch_workout_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Fetch data with name mapping
        df = self._fetch_generic_data(
            access_token,
            "com.google.activity.segment",
            is_source_id=False,
            name_map=lambda v: {"activity_type": v} if v is not None else {}
        )
        
        # Ensure we have the correct column structure for the workouts table
        if not df.empty:
            # Add missing columns that are expected by the database
            if "user_id" not in df.columns:
                df["user_id"] = user_id
            if "duration_minutes" not in df.columns:
                df["duration_minutes"] = 0.0
            if "workout_type" not in df.columns:
                df["workout_type"] = None
            if "calories_burned" not in df.columns:
                df["calories_burned"] = 0.0
            if "distance" not in df.columns:
                df["distance"] = 0.0
            if "average_heart_rate" not in df.columns:
                df["average_heart_rate"] = None
            if "max_heart_rate" not in df.columns:
                df["max_heart_rate"] = None
            if "min_heart_rate" not in df.columns:
                df["min_heart_rate"] = None
            if "average_speed" not in df.columns:
                df["average_speed"] = 0.0
            if "max_speed" not in df.columns:
                df["max_speed"] = 0.0
            if "elevation_gain" not in df.columns:
                df["elevation_gain"] = 0.0
            if "elevation_loss" not in df.columns:
                df["elevation_loss"] = 0.0
            if "steps" not in df.columns:
                df["steps"] = 0
            if "strokes" not in df.columns:
                df["strokes"] = 0
            if "laps" not in df.columns:
                df["laps"] = 0
            if "notes" not in df.columns:
                df["notes"] = None
                
            # Select only the columns that exist in the database table
            db_columns = [
                "id", "user_id", "start_time", "end_time", "duration_minutes", "workout_type",
                "calories_burned", "distance", "average_heart_rate", "max_heart_rate", 
                "min_heart_rate", "average_speed", "max_speed", "elevation_gain", 
                "elevation_loss", "steps", "strokes", "laps", "notes"
            ]
            # Only select columns that actually exist in the DataFrame
            existing_columns = [col for col in db_columns if col in df.columns]
            df = df[existing_columns]
                
        return df

    def _fetch_general_health_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Fetch data with name mapping
        df = self._fetch_generic_data(
            access_token,
            "com.google.weight",
            is_source_id=False,
            name_map=lambda v: {"weight_kg": v} if v is not None else {}
        )
        
        # Ensure we have the correct column structure for the general health table
        if not df.empty:
            # Rename start_time to timestamp to match database schema
            if "start_time" in df.columns and "timestamp" not in df.columns:
                df.rename(columns={"start_time": "timestamp"}, inplace=True)
            # Add missing columns
            if "user_id" not in df.columns:
                df["user_id"] = user_id
            # Add data_type column
            df["data_type"] = "weight"
            # Rename weight_kg to value to match database schema
            if "weight_kg" in df.columns:
                df.rename(columns={"weight_kg": "value"}, inplace=True)
                df["unit"] = "kg"
            if "metadata" not in df.columns:
                df["metadata"] = None
                
            # Select only the columns that exist in the database table
            db_columns = ["id", "user_id", "data_type", "timestamp", "value", "unit", "metadata"]
            # Only select columns that actually exist in the DataFrame
            existing_columns = [col for col in db_columns if col in df.columns]
            df = df[existing_columns]
                
        return df
