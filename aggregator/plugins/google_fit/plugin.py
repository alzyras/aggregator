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

TOKEN_FILE = "google_fit_tokens.json"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REDIRECT_URI = "http://localhost:8080/oauth2callback"

SCOPES = [
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.heart_rate.read", 
    "https://www.googleapis.com/auth/fitness.body.read",
    "https://www.googleapis.com/auth/fitness.sleep.read",
    "https://www.googleapis.com/auth/fitness.location.read",
]

CHUNK_DAYS = 90  # Split large requests into 90-day chunks


class GoogleFitPlugin(PluginInterface):
    """Google Fit plugin implementation using Google Fit API."""

    @property
    def name(self) -> str:
        return "google_fit"

    # ---------------- REQUIRED INTERFACE METHODS ---------------- #

    def setup_database(self) -> bool:
        sql_files = [
            "aggregator/plugins/google_fit/sql/steps.sql",
            # Consolidate heart-related data into single table
            "aggregator/plugins/google_fit/sql/heart.sql"
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
        """Accepts either a single DataFrame and table, or a dict of {table: df}."""
        total_inserted, total_duplicates = 0, 0
        try:
            if isinstance(data, dict):
                for table, df in data.items():
                    if df is None or df.empty:
                        continue
                    # Only process steps and heart data
                    if table in ["google_fit_steps", "google_fit_heart"]:
                        ins, dup = write_samsung_dataframe_to_mysql_batch(df, table)
                        logger.info(f"Wrote {ins} new rows ({dup} duplicates) to {table}")
                        logger.debug(f"Insert count: {ins}, Duplicate count: {dup}")
                        total_inserted += ins
                        total_duplicates += dup
                return total_inserted, total_duplicates
            else:
                logger.error("write_to_database called with non-dict payload for google_fit")
                return 0, 0
        except Exception as e:
            logger.error(f"Error writing google_fit data: {e}", exc_info=True)
            return total_inserted, total_duplicates

    # ---------------- MAIN FETCH LOGIC ---------------- #

    def fetch_data(self) -> dict:
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
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning("User info endpoint not available, using default user ID")
                    user_id = "unknown_user"
                else:
                    logger.error(f"Error getting user ID: {e}")
                    user_id = "unknown_user"
            except Exception as e:
                logger.error(f"Error getting user ID: {e}")
                user_id = "unknown_user"

            # Build dataframes - only steps and heart rate
            steps_df = self._fetch_steps_data(access_token, user_id)
            hr_df = self._fetch_heart_rate_data(access_token, user_id)

            # Mark full load as completed after successful fetch
            plugin_config = PluginConfig(self.name)
            if not plugin_config.is_full_load_completed():
                plugin_config.mark_full_load_completed()

            return {
                "google_fit_steps": steps_df,
                "google_fit_heart": hr_df,
            }

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

    def _map_activity_type(self, activity_code: int) -> str:
        """Map Google Fit activity codes to readable names."""
        activity_names = {
            0: "Unknown",
            1: "Still",
            2: "Tilting",
            3: "Walking",
            4: "Running",
            5: "Biking",
            6: "Vehicle",
            7: "On Foot",
            8: "On Bicycle",
            9: "Walking",
            10: "Running",
            11: "Cycling",
            12: "Swimming",
            13: "Mountain Biking",
            14: "Other",
            15: "Aerobics",
            16: "Badminton",
            17: "Baseball",
            18: "Basketball",
            19: "Biathlon",
            20: "Handbiking",
            21: "Mountain Biking",
            22: "Road Biking",
            23: "Spinning",
            24: "Stationary Biking",
            25: "Utility Biking",
            26: "Boxing",
            27: "Calisthenics",
            28: "Circuit Training",
            29: "Cricket",
            30: "Dancing",
            31: "Elliptical",
            32: "Fencing",
            33: "Football",
            34: "Gardening",
            35: "Hiking",
            36: "Hockey",
            37: "Horseback Riding",
            38: "Housework",
            39: "Jumping Rope",
            40: "Kayaking",
            41: "Kettlebell Training",
            42: "Kickboxing",
            43: "Kitesurfing",
            44: "Martial Arts",
            45: "Meditation",
            46: "Mixed Martial Arts",
            47: "P90X Exercises",
            48: "Paragliding",
            49: "Pilates",
            50: "Polo",
            51: "Racquetball",
            52: "Rock Climbing",
            53: "Rowing",
            54: "Rowing Machine",
            55: "Rugby",
            56: "Jogging",
            57: "Running on Sand",
            58: "Running Treadmill",
            59: "Sailing",
            60: "Scuba Diving",
            61: "Skateboarding",
            62: "Skating",
            63: "Cross Skating",
            64: "Indoor Skating",
            65: "Inline Skating",
            66: "Skiing",
            67: "Back Country Skiing",
            68: "Cross Country Skiing",
            69: "Downhill Skiing",
            70: "Kite Skiing",
            71: "Roller Skiing",
            72: "Sledding",
            73: "Sleeping",
            74: "Light Sleep",
            75: "Deep Sleep",
            76: "REM Sleep",
            77: "Snowboarding",
            78: "Snowmobile",
            79: "Snowshoeing",
            80: "Squash",
            81: "Stair Climbing",
            82: "Stair Climbing Machine",
            83: "Stand Up Paddleboarding",
            84: "Strength Training",
            85: "Surfing",
            86: "Swimming Open Water",
            87: "Swimming Pool",
            88: "Table Tennis",
            89: "Team Sports",
            90: "Tennis",
            91: "Treadmill",
            92: "Volleyball",
            93: "Volleyball Beach",
            94: "Volleyball Indoor",
            95: "Wakeboarding",
            96: "Walking Nordic",
            97: "Walking Treadmill",
            98: "Waterpolo",
            99: "Weightlifting",
            100: "Wheelchair",
            101: "Windsurfing",
            102: "Yoga",
            103: "Zumba",
            104: "Diving",
            105: "Ergometer",
            106: "Ice Skate",
            107: "Indoor Rowing",
            108: "Jump Rope",
            109: "Lat Pulldown",
            110: "Leg Press",
            111: "Rowing Machine",
            112: "Shoulder Press",
            113: "Triceps Extension",
            114: "Weight Machine",
            115: "Barbell Curl",
            116: "Bench Press",
            117: "Bulgarian Squat",
            118: "Cable Crossover",
            119: "Cable Crunch",
            120: "Deadlift",
            121: "Decline Bench Press",
            122: "Dumbbell Curl",
            123: "Dumbbell Fly",
            124: "Dumbbell Press",
            125: "Hammer Curl",
            126: "Hip Thrust",
            127: "Incline Bench Press",
            128: "Lateral Raise",
            129: "Lunge",
            130: "Plank",
            131: "Preacher Curl",
            132: "Pull Up",
            133: "Push Up",
            134: "Romanian Deadlift",
            135: "Russian Twist",
            136: "Seated Row",
            137: "Single Leg Deadlift",
            138: "Sit Up",
            139: "Skullcrusher",
            140: "Squat",
            141: "Standing Military Press",
            142: "Step Up",
            143: "Straight Leg Deadlift",
            144: "Sumo Deadlift",
            145: "Upright Row",
            146: "V Bar Pulldown",
            147: "Calf Raise",
            148: "Chest Dip",
            149: "Chin Up",
            150: "Close Grip Bench Press",
            151: "Close Grip Pulldown",
            152: "Concentration Curl",
            153: "Crossover",
            154: "Crunch",
            155: "Decline Sit Up",
            156: "Dumbbell Kickback",
            157: "Dumbbell Row",
            158: "Fly",
            159: "Front Raise",
            160: "High Knee",
            161: "Hip Abductor",
            162: "Hip Adductor",
            163: "Leg Extension",
            164: "Leg Raise",
            165: "Lying Triceps Extension",
            166: "Machine Fly",
            167: "One Arm Dumbbell Press",
            168: "One Arm Lat Pulldown",
            169: "One Arm Row",
            170: "Overhead Press",
            171: "Preacher Hammer Curl",
            172: "Rear Delt Fly",
            173: "Reverse Crunch",
            174: "Reverse Curl",
            175: "Reverse Fly",
            176: "Seated Calf Raise",
            177: "Seated Dumbbell Curl",
            178: "Seated Dumbbell Press",
            179: "Seated Leg Curl",
            180: "Seated Leg Press",
            181: "Side Bend",
            182: "Single Arm Cable Crossover",
            183: "Single Arm Cable Fly",
            184: "Single Arm Dumbbell Curl",
            185: "Single Arm Dumbbell Fly",
            186: "Single Arm Dumbbell Press",
            187: "Single Arm Preacher Curl",
            188: "Single Arm Rear Delt Fly",
            189: "Single Arm Seated Dumbbell Curl",
            190: "Single Arm Seated Dumbbell Press",
            191: "Single Arm Standing Dumbbell Curl",
            192: "Single Arm Standing Dumbbell Press",
            193: "Single Leg Glute Bridge",
            194: "Single Leg Hip Thrust",
            195: "Single Leg Romanian Deadlift",
            196: "Single Leg Squat",
            197: "Standing Barbell Curl",
            198: "Standing Calf Raise",
            199: "Standing Dumbbell Curl",
            200: "Standing Dumbbell Press",
            201: "Standing Leg Curl",
            202: "Standing Military Press",
            203: "Standing Overhead Press",
            204: "Standing Triceps Extension",
            205: "Step Up",
            206: "Straight Arm Pulldown",
            207: "Superman",
            208: "T Bar Row",
            209: "Torso Rotation",
            210: "Triceps Dip",
            211: "Triceps Extension",
            212: "Upright Row",
            213: "V Bar Pulldown",
            214: "Weight Machine",
            215: "Wide Grip Bench Press",
            216: "Wide Grip Pulldown",
            217: "Wide Grip Pull Up",
            218: "Wide Grip Seated Row",
            219: "Wide Grip Upright Row",
            220: "Wide Stance Squat",
            221: "Zercher Squat",
            222: "Zottman Curl",
        }
        return activity_names.get(activity_code, f"Activity_{activity_code}")

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

    def _fetch_generic_data(self, access_token: str, data_source_or_type: str, is_source_id=True, name_map=None, bucket_millis: int = 86400000) -> pd.DataFrame:
        """Generic fetch function for steps, HR, sleep, workouts, weight."""
        headers = {"Authorization": f"Bearer {access_token}"}
        
        # Get data fetch range from plugin config
        plugin_config = PluginConfig(self.name)
        days_to_fetch = plugin_config.get_data_fetch_range_days()
        
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days_to_fetch)

        all_records = []

        for start_ms, end_ms in self._chunked_time_ranges(start_dt, end_dt):
            if is_source_id:
                aggregate_by = [{"dataSourceId": data_source_or_type}]
            else:
                aggregate_by = [{"dataTypeName": data_source_or_type}]

            body = {
                "aggregateBy": aggregate_by,
                "bucketByTime": {"durationMillis": bucket_millis},
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

    def _fetch_raw_dataset(self, access_token: str, data_source_id: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        headers = {"Authorization": f"Bearer {access_token}"}
        start_ns = int(start_dt.timestamp() * 1_000_000_000)
        end_ns = int(end_dt.timestamp() * 1_000_000_000)
        url = f"https://www.googleapis.com/fitness/v1/users/me/dataSources/{urllib.parse.quote(data_source_id, safe='')}/datasets/{start_ns}-{end_ns}"
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            logger.error(f"Datasets API error {r.status_code}: {r.text}")
            return pd.DataFrame()
        data = r.json()
        rows = []
        for point in data.get("point", []):
            start_ts = int(point.get("startTimeNanos", 0)) / 1_000_000_000
            end_ts = int(point.get("endTimeNanos", 0)) / 1_000_000_000
            start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None)
            end_time = datetime.fromtimestamp(end_ts, tz=timezone.utc).replace(tzinfo=None)
            vals = point.get("value", [])
            record = {"start_time": start_time, "end_time": end_time}
            if vals:
                v = vals[0]
                if "fpVal" in v:
                    record["value"] = float(v["fpVal"])  # e.g., kcal or meters
                elif "intVal" in v:
                    record["int_value"] = int(v["intVal"])  # e.g., activity code
            rows.append(record)
        return pd.DataFrame(rows)

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
            # Aggregate steps by day (group by date and sum steps)
            df["date"] = df["start_time"].dt.date
            daily_steps = df.groupby("date").agg({
                "steps": "sum",
                "start_time": "min",  # Use earliest timestamp of the day
            }).reset_index()
            
            # Create proper DataFrame with one row per day
            result_df = pd.DataFrame()
            result_df["id"] = [str(uuid.uuid4()) for _ in range(len(daily_steps))]
            result_df["user_id"] = user_id
            # For steps, use only the date part (set time to 00:00:00)
            result_df["timestamp"] = pd.to_datetime(daily_steps["date"])  # date-only, time 00:00:00 by default
            result_df["steps"] = daily_steps["steps"]
            result_df["distance"] = 0.0  # Would need separate data source
            result_df["calories"] = 0.0    # Would need separate data source
            result_df["speed"] = 0.0      # Would need separate data source
            result_df["heart_rate"] = None # Would need separate data source
            
            # Ensure dedupe by user+date
            result_df = result_df.sort_values("timestamp").drop_duplicates(subset=["user_id", "timestamp"], keep="last")
            return result_df
            
        return df

    def _fetch_heart_rate_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Fetch heart rate data
        hr_df = self._fetch_generic_data(
            access_token,
            "com.google.heart_rate.bpm",
            is_source_id=False,
            name_map=lambda v: {"heart_rate": v} if v is not None else {},
            bucket_millis=3600000  # 1 hour
        )
        
        if not hr_df.empty:
            # start_time will already be bucketed to 1-hour windows by aggregate API
            hr_df["rounded_hour"] = hr_df["start_time"].dt.floor("h")

            # Group by hour and calculate statistics across multiple samples in same hour
            hourly_stats = hr_df.groupby(["rounded_hour"]).agg({
                "heart_rate": ["mean", "min", "max", "count"],
                "start_time": "first"
            }).reset_index()
            
            # Flatten column names
            hourly_stats.columns = ["rounded_hour", "avg_hr", "min_hr", "max_hr", "count", "sample_time"]
            
            # Create proper DataFrame with clean timestamps (HH:00:00)
            result_df = pd.DataFrame()
            result_df["id"] = [str(uuid.uuid4()) for _ in range(len(hourly_stats))]
            result_df["user_id"] = user_id
            result_df["timestamp"] = hourly_stats["rounded_hour"]
            result_df["heart_rate"] = hourly_stats["avg_hr"].round(2)
            result_df["heart_rate_zone"] = None  # Would calculate based on user's max HR
            result_df["measurement_type"] = "bpm"
            result_df["context"] = None
            
            return result_df
            
        return pd.DataFrame()

    def _fetch_sleep_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Not implemented as per requirements
        return pd.DataFrame()

    def _fetch_workout_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Not implemented as per requirements
        return pd.DataFrame()


    def _fetch_general_health_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Not implemented as per requirements
        return pd.DataFrame()