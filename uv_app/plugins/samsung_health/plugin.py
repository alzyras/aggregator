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
    "https://www.googleapis.com/auth/fitness.location.read",
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
            # Consolidate heart-related data into single table
            "uv_app/plugins/samsung_health/sql/sleep.sql",
            "uv_app/plugins/samsung_health/sql/workouts.sql",
            "uv_app/plugins/samsung_health/sql/general.sql",
            "uv_app/plugins/samsung_health/sql/heart.sql"
        ]
        try:
            for sql_file in sql_files:
                execute_sql_file(sql_file)
            logger.info("Samsung Health database schema initialized")
        except Exception as e:
            logger.error(f"Error setting up Samsung Health database: {e}", exc_info=True)

    def write_to_database(self, data) -> tuple[int, int]:
        """Accepts either a single DataFrame and table, or a dict of {table: df}."""
        total_inserted, total_duplicates = 0, 0
        try:
            if isinstance(data, dict):
                for table, df in data.items():
                    if df is None or df.empty:
                        continue
                    ins, dup = write_samsung_dataframe_to_mysql_batch(df, table)
                    logger.info(f"Wrote {ins} new rows ({dup} duplicates) to {table}")
                    print(ins, dup)
                    total_inserted += ins
                    total_duplicates += dup
                return total_inserted, total_duplicates
            else:
                logger.error("write_to_database called with non-dict payload for samsung_health")
                return 0, 0
        except Exception as e:
            logger.error(f"Error writing samsung_health data: {e}", exc_info=True)
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
            except Exception as e:
                logger.error(f"Error getting user ID: {e}")
                user_id = "unknown_user"

            # Build dataframes
            steps_df = self._fetch_steps_data(access_token, user_id)
            hr_df = self._fetch_heart_rate_data(access_token, user_id)
            sleep_df = self._fetch_sleep_data(access_token, user_id)
            wo_df = self._fetch_workout_data(access_token, user_id)
            gen_df = self._fetch_general_health_data(access_token, user_id)

            return {
                "samsung_health_steps": steps_df,
                "samsung_health_heart": hr_df,
                "samsung_health_sleep": sleep_df,
                "samsung_health_workouts": wo_df,
                "samsung_health_general": gen_df,
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
        try:
            # Try different sleep data sources
            sleep_data_sources = [
                "com.google.sleep.segment",
                "com.google.sleep.session",
                "derived:com.google.sleep.segment:com.google.android.gms:sleep_from_device"
            ]
            
            all_sleep_data = []
            
            for data_source in sleep_data_sources:
                try:
                    # Fetch data with name mapping
                    df = self._fetch_generic_data(
                        access_token,
                        data_source,
                        is_source_id=False,
                        name_map=lambda v: {"sleep_type": v} if v is not None else {}
                    )
                    
                    if not df.empty:
                        all_sleep_data.append(df)
                        logger.info(f"Successfully fetched sleep data from {data_source}")
                        break  # If we get data, use it and stop trying other sources
                except Exception as e:
                    logger.warning(f"Could not fetch sleep data from {data_source}: {e}")
                    continue
            
            # Combine all sleep data
            if all_sleep_data:
                df = pd.concat(all_sleep_data, ignore_index=True)
            else:
                df = pd.DataFrame()
            
            # Ensure we have the correct column structure for the sleep table
            if not df.empty:
                # Calculate sleep duration in minutes
                if "start_time" in df.columns and "end_time" in df.columns:
                    df["duration_minutes"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60
                
                # Add missing columns that are expected by the database
                if "user_id" not in df.columns:
                    df["user_id"] = user_id
                if "duration_minutes" not in df.columns:
                    df["duration_minutes"] = 0.0
                if "sleep_score" not in df.columns:
                    df["sleep_score"] = None
                if "deep_sleep_minutes" not in df.columns:
                    # Try to infer deep sleep from sleep_type
                    if "sleep_type" in df.columns:
                        df["deep_sleep_minutes"] = df.apply(
                            lambda row: row["duration_minutes"] if row["sleep_type"] == 3 else 0.0, 
                            axis=1
                        )
                    else:
                        df["deep_sleep_minutes"] = 0.0
                if "light_sleep_minutes" not in df.columns:
                    # Try to infer light sleep from sleep_type
                    if "sleep_type" in df.columns:
                        df["light_sleep_minutes"] = df.apply(
                            lambda row: row["duration_minutes"] if row["sleep_type"] == 2 else 0.0, 
                            axis=1
                        )
                    else:
                        df["light_sleep_minutes"] = 0.0
                if "rem_sleep_minutes" not in df.columns:
                    # Try to infer REM sleep from sleep_type
                    if "sleep_type" in df.columns:
                        df["rem_sleep_minutes"] = df.apply(
                            lambda row: row["duration_minutes"] if row["sleep_type"] == 4 else 0.0, 
                            axis=1
                        )
                    else:
                        df["rem_sleep_minutes"] = 0.0
                if "awake_minutes" not in df.columns:
                    # Try to infer awake time from sleep_type
                    if "sleep_type" in df.columns:
                        df["awake_minutes"] = df.apply(
                            lambda row: row["duration_minutes"] if row["sleep_type"] == 1 else 0.0, 
                            axis=1
                        )
                    else:
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
        headers = {"Authorization": f"Bearer {access_token}"}
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=548)

        allowed_map = {
            "Walking": "walk",
            "Walking Nordic": "walk",
            "Walking Treadmill": "walk",
            "Hiking": "walk",
            "Running": "run",
            "Jogging": "run",
            "Running on Sand": "run",
            "Treadmill": "run",
            "Cycling": "cycle",
            "Road Biking": "cycle",
            "Stationary Biking": "cycle",
            "Mountain Biking": "cycle",
            "On Bicycle": "cycle",
        }

        # 1) Pull manual sessions first (tend to be user-initiated workouts)
        session_rows = []
        for range_start, range_end in self._chunked_time_ranges(start_dt, end_dt):
            start_rfc3339 = datetime.fromtimestamp(range_start/1000, tz=timezone.utc).isoformat()
            end_rfc3339 = datetime.fromtimestamp(range_end/1000, tz=timezone.utc).isoformat()
            url = f"https://www.googleapis.com/fitness/v1/users/me/sessions?startTime={start_rfc3339}&endTime={end_rfc3339}"
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                logger.error(f"Sessions API error {resp.status_code}: {resp.text}")
                continue
            sessions = resp.json().get("session", [])
            for s in sessions:
                activity_code = s.get("activityType")
                start_ts = int(s.get("startTimeMillis", 0))/1000
                end_ts = int(s.get("endTimeMillis", 0))/1000
                if not start_ts or not end_ts:
                    continue
                start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None)
                end_time = datetime.fromtimestamp(end_ts, tz=timezone.utc).replace(tzinfo=None)
                duration_min = (end_time - start_time).total_seconds() / 60
                if duration_min < 1 or duration_min > 480:
                    continue
                workout_type_name = self._map_activity_type(activity_code)
                normalized = allowed_map.get(workout_type_name)
                if not normalized:
                    # skip sessions that aren't our target types to reduce 'other'
                    continue
                session_rows.append({
                    "id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_minutes": round(duration_min, 2),
                    "workout_type": normalized,  # manual by default
                })
            time.sleep(0.15)

        # 2) Fall back to activity segments for auto-detected movement outside sessions
        auto_rows = []
        for start_ms, end_ms in self._chunked_time_ranges(start_dt, end_dt):
            body = {
                "aggregateBy": [{"dataTypeName": "com.google.activity.segment"}],
                "bucketByTime": {"durationMillis": 86400000},
                "startTimeMillis": start_ms,
                "endTimeMillis": end_ms
            }
            url = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
            r = requests.post(url, headers=headers, json=body)
            if r.status_code != 200:
                logger.error(f"Google Fit API error {r.status_code}: {r.text}")
                continue
            data = r.json()
            for bucket in data.get("bucket", []):
                for dataset in bucket.get("dataset", []):
                    for point in dataset.get("point", []):
                        start_ts = int(point["startTimeNanos"]) / 1_000_000_000
                        end_ts = int(point["endTimeNanos"]) / 1_000_000_000
                        for value in point.get("value", []):
                            if "intVal" not in value:
                                continue
                            workout_type_name = self._map_activity_type(value["intVal"])
                            normalized = allowed_map.get(workout_type_name)
                            if not normalized:
                                # skip non-target activities
                                continue
                            start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc).replace(tzinfo=None)
                            end_time = datetime.fromtimestamp(end_ts, tz=timezone.utc).replace(tzinfo=None)
                            duration_min = (end_time - start_time).total_seconds() / 60
                            if duration_min < 1 or duration_min > 480:
                                continue
                            auto_rows.append({
                                "id": str(uuid.uuid4()),
                                "user_id": user_id,
                                "start_time": start_time,
                                "end_time": end_time,
                                "duration_minutes": round(duration_min, 2),
                                "workout_type": f"{normalized}_auto",
                            })
            time.sleep(0.15)

        # Prefer sessions (manual). Only add auto segments that do not overlap existing sessions of same type
        manual_df = pd.DataFrame(session_rows)
        auto_df = pd.DataFrame(auto_rows)

        if not manual_df.empty and not auto_df.empty:
            non_overlapping_auto = []
            for i, row in auto_df.iterrows():
                mask = (
                    (manual_df["workout_type"] == row["workout_type"].replace("_auto", "")) &
                    (manual_df["start_time"] <= row["end_time"]) &
                    (manual_df["end_time"] >= row["start_time"])  # overlap
                )
                if not manual_df.loc[mask].empty:
                    continue
                non_overlapping_auto.append(row)
            auto_df = pd.DataFrame(non_overlapping_auto)

        df = pd.concat([manual_df, auto_df], ignore_index=True) if (not manual_df.empty or not auto_df.empty) else pd.DataFrame()

        if df.empty:
            return df

        # Enrich with calories (kcal) and distance (km) via aggregate API (hourly buckets to align better)
        try:
            calories = self._fetch_generic_data(access_token, "com.google.calories.expended", is_source_id=False, name_map=lambda v: {"cal": v}, bucket_millis=3600000)
            distance = self._fetch_generic_data(access_token, "com.google.distance.delta", is_source_id=False, name_map=lambda v: {"dist": v}, bucket_millis=3600000)
        except Exception:
            calories = pd.DataFrame()
            distance = pd.DataFrame()

        df["calories_burned"] = 0.0
        df["distance"] = 0.0

        if not calories.empty and {"start_time", "end_time", "cal"}.issubset(calories.columns):
            for i, row in df.iterrows():
                mask = (calories["start_time"] >= row["start_time"]) & (calories["end_time"] <= row["end_time"]) 
                cal_sum = calories.loc[mask, "cal"].sum()
                if cal_sum > 0:
                    df.at[i, "calories_burned"] = round(float(cal_sum), 2)

        if not distance.empty and {"start_time", "end_time", "dist"}.issubset(distance.columns):
            for i, row in df.iterrows():
                mask = (distance["start_time"] >= row["start_time"]) & (distance["end_time"] <= row["end_time"]) 
                dist_sum = distance.loc[mask, "dist"].sum()  # meters
                if dist_sum > 0:
                    df.at[i, "distance"] = round(float(dist_sum) / 1000.0, 3)  # km

        # Compute average speed where possible
        # average_speed in km/h
        df["average_speed"] = df.apply(lambda r: (r["distance"] / (r["duration_minutes"] / 60)) if r["distance"] > 0 and r["duration_minutes"] > 0 else 0.0, axis=1)
        df["max_speed"] = 0.0
        df["average_heart_rate"] = None
        df["max_heart_rate"] = None
        df["min_heart_rate"] = None
        df["elevation_gain"] = 0.0
        df["elevation_loss"] = 0.0
        df["steps"] = 0
        df["strokes"] = 0
        df["laps"] = 0
        df["notes"] = None

        # Heuristic: auto-detected activities
        # If duration is very short and type is walk/run/cycle with no calories/distance, mark as auto
        def detect_auto(row):
            if row["workout_type"] in {"walk", "run", "cycle"}:
                if (row["calories_burned"] == 0 or pd.isna(row["calories_burned"])) and row["distance"] == 0:
                    return f"{row['workout_type']}_auto"
            return row["workout_type"]

        df["workout_type"] = df.apply(detect_auto, axis=1)

        # Final logical dedupe
        df = df.sort_values("start_time").drop_duplicates(subset=["user_id", "start_time", "end_time", "workout_type"], keep="last")

        return df

    def _fetch_general_health_data(self, access_token: str, user_id: str = "unknown_user") -> pd.DataFrame:
        # Fetch height, weight, and body fat percentage data for general health
        health_data_types = [
            ("com.google.weight", "weight", "kg"),
            ("com.google.height", "height", "cm"),
            ("com.google.body.fat.percentage", "body_fat_percentage", "%"),
        ]
        
        all_health_data = []
        
        for data_type, friendly_name, unit in health_data_types:
            try:
                # Fetch data with name mapping
                df = self._fetch_generic_data(
                    access_token,
                    data_type,
                    is_source_id=False,
                    name_map=lambda v: {friendly_name: v} if v is not None else {}
                )
                
                if not df.empty:
                    # Prepare data for database
                    df["user_id"] = user_id
                    df["data_type"] = friendly_name
                    df["unit"] = unit
                    df["metadata"] = None
                    
                    # Rename start_time to timestamp to match database schema
                    if "start_time" in df.columns and "timestamp" not in df.columns:
                        df.rename(columns={"start_time": "timestamp"}, inplace=True)
                    
                    # Rename the value column to match database schema
                    if friendly_name in df.columns:
                        df.rename(columns={friendly_name: "value"}, inplace=True)
                    
                    # Select only the columns that exist in the database table
                    db_columns = ["id", "user_id", "data_type", "timestamp", "value", "unit", "metadata"]
                    # Only select columns that actually exist in the DataFrame
                    existing_columns = [col for col in db_columns if col in df.columns]
                    df = df[existing_columns]
                    
                    all_health_data.append(df)
            except Exception as e:
                logger.warning(f"Could not fetch {friendly_name} data: {e}")
                continue
        
        # Combine all health data
        if all_health_data:
            combined_df = pd.concat(all_health_data, ignore_index=True)
            # Remove duplicates based on user_id, data_type, and date (keep most recent)
            if not combined_df.empty and "timestamp" in combined_df.columns:
                combined_df["date"] = combined_df["timestamp"].dt.date
                combined_df = combined_df.sort_values("timestamp").drop_duplicates(
                    subset=["user_id", "data_type", "date"], keep="last"
                ).drop("date", axis=1)
            return combined_df
        else:
            return pd.DataFrame()
