import os
import mysql.connector
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, DATETIME, DECIMAL, TEXT, JSON


MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_DB = os.environ.get("MYSQL_DB")
MYSQL_USER = os.environ.get("MYSQL_USER")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")


def execute_sql_file(filepath):
    """Executes SQL commands from a file."""
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
        )
        connection = engine.connect()

        with open(filepath) as sql_file:
            sql_commands = sql_file.read().split(";")  # Split by semicolon

        for command in sql_commands:
            command = command.strip()
            if command:  # Ignore empty commands
                connection.execute(text(command))  # wrap command in text()

        connection.commit()
        print(f"✅ SQL commands from {filepath} executed successfully.")

    except mysql.connector.Error as err:
        print(f"❌ Error executing SQL from {filepath}: {err}")
    except FileNotFoundError:
        print(f"❌ File not found: {filepath}")
    finally:
        if "connection" in locals() and connection:
            connection.close()


def write_samsung_dataframe_to_mysql_batch(df, table_name):
    """Writes a Samsung Health DataFrame to MySQL in batches, avoiding duplicates."""
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
        )

        # Define data types based on table name
        if table_name == "samsung_health_steps":
            dtype_mapping = {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "timestamp": DATETIME,
                "steps": DECIMAL(10, 2),
                "distance": DECIMAL(10, 2),
                "calories": DECIMAL(10, 2),
                "speed": DECIMAL(10, 2),
                "heart_rate": DECIMAL(5, 2),
            }
        elif table_name == "samsung_health_heart":
            dtype_mapping = {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "timestamp": DATETIME,
                "heart_rate": DECIMAL(5, 2),
                "heart_rate_zone": VARCHAR(50),
                "measurement_type": VARCHAR(50),
                "context": VARCHAR(50),
            }
        elif table_name == "samsung_health_sleep":
            dtype_mapping = {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "start_time": DATETIME,
                "end_time": DATETIME,
                "duration_minutes": DECIMAL(10, 2),
                "sleep_score": DECIMAL(5, 2),
                "deep_sleep_minutes": DECIMAL(10, 2),
                "light_sleep_minutes": DECIMAL(10, 2),
                "rem_sleep_minutes": DECIMAL(10, 2),
                "awake_minutes": DECIMAL(10, 2),
                "sleep_efficiency": DECIMAL(5, 2),
                "bed_time": DATETIME,
                "wake_up_time": DATETIME,
            }
        elif table_name == "samsung_health_workouts":
            dtype_mapping = {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "start_time": DATETIME,
                "end_time": DATETIME,
                "duration_minutes": DECIMAL(10, 2),
                "workout_type": VARCHAR(100),
                "calories_burned": DECIMAL(10, 2),
                "distance": DECIMAL(10, 2),
                "average_heart_rate": DECIMAL(5, 2),
                "max_heart_rate": DECIMAL(5, 2),
                "min_heart_rate": DECIMAL(5, 2),
                "average_speed": DECIMAL(10, 2),
                "max_speed": DECIMAL(10, 2),
                "elevation_gain": DECIMAL(10, 2),
                "elevation_loss": DECIMAL(10, 2),
                "steps": DECIMAL(10, 2),
                "strokes": DECIMAL(10, 2),
                "laps": DECIMAL(10, 2),
                "notes": TEXT,
            }
        elif table_name == "samsung_health_general":
            dtype_mapping = {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "data_type": VARCHAR(100),
                "timestamp": DATETIME,
                "value": DECIMAL(15, 6),
                "unit": VARCHAR(50),
                "metadata": JSON,
            }
        else:
            print(f"❌ Unknown table name: {table_name}")
            return 0, 0

        temp_table_name = f"temp_{table_name}"

        original_count = len(df)
        # Stronger logical dedupe per table
        if table_name == "samsung_health_steps" and {"user_id", "timestamp", "steps"}.issubset(df.columns):
            df = df.sort_values("timestamp").drop_duplicates(subset=["user_id", "timestamp"], keep="last")
        elif table_name == "samsung_health_heart" and {"user_id", "timestamp"}.issubset(df.columns):
            df = df.sort_values("timestamp").drop_duplicates(subset=["user_id", "timestamp"], keep="last")
        elif table_name == "samsung_health_workouts" and {"user_id", "start_time", "end_time", "workout_type"}.issubset(df.columns):
            df = df.sort_values("start_time").drop_duplicates(subset=["user_id", "start_time", "end_time", "workout_type"], keep="last")
        elif table_name == "samsung_health_sleep" and {"user_id", "start_time", "end_time"}.issubset(df.columns):
            df = df.sort_values("start_time").drop_duplicates(subset=["user_id", "start_time", "end_time"], keep="last")
        elif "id" in df.columns:
            df = df.drop_duplicates(subset=["id"], keep="first")
        duplicate_count = original_count - len(df)

        print(f"🟡 Original rows: {original_count}, "
              f"Dropped duplicates: {duplicate_count}, "
              f"Remaining: {len(df)}")

        if df.empty:
            print("⚠️ No rows to insert after deduplication.")
            return 0, duplicate_count

        with engine.begin() as connection:
            # Write to temporary table
            df.to_sql(
                temp_table_name,
                con=connection,
                if_exists="replace",
                index=False,
                dtype=dtype_mapping,
            )
            print(f"🟡 Temp table '{temp_table_name}' created with {len(df)} rows.")

            # Use INSERT ... ON DUPLICATE KEY UPDATE for better duplicate handling
            columns = ", ".join(df.columns)
            
            insert_query = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns}
                FROM {temp_table_name}
                ON DUPLICATE KEY UPDATE
            """
            
            # Build the SET clause for ON DUPLICATE KEY UPDATE
            set_clauses = []
            for col in df.columns:
                if col != "id":  # Don't update the primary key
                    set_clauses.append(f"{col} = VALUES({col})")
            
            insert_query += ", ".join(set_clauses)
            
            result = connection.execute(text(insert_query))
            inserted_count = result.rowcount if result.rowcount is not None else 0
            print(f"✅ Inserted/Updated {inserted_count} rows in {table_name}.")

            # Drop temp table
            connection.execute(text(f"DROP TABLE {temp_table_name}"))
            print(f"🟡 Temp table '{temp_table_name}' dropped.")

        return inserted_count, duplicate_count

    except mysql.connector.Error as err:
        print(f"❌ MySQL error while writing DataFrame: {err}")
        return 0, 0
    except Exception as e:
        print(f"❌ Unexpected error while writing DataFrame: {e}")
        return 0, 0