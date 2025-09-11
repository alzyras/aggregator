import os
import mysql.connector
from sqlalchemy import create_engine, text
from sqlalchemy.types import BIGINT, BOOLEAN, DATETIME, DECIMAL, TEXT, VARCHAR


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


def write_toggl_dataframe_to_mysql_batch(df):
    """Writes a Toggl DataFrame to MySQL in batches, avoiding duplicates."""
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
        )

        dtype_mapping = {
            "id": BIGINT,
            "user_id": BIGINT,
            "user_name": VARCHAR(255),
            "project_id": BIGINT,
            "project_name": VARCHAR(255),
            "client_id": BIGINT,
            "client_name": VARCHAR(255),
            "description": TEXT,
            "start_time": DATETIME,
            "end_time": DATETIME,
            "duration_minutes": DECIMAL(10, 2),
            "tags": TEXT,
            "billable": BOOLEAN,
            "created_at": DATETIME,
        }

        temp_table_name = "temp_toggl_entries"

        original_count = len(df)
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
            insert_query = f"""
                INSERT INTO toggl_entries (
                    id, user_id, user_name, project_id, project_name, client_id, client_name,
                    description, start_time, end_time, duration_minutes, tags, billable, created_at
                )
                SELECT 
                    id, user_id, user_name, project_id, project_name, client_id, client_name,
                    description, start_time, end_time, duration_minutes, tags, billable, created_at
                FROM {temp_table_name}
                ON DUPLICATE KEY UPDATE
                    user_id = VALUES(user_id),
                    user_name = VALUES(user_name),
                    project_id = VALUES(project_id),
                    project_name = VALUES(project_name),
                    client_id = VALUES(client_id),
                    client_name = VALUES(client_name),
                    description = VALUES(description),
                    start_time = VALUES(start_time),
                    end_time = VALUES(end_time),
                    duration_minutes = VALUES(duration_minutes),
                    tags = VALUES(tags),
                    billable = VALUES(billable),
                    created_at = VALUES(created_at)
            """
            result = connection.execute(text(insert_query))
            inserted_count = result.rowcount if result.rowcount is not None else 0
            print(f"✅ Inserted/Updated {inserted_count} rows in toggl_entries.")

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