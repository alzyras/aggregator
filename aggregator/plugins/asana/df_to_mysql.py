import os

import mysql.connector
from sqlalchemy import create_engine, text
from sqlalchemy.types import BOOLEAN, DATETIME, REAL, TEXT, VARCHAR

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


def write_asana_dataframe_to_mysql_batch(df):
    """Writes an Asana DataFrame to a MySQL database in batches, avoiding duplicates based on task_id. Returns insert and duplicate counts."""
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
        )
        connection = engine.connect()

        dtype_mapping = {
            "task_id": VARCHAR(255),
            "task_name": TEXT,
            "time_to_completion": REAL,
            "project": TEXT,
            "project_created_at": DATETIME,
            "project_notes": TEXT,
            "project_owner": TEXT,
            "completed_by_name": TEXT,
            "completed_by_email": TEXT,
            "completed": BOOLEAN,
            "task_description": TEXT,
            "date": DATETIME,
            "created_by_name": TEXT,
            "created_by_email": TEXT,
            "type": VARCHAR(10),
        }

        temp_table_name = "temp_asana_tasks"

        # Remove duplicates in the DataFrame based on task_id (MySQL PK constraint)
        original_count = len(df)
        df = df.drop_duplicates(subset=["task_id"], keep="first")
        duplicate_count = original_count - len(df)

        df.to_sql(
            temp_table_name,
            con=connection,
            if_exists="replace",
            index=False,
            dtype=dtype_mapping,
        )

        insert_query = f"""
            INSERT IGNORE INTO asana_items (
                task_id, task_name, time_to_completion, project, project_created_at, project_notes,
                project_owner, completed_by_name, completed_by_email, completed, task_description,
                date, created_by_name, created_by_email, type
            )
            SELECT 
                task_id, task_name, time_to_completion, project, project_created_at, project_notes,
                project_owner, completed_by_name, completed_by_email, completed, task_description,
                date, created_by_name, created_by_email, type
            FROM {temp_table_name}
        """
        result = connection.execute(text(insert_query))
        inserted_count = result.rowcount

        connection.execute(text(f"DROP TABLE {temp_table_name}"))
        connection.commit()
        print("✅ Asana DataFrame written to MySQL successfully.")
        return inserted_count, duplicate_count

    except mysql.connector.Error as err:
        print(f"❌ Error writing Asana DataFrame to MySQL: {err}")
        return 0, 0
    finally:
        if "connection" in locals() and connection:
            connection.close()
