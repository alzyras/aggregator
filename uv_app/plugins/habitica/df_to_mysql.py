import os

import mysql.connector
from sqlalchemy import create_engine, text
from sqlalchemy.types import BOOLEAN, DATETIME, DECIMAL, TEXT, VARCHAR

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


def write_dataframe_to_mysql_batch(df, item_type_name):
    """Writes a Pandas DataFrame to a MySQL database in batches, avoiding duplicates. Returns insert and duplicate counts."""
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
        )
        connection = engine.connect()

        dtype_mapping = {
            "item_id": VARCHAR(36),
            "item_name": VARCHAR(255),
            "item_type": VARCHAR(50),
            "value": DECIMAL(10, 8),
            "date_created": DATETIME,
            "date_completed": DATETIME,
            "notes": TEXT,
            "priority": DECIMAL(3, 1),
            "tags": TEXT,
            "completed": BOOLEAN,
        }

        temp_table_name = "temp_habitica_items"

        # Remove duplicates from DataFrame based on specified columns
        original_count = len(df)
        df = df.drop_duplicates(
            subset=["item_id", "item_name", "item_type", "date_completed"], keep="first"
        )
        duplicate_count = original_count - len(df)

        df.to_sql(
            temp_table_name,
            con=connection,
            if_exists="replace",
            index=False,
            dtype=dtype_mapping,
        )

        insert_query = f"""
            INSERT INTO habitica_items (item_id, item_name, item_type, value, date_created, date_completed, notes, priority, tags, completed)
            SELECT t.item_id, t.item_name, t.item_type, t.value, t.date_created, t.date_completed, t.notes, t.priority, t.tags, t.completed
            FROM {temp_table_name} t
            LEFT JOIN habitica_items h ON t.item_id = h.item_id AND t.item_name = h.item_name AND t.item_type = h.item_type AND t.date_completed = h.date_completed
            WHERE h.item_id IS NULL
        """
        result = connection.execute(text(insert_query))
        inserted_count = result.rowcount

        connection.execute(text(f"DROP TABLE {temp_table_name}"))
        connection.commit()
        print(
            f"✅ {item_type_name} DataFrame written to MySQL in batches successfully."
        )
        return inserted_count, duplicate_count

    except mysql.connector.Error as err:
        print(f"❌ Error writing {item_type_name} to MySQL: {err}")
        return 0, 0
    finally:
        if "connection" in locals() and connection:
            connection.close()
