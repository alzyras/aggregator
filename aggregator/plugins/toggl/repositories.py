import logging
from pathlib import Path
from typing import Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import BIGINT, BOOLEAN, DATETIME, DECIMAL, TEXT, VARCHAR

from aggregator.infrastructure.database import connection, execute_sql_file

logger = logging.getLogger(__name__)


class TogglRepository:
    table_name = "toggl_items"

    def ensure_schema(self) -> None:
        execute_sql_file(
            str(Path(__file__).parent / "queries" / "toggl_entries.sql")
        )

    def write_entries(self, df: pd.DataFrame) -> Tuple[int, int]:
        if df is None or df.empty:
            return 0, 0

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

        temp_table_name = "temp_toggl_items"

        original_count = len(df)
        df = df.drop_duplicates(subset=["id"], keep="first")
        duplicate_count = original_count - len(df)

        with connection() as conn:
            df.to_sql(
                temp_table_name,
                con=conn,
                if_exists="replace",
                index=False,
                dtype=dtype_mapping,
            )

            columns = ", ".join(df.columns)
            set_clauses = ", ".join([f"{col}=VALUES({col})" for col in df.columns if col != "id"])

            insert_query = f"""
                INSERT INTO {self.table_name} ({columns})
                SELECT {columns}
                FROM {temp_table_name}
                ON DUPLICATE KEY UPDATE {set_clauses}
            """

            result = conn.execute(text(insert_query))
            inserted_count = result.rowcount if result.rowcount is not None else 0

            conn.execute(text(f"DROP TABLE {temp_table_name}"))

        logger.info("Toggl data written to MySQL (%s inserted, %s duplicates)", inserted_count, duplicate_count)
        return inserted_count, duplicate_count
