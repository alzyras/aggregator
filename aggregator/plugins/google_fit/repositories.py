import logging
from pathlib import Path
from typing import Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import DATETIME, DECIMAL, JSON, VARCHAR

from aggregator.infrastructure.database import connection, execute_sql_file

logger = logging.getLogger(__name__)


class GoogleFitRepository:
    def ensure_schema(self) -> None:
        base = Path(__file__).parent / "queries"
        for sql_file in ["steps.sql", "heart.sql", "general.sql"]:
            execute_sql_file(str(base / sql_file))

    def write_dataframe(self, df: pd.DataFrame, table_name: str) -> Tuple[int, int]:
        if df is None or df.empty:
            return 0, 0

        dtype_mapping = {
            "google_fit_steps": {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "timestamp": DATETIME,
                "steps": DECIMAL(10, 2),
            },
            "google_fit_heart": {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "timestamp": DATETIME,
                "heart_rate": DECIMAL(5, 2),
            },
            "google_fit_general": {
                "id": VARCHAR(255),
                "user_id": VARCHAR(255),
                "data_type": VARCHAR(100),
                "timestamp": DATETIME,
                "value": DECIMAL(15, 6),
                "unit": VARCHAR(50),
                "metadata": JSON,
            },
        }

        if table_name not in dtype_mapping:
            logger.error("Unknown Google Fit table: %s", table_name)
            return 0, 0

        temp_table_name = f"temp_{table_name}"

        original_count = len(df)
        df = df.drop_duplicates(subset=df.columns.difference(["id"]).tolist(), keep="last")
        duplicate_count = original_count - len(df)

        with connection() as conn:
            df.to_sql(
                temp_table_name,
                con=conn,
                if_exists="replace",
                index=False,
                dtype=dtype_mapping[table_name],
            )

            columns = ", ".join(df.columns)
            set_clauses = ", ".join(
                [f"{col}=VALUES({col})" for col in df.columns if col != "id"]
            )

            insert_query = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns}
                FROM {temp_table_name}
                ON DUPLICATE KEY UPDATE {set_clauses}
            """

            result = conn.execute(text(insert_query))
            inserted_count = result.rowcount if result.rowcount is not None else 0

            conn.execute(text(f"DROP TABLE {temp_table_name}"))

        logger.info(
            "Google Fit data written to %s (%s inserted, %s duplicates)",
            table_name,
            inserted_count,
            duplicate_count,
        )
        return inserted_count, duplicate_count
