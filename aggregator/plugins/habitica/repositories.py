import logging
from pathlib import Path
from typing import Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import BOOLEAN, DATETIME, DECIMAL, TEXT, VARCHAR

from aggregator.infrastructure.database import connection, execute_sql_file

logger = logging.getLogger(__name__)


class HabiticaRepository:
    table_name = "habitica_items"

    def ensure_schema(self) -> None:
        execute_sql_file(
            str(Path(__file__).parent / "queries" / "habitica_items.sql")
        )

    def write_items(self, df: pd.DataFrame) -> Tuple[int, int]:
        if df is None or df.empty:
            return 0, 0

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

        original_count = len(df)
        df = df.drop_duplicates(
            subset=["item_id", "item_name", "item_type", "date_completed"],
            keep="first",
        )
        duplicate_count = original_count - len(df)

        with connection() as conn:
            df.to_sql(
                temp_table_name,
                con=conn,
                if_exists="replace",
                index=False,
                dtype=dtype_mapping,
            )

            insert_query = f"""
                INSERT INTO {self.table_name} (item_id, item_name, item_type, value, date_created, date_completed, notes, priority, tags, completed)
                SELECT t.item_id, t.item_name, t.item_type, t.value, t.date_created, t.date_completed, t.notes, t.priority, t.tags, t.completed
                FROM {temp_table_name} t
                LEFT JOIN {self.table_name} h ON t.item_id = h.item_id AND t.item_name = h.item_name AND t.item_type = h.item_type AND t.date_completed = h.date_completed
                WHERE h.item_id IS NULL
            """
            result = conn.execute(text(insert_query))
            inserted_count = result.rowcount

            conn.execute(text(f"DROP TABLE {temp_table_name}"))

        logger.info("Habitica data written to MySQL (%s inserted, %s duplicates)", inserted_count, duplicate_count)
        return inserted_count, duplicate_count
