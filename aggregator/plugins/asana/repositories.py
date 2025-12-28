import logging
from pathlib import Path
from typing import Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import BOOLEAN, DATETIME, REAL, TEXT, VARCHAR

from aggregator.infrastructure.database import connection, execute_sql_file, get_engine

logger = logging.getLogger(__name__)


class AsanaRepository:
    table_name = "asana_items"

    def __init__(self) -> None:
        self.engine = get_engine()

    def ensure_schema(self) -> None:
        execute_sql_file(
            str(Path(__file__).parent / "queries" / "asana_items.sql")
        )

    def write_tasks(self, df: pd.DataFrame) -> Tuple[int, int]:
        if df is None or df.empty:
            return 0, 0

        dtype_mapping = {
            "task_id": VARCHAR(255),
            "task_name": TEXT,
            "time_to_completion": REAL,
            "project": TEXT,
            "workspace_id": TEXT,
            "workspace_name": TEXT,
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

        original_count = len(df)
        df = df.drop_duplicates(subset=["task_id"], keep="first")
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
                INSERT IGNORE INTO {self.table_name} (
                    task_id, task_name, time_to_completion, project, workspace_id, workspace_name, project_created_at, project_notes,
                    project_owner, completed_by_name, completed_by_email, completed, task_description,
                    date, created_by_name, created_by_email, type
                )
                SELECT 
                    task_id, task_name, time_to_completion, project, workspace_id, workspace_name, project_created_at, project_notes,
                    project_owner, completed_by_name, completed_by_email, completed, task_description,
                    date, created_by_name, created_by_email, type
                FROM {temp_table_name}
            """
            result = conn.execute(text(insert_query))
            inserted_count = result.rowcount
            conn.execute(text(f"DROP TABLE {temp_table_name}"))

        logger.info("Asana data written to MySQL (%s inserted, %s duplicates)", inserted_count, duplicate_count)
        return inserted_count, duplicate_count
