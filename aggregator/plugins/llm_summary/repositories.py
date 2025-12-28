import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from aggregator.infrastructure.database import connection

logger = logging.getLogger(__name__)

BASE_QUERIES = Path(__file__).parent / "queries"


class LlmSummaryRepository:
    """Read-only access to plugin tables with defensive existence checks."""

    def __init__(self) -> None:
        self._logger = logger

    def table_exists(self, table: str) -> bool:
        sql = text(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = :table
            """
        )
        with connection() as conn:
            result = conn.execute(sql, {"table": table})
            return result.scalar() > 0

    def get_monthly_summary(self, sql_file: str, params: Dict[str, Any]) -> List[Dict]:
        # Google Fit needs dynamic column resolution
        if sql_file == "google_fit_monthly_summary.sql":
            return self._google_fit_monthly(params)
        path = BASE_QUERIES / sql_file
        if not path.exists():
            return []
        query = path.read_text()
        with connection() as conn:
            result = conn.execute(text(query), params)
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_categories(self, sql_file: str, params: Dict[str, Any]) -> List[Dict]:
        if sql_file == "google_fit_categories.sql":
            return self._google_fit_categories(params)
        return self.get_monthly_summary(sql_file, params)

    def get_plugin_presence(self) -> Dict[str, bool]:
        return {
            "asana": self.table_exists("asana_items"),
            "toggl": self.table_exists("toggl_items"),
            "habitica": self.table_exists("habitica_items"),
            "google_fit_steps": self.table_exists("google_fit_steps"),
            "google_fit_heart": self.table_exists("google_fit_heart"),
            "google_fit_general": self.table_exists("google_fit_general"),
        }

    def get_recent_examples(
        self, table: str, date_column: str, fields: List[str], limit_rows: int = 5
    ) -> List[Dict[str, Any]]:
        if not self.table_exists(table):
            return []
        field_sql = ", ".join(fields)
        query = text(
            f"""
            SELECT {field_sql}
            FROM {table}
            ORDER BY {date_column} DESC
            LIMIT :limit_rows
            """
        )
        with connection() as conn:
            result = conn.execute(query, {"limit_rows": limit_rows})
            cols = result.keys()
            return [dict(zip(cols, row)) for row in result.fetchall()]

    # Google Fit helpers with dynamic date column detection
    def _date_column(self, table: str) -> Optional[str]:
        sql = text(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = :table
              AND DATA_TYPE IN ('datetime', 'timestamp', 'date')
            ORDER BY FIELD(DATA_TYPE, 'datetime', 'timestamp', 'date'), ORDINAL_POSITION
            LIMIT 1
            """
        )
        with connection() as conn:
            row = conn.execute(sql, {"table": table}).fetchone()
            return row[0] if row else None

    def _google_fit_monthly(self, params: Dict[str, Any]) -> List[Dict]:
        steps_col = self._date_column("google_fit_steps")
        heart_col = self._date_column("google_fit_heart")
        general_col = self._date_column("google_fit_general")

        if not all([steps_col, heart_col, general_col]):
            return []

        query = f"""
            SELECT
                period,
                SUM(steps_total) AS steps_total,
                AVG(heart_avg) AS heart_avg,
                SUM(general_count) AS general_count
            FROM (
                SELECT DATE_FORMAT({steps_col}, '%Y-%m') AS period,
                       SUM(steps) AS steps_total,
                       NULL AS heart_avg,
                       NULL AS general_count
                FROM google_fit_steps
                WHERE {steps_col} BETWEEN :start_date AND :end_date
                GROUP BY period
                UNION ALL
                SELECT DATE_FORMAT({heart_col}, '%Y-%m') AS period,
                       NULL,
                       AVG(heart_rate) AS heart_avg,
                       NULL
                FROM google_fit_heart
                WHERE {heart_col} BETWEEN :start_date AND :end_date
                GROUP BY period
                UNION ALL
                SELECT DATE_FORMAT({general_col}, '%Y-%m') AS period,
                       NULL,
                       NULL,
                       COUNT(*) AS general_count
                FROM google_fit_general
                WHERE {general_col} BETWEEN :start_date AND :end_date
                GROUP BY period
            ) t
            GROUP BY period
            ORDER BY period DESC
            LIMIT :limit_rows;
        """
        with connection() as conn:
            result = conn.execute(text(query), params)
            cols = result.keys()
            return [dict(zip(cols, row)) for row in result.fetchall()]

    def _google_fit_categories(self, params: Dict[str, Any]) -> List[Dict]:
        general_col = self._date_column("google_fit_general")
        if not general_col:
            return []
        query = f"""
            SELECT
                data_type AS category,
                COUNT(*) AS records,
                AVG(value) AS avg_value
            FROM google_fit_general
            WHERE {general_col} BETWEEN :start_date AND :end_date
            GROUP BY category
            ORDER BY records DESC
            LIMIT :limit_rows;
        """
        with connection() as conn:
            result = conn.execute(text(query), params)
            cols = result.keys()
            return [dict(zip(cols, row)) for row in result.fetchall()]
