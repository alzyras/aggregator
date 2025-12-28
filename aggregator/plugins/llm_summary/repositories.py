import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

    # New aggregate helpers
    def run_query(self, sql_file: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        path = BASE_QUERIES / sql_file
        if not path.exists():
            return []
        query = path.read_text()
        with connection() as conn:
            result = conn.execute(text(query), params)
            cols = result.keys()
            return [dict(zip(cols, row)) for row in result.fetchall()]

    def global_monthly(self, start_date, end_date, limit_rows=12) -> List[Dict]:
        return self.run_query("global_monthly_rollup.sql", {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})

    def global_weekly(self, start_date, end_date, limit_rows=12) -> List[Dict]:
        return self.run_query("global_weekly_rollup.sql", {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})

    def global_daily(self, start_date, end_date, limit_rows=30) -> List[Dict]:
        return self.run_query("global_daily_rollup.sql", {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})

    def top_categories_30d(self, end_date, limit_rows=8) -> List[Dict]:
        return self.run_query("global_top_categories_30d.sql", {"end_date": end_date, "limit_rows": limit_rows})

    def asana_monthly_projects(self, start_date, end_date, limit_rows=50) -> List[Dict]:
        return self.run_query("asana_monthly_project_rollup.sql", {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})

    def toggl_monthly_projects(self, start_date, end_date, limit_rows=50) -> List[Dict]:
        return self.run_query("toggl_monthly_project_rollup.sql", {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})

    def toggl_session_stats(self, start_date, end_date) -> List[Dict]:
        return self.run_query("toggl_session_stats.sql", {"start_date": start_date, "end_date": end_date})

    def habitica_daily(self, start_date, end_date) -> List[Dict]:
        return self.run_query("habitica_daily_completions.sql", {"start_date": start_date, "end_date": end_date})

    def habitica_monthly_categories(self, start_date, end_date, limit_rows=50) -> List[Dict]:
        return self.run_query("habitica_monthly_category_rollup.sql", {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})

    def google_fit_daily_steps(self, start_date, end_date) -> List[Dict]:
        return self.run_query("google_fit_daily_steps.sql", {"start_date": start_date, "end_date": end_date})

    def google_fit_weekly_health(self, start_date, end_date, limit_rows=12) -> List[Dict]:
        return self.run_query("google_fit_weekly_health_rollup.sql", {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})

    def coverage_by_source(self, start_date, end_date) -> List[Dict]:
        return self.run_query("coverage_by_source.sql", {"start_date": start_date, "end_date": end_date})

    # Windowed totals per source (start inclusive, end exclusive)
    def asana_totals(self, start_date, end_date) -> Dict[str, Any]:
        if not self.table_exists("asana_items"):
            return {}
        sql = text(
            """
            SELECT COUNT(*) AS completed, COUNT(DISTINCT project) AS projects, COUNT(DISTINCT DATE(date)) AS coverage_days
            FROM asana_items
            WHERE date >= :start_date AND date < :end_date
            """
        )
        with connection() as conn:
            row = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).fetchone()
            return dict(row._mapping) if row else {}

    def toggl_totals(self, start_date, end_date) -> Dict[str, Any]:
        if not self.table_exists("toggl_items"):
            return {}
        sql = text(
            """
            SELECT
                SUM(duration_minutes) AS minutes,
                AVG(duration_minutes) AS avg_session_minutes,
                SUM(CASE WHEN duration_minutes >= 60 THEN duration_minutes ELSE 0 END) AS deep_minutes,
                COUNT(*) AS sessions,
                COUNT(DISTINCT DATE(start_time)) AS coverage_days
            FROM toggl_items
            WHERE start_time >= :start_date AND start_time < :end_date
            """
        )
        with connection() as conn:
            row = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).fetchone()
            return dict(row._mapping) if row else {}

    def habitica_totals(self, start_date, end_date) -> Dict[str, Any]:
        if not self.table_exists("habitica_items"):
            return {}
        sql = text(
            """
            SELECT COUNT(*) AS completions, COUNT(DISTINCT DATE(date_completed)) AS coverage_days
            FROM habitica_items
            WHERE date_completed >= :start_date AND date_completed < :end_date
            """
        )
        with connection() as conn:
            row = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).fetchone()
            return dict(row._mapping) if row else {}

    def google_fit_totals(self, start_date, end_date) -> Dict[str, Any]:
        if not self.table_exists("google_fit_steps"):
            return {}
        sql = text(
            """
            SELECT
                SUM(steps) AS steps,
                COUNT(DISTINCT DATE(timestamp)) AS coverage_days
            FROM google_fit_steps
            WHERE timestamp >= :start_date AND timestamp < :end_date
            """
        )
        with connection() as conn:
            row = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).fetchone()
            return dict(row._mapping) if row else {}

    def toggl_daily_series(self, start_date, end_date) -> List[Dict]:
        if not self.table_exists("toggl_items"):
            return []
        sql = text(
            """
            SELECT DATE(start_time) AS day, SUM(duration_minutes) AS minutes
            FROM toggl_items
            WHERE start_time >= :start_date AND start_time < :end_date
            GROUP BY day
            ORDER BY day DESC
            """
        )
        with connection() as conn:
            res = conn.execute(sql, {"start_date": start_date, "end_date": end_date})
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def asana_daily_series(self, start_date, end_date) -> List[Dict]:
        if not self.table_exists("asana_items"):
            return []
        sql = text(
            """
            SELECT DATE(date) AS day, COUNT(*) AS completed
            FROM asana_items
            WHERE date >= :start_date AND date < :end_date
            GROUP BY day
            ORDER BY day DESC
            """
        )
        with connection() as conn:
            res = conn.execute(sql, {"start_date": start_date, "end_date": end_date})
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def habitica_daily_series(self, start_date, end_date) -> List[Dict]:
        return self.habitica_daily(start_date, end_date)

    def fit_daily_series(self, start_date, end_date) -> List[Dict]:
        return self.google_fit_daily_steps(start_date, end_date)

    def asana_categories_window(self, start_date, end_date, limit_rows=8) -> List[Dict]:
        if not self.table_exists("asana_items"):
            return []
        sql = text(
            """
            SELECT project AS category, COUNT(*) AS total_value
            FROM asana_items
            WHERE date >= :start_date AND date < :end_date
            GROUP BY category
            ORDER BY total_value DESC
            LIMIT :limit_rows
            """
        )
        with connection() as conn:
            res = conn.execute(sql, {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def toggl_categories_window(self, start_date, end_date, limit_rows=8) -> List[Dict]:
        if not self.table_exists("toggl_items"):
            return []
        sql = text(
            """
            SELECT COALESCE(project_name, client_name, 'Uncategorized') AS category, SUM(duration_minutes) AS total_value
            FROM toggl_items
            WHERE start_time >= :start_date AND start_time < :end_date
            GROUP BY category
            ORDER BY total_value DESC
            LIMIT :limit_rows
            """
        )
        with connection() as conn:
            res = conn.execute(sql, {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def habitica_categories_window(self, start_date, end_date, limit_rows=8) -> List[Dict]:
        if not self.table_exists("habitica_items"):
            return []
        sql = text(
            """
            SELECT COALESCE(item_type, 'unknown') AS category, COUNT(*) AS total_value
            FROM habitica_items
            WHERE date_completed >= :start_date AND date_completed < :end_date
            GROUP BY category
            ORDER BY total_value DESC
            LIMIT :limit_rows
            """
        )
        with connection() as conn:
            res = conn.execute(sql, {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def fit_categories_window(self, start_date, end_date, limit_rows=8) -> List[Dict]:
        if not self.table_exists("google_fit_general"):
            return []
        general_col = self._date_column("google_fit_general") or "timestamp"
        sql = text(
            f"""
            SELECT data_type AS category, COUNT(*) AS total_value
            FROM google_fit_general
            WHERE {general_col} >= :start_date AND {general_col} < :end_date
            GROUP BY category
            ORDER BY total_value DESC
            LIMIT :limit_rows
            """
        )
        with connection() as conn:
            res = conn.execute(sql, {"start_date": start_date, "end_date": end_date, "limit_rows": limit_rows})
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    # Focused discovery (keyword-based)
    def _like_clauses(self, fields: List[str], patterns: List[str]) -> Tuple[str, Dict[str, Any]]:
        clauses = []
        params: Dict[str, Any] = {}
        for i, pat in enumerate(patterns):
            pname = f"pat{i}"
            params[pname] = f"%{pat}%"
            field_checks = [f"LOWER(COALESCE({f},'')) LIKE :{pname}" for f in fields]
            clauses.append("(" + " OR ".join(field_checks) + ")")
        if not clauses:
            clauses.append("1=0")
        return " OR ".join(clauses), params

    def asana_focus_daily(self, patterns: List[str], start_date=None, end_date=None) -> List[Dict]:
        if not self.table_exists("asana_items"):
            return []
        clause, params = self._like_clauses(["task_name", "task_description", "project"], patterns)
        sql = text(
            f"""
            SELECT DATE(date) AS day, COUNT(*) AS value
            FROM asana_items
            WHERE date >= :start_date AND date < :end_date AND ({clause})
            GROUP BY day
            ORDER BY day DESC
            """
        )
        params.update({"start_date": start_date, "end_date": end_date})
        with connection() as conn:
            res = conn.execute(sql, params)
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def toggl_focus_daily(self, patterns: List[str], start_date=None, end_date=None) -> List[Dict]:
        if not self.table_exists("toggl_items"):
            return []
        clause, params = self._like_clauses(["project_name", "client_name", "description"], patterns)
        sql = text(
            f"""
            SELECT DATE(start_time) AS day, SUM(duration_minutes) AS value
            FROM toggl_items
            WHERE start_time >= :start_date AND start_time < :end_date AND ({clause})
            GROUP BY day
            ORDER BY day DESC
            """
        )
        params.update({"start_date": start_date, "end_date": end_date})
        with connection() as conn:
            res = conn.execute(sql, params)
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def habitica_focus_daily(self, patterns: List[str], start_date=None, end_date=None) -> List[Dict]:
        if not self.table_exists("habitica_items"):
            return []
        clause, params = self._like_clauses(["item_name", "notes", "tags"], patterns)
        sql = text(
            f"""
            SELECT DATE(date_completed) AS day, COUNT(*) AS value
            FROM habitica_items
            WHERE date_completed >= :start_date AND date_completed < :end_date AND ({clause})
            GROUP BY day
            ORDER BY day DESC
            """
        )
        params.update({"start_date": start_date, "end_date": end_date})
        with connection() as conn:
            res = conn.execute(sql, params)
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    def fit_focus_daily(self, patterns: List[str], start_date=None, end_date=None) -> List[Dict]:
        if not self.table_exists("google_fit_general"):
            return []
        general_col = self._date_column("google_fit_general") or "timestamp"
        clause, params = self._like_clauses(["data_type", "source"], patterns)
        sql = text(
            f"""
            SELECT DATE({general_col}) AS day, COUNT(*) AS value
            FROM google_fit_general
            WHERE {general_col} >= :start_date AND {general_col} < :end_date AND ({clause})
            GROUP BY day
            ORDER BY day DESC
            """
        )
        params.update({"start_date": start_date, "end_date": end_date})
        with connection() as conn:
            res = conn.execute(sql, params)
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]
