import os
import pandas as pd
import logging
from typing import Tuple

from uv_app.plugin_interface import PluginInterface
from uv_app.plugins.habitica.df_to_mysql import write_dataframe_to_mysql_batch, execute_sql_file

from .get_habits_dailies_df import main as get_habits_dailies_df
from .get_todos_df import main as get_todos_df

# Create a logger for this module
logger = logging.getLogger(__name__)


class HabiticaPlugin(PluginInterface):
    """Habitica plugin implementation."""

    @property
    def name(self) -> str:
        """Return the name of the plugin."""
        return "habitica"

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from the service and return as a DataFrame."""
        logger.info("Fetching data from Habitica")
        df_habits_dailies = get_habits_dailies_df()
        df_todos = get_todos_df()
        df = pd.concat([df_habits_dailies, df_todos], ignore_index=True)
        return df

    def write_to_database(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Write DataFrame to database and return (inserted_count, duplicate_count)."""
        if df is not None and not df.empty:
            logger.info(f"Writing {len(df)} records to database")
            inserted_count, duplicate_count = write_dataframe_to_mysql_batch(df, "Habitica")
            return inserted_count, duplicate_count
        return 0, 0

    def setup_database(self) -> bool:
        """Set up the database schema for this plugin."""
        logger.info("Setting up habitica database schema")
        # Execute the SQL file to create the table
        execute_sql_file("uv_app/plugins/habitica/sql/habitica_items.sql")
        return True
