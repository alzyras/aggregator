import os
import pandas as pd
import logging
from typing import Tuple

from aggregator.plugin_interface import PluginInterface
from aggregator.plugin_config import PluginConfig
from aggregator.plugins.habitica.df_to_mysql import write_dataframe_to_mysql_batch, execute_sql_file

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
        
        # Get data fetch range from plugin config
        plugin_config = PluginConfig(self.name)
        days_to_fetch = plugin_config.get_data_fetch_range_days()
        
        df_habits_dailies = get_habits_dailies_df(days_to_fetch)
        df_todos = get_todos_df(days_to_fetch)
        df = pd.concat([df_habits_dailies, df_todos], ignore_index=True)
        
        # Mark full load as completed after successful fetch
        if not plugin_config.is_full_load_completed():
            plugin_config.mark_full_load_completed()
        
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
        execute_sql_file("aggregator/plugins/habitica/sql/habitica_items.sql")
        return True
