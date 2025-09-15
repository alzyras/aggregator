import os
import pandas as pd
import logging
from typing import Tuple

from aggregator.plugin_interface import PluginInterface
from aggregator.plugin_config import PluginConfig
from aggregator.plugins.asana.df_to_mysql import write_asana_dataframe_to_mysql_batch, execute_sql_file

from .get_done_tasks_df import get_asana_completed_tasks_df as get_asana_df

# Create a logger for this module
logger = logging.getLogger(__name__)


class AsanaPlugin(PluginInterface):
    """Asana plugin implementation."""

    @property
    def name(self) -> str:
        """Return the name of the plugin."""
        return "asana"

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from the service and return as a DataFrame."""
        logger.info("Fetching data from Asana")
        # Get the access token and workspace GID from environment variables
        access_token = os.environ.get("ASANA_PERSONAL_ACCESS_TOKEN")
        workspace_gid = os.environ.get("ASANA_WORKSPACE_GID")
        
        # Get data fetch range from plugin config
        plugin_config = PluginConfig(self.name)
        days_to_fetch = plugin_config.get_data_fetch_range_days()
        
        df = get_asana_df(access_token, workspace_gid, days_to_fetch)
        
        # Mark full load as completed after successful fetch
        if not plugin_config.is_full_load_completed():
            plugin_config.mark_full_load_completed()
        
        return df

    def write_to_database(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Write DataFrame to database and return (inserted_count, duplicate_count)."""
        if df is not None and not df.empty:
            logger.info(f"Writing {len(df)} records to database")
            inserted_count, duplicate_count = write_asana_dataframe_to_mysql_batch(df)
            return inserted_count, duplicate_count
        return 0, 0

    def setup_database(self) -> bool:
        """Set up the database schema for this plugin."""
        logger.info("Setting up asana database schema")
        # Execute the SQL file to create the table
        execute_sql_file("aggregator/plugins/asana/sql/asana_items.sql")
        return True
