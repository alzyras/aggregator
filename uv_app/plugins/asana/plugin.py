import pandas as pd
import logging
from typing import Tuple
from uv_app.plugin_interface import PluginInterface
from uv_app.config import config
from uv_app.plugins.asana.get_done_tasks_df import get_df as get_asana_df
from uv_app.plugins.asana.df_to_mysql import write_asana_dataframe_to_mysql_batch, execute_sql_file


# Create a logger for this module
logger = logging.getLogger(__name__)


class AsanaPlugin(PluginInterface):
    """Asana plugin implementation."""

    @property
    def name(self) -> str:
        """Return the name of the plugin."""
        return "asana"

    def fetch_data(self) -> pd.DataFrame:
        """Fetch completed tasks from Asana."""
        # Pass None for client_id and client_secret for backward compatibility
        dataframe = get_asana_df(
            None,  # client_id (not used)
            None,  # client_secret (not used)
            config.asana_workspace_gid
        )
        # Return an empty DataFrame if we couldn't fetch data
        if dataframe is None:
            return pd.DataFrame()
        return dataframe

    def write_to_database(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Write Asana DataFrame to MySQL database."""
        if df is not None and not df.empty:
            return write_asana_dataframe_to_mysql_batch(df)
        return 0, 0

    def setup_database(self) -> bool:
        """Set up the Asana database schema."""
        try:
            execute_sql_file("store/sql/asana_items.sql")
            return True
        except Exception as e:
            logger.error(f"Error setting up Asana database: {e}")
            return False
