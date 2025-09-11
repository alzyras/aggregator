import pandas as pd
import logging
from typing import Tuple
from uv_app.plugin_interface import PluginInterface


# Create a logger for this module
logger = logging.getLogger(__name__)


class ExamplePlugin(PluginInterface):
    """Example plugin implementation."""

    @property
    def name(self) -> str:
        """Return the name of the plugin."""
        return "example"

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from the service and return as a DataFrame."""
        # TODO: Implement data fetching logic
        logger.info("Fetching data from example service")
        # Return an empty DataFrame as a placeholder
        return pd.DataFrame()

    def write_to_database(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Write DataFrame to database and return (inserted_count, duplicate_count)."""
        # TODO: Implement database writing logic
        if df is not None and not df.empty:
            logger.info(f"Writing {len(df)} records to database")
            return len(df), 0  # placeholder values
        return 0, 0

    def setup_database(self) -> bool:
        """Set up the database schema for this plugin."""
        # TODO: Implement database setup logic
        logger.info("Setting up example database schema")
        return True