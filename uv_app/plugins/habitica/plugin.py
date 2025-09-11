import pandas as pd
import logging
from typing import Tuple
from uv_app.plugin_interface import PluginInterface
from uv_app.plugins.habitica.get_habits_dailies_df import main as get_habits_dailies_df
from uv_app.plugins.habitica.get_todos_df import main as get_todos_df
from uv_app.plugins.habitica.df_to_mysql import write_dataframe_to_mysql_batch, execute_sql_file


# Create a logger for this module
logger = logging.getLogger(__name__)


class HabiticaPlugin(PluginInterface):
    """Habitica plugin implementation."""

    @property
    def name(self) -> str:
        """Return the name of the plugin."""
        return "habitica"

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from Habitica (todos, habits, and dailies)."""
        # Get todos
        df_todos = get_todos_df()
        
        # Get habits and dailies
        df_habits_dailies = get_habits_dailies_df()
        
        # Combine dataframes
        if df_todos is not None and df_habits_dailies is not None:
            combined_dataframe = pd.concat([df_todos, df_habits_dailies], ignore_index=True)
            return combined_dataframe
        if df_todos is not None:
            return df_todos
        if df_habits_dailies is not None:
            return df_habits_dailies
        return pd.DataFrame()

    def write_to_database(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Write Habitica DataFrame to MySQL database."""
        if df is not None and not df.empty:
            # For simplicity, we'll treat all items the same way
            # In a more complex implementation, we might want to separate them
            return write_dataframe_to_mysql_batch(df, "Habitica Items")
        return 0, 0

    def setup_database(self) -> bool:
        """Set up the Habitica database schema."""
        try:
            execute_sql_file("store/sql/habitica_items.sql")
            return True
        except Exception as e:
            logger.error(f"Error setting up Habitica database: {e}")
            return False
