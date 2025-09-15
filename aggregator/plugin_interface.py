from abc import ABC, abstractmethod

import pandas as pd


class PluginInterface(ABC):
    """Abstract base class for all plugins."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the plugin."""

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from the service and return as a DataFrame."""

    @abstractmethod
    def write_to_database(self, df: pd.DataFrame) -> tuple[int, int]:
        """Write DataFrame to database and return (inserted_count, duplicate_count)."""

    @abstractmethod
    def setup_database(self) -> bool:
        """Set up the database schema for this plugin."""
