import logging
from typing import Tuple

import pandas as pd

from aggregator.core.apps import PluginService
from aggregator.infrastructure.filesystem import PluginState
from aggregator.settings import settings

from .get_done_tasks_df import get_asana_completed_tasks_df
from .repositories import AsanaRepository

logger = logging.getLogger(__name__)


class AsanaService(PluginService):
    name = "asana"

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        self.state = PluginState(self.name)
        self.repository = AsanaRepository()

    def setup(self) -> bool:
        self.repository.ensure_schema()
        return True

    def fetch_data(self) -> pd.DataFrame | None:
        creds = self.settings.asana
        days_to_fetch = self.state.get_data_fetch_range_days()
        logger.info("Asana fetch: requesting last %s days", days_to_fetch)
        df = get_asana_completed_tasks_df(
            creds.get("personal_access_token"),
            creds.get("workspace_gid"),
            days_to_fetch,
        )
        if df is None:
            logger.warning("Asana fetch returned no dataframe")
        else:
            logger.info("Asana fetch returned %s rows", len(df))
        if df is not None and not df.empty and not self.state.is_full_load_completed():
            self.state.mark_full_load_completed()
        return df

    def write_data(self, payload: pd.DataFrame) -> Tuple[int, int]:
        return self.repository.write_tasks(payload)
