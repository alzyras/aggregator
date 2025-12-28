import logging
from typing import Tuple

import pandas as pd

from aggregator.core.apps import PluginService
from aggregator.infrastructure.filesystem import PluginState
from aggregator.settings import settings

from .get_habits_dailies_df import fetch_all_data
from .get_todos_df import create_dataframe, fetch_tags, get_completed_todos
from .repositories import HabiticaRepository

logger = logging.getLogger(__name__)


class HabiticaService(PluginService):
    name = "habitica"

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        self.state = PluginState(self.name)
        self.repository = HabiticaRepository()

    def setup(self) -> bool:
        self.repository.ensure_schema()
        return True

    def fetch_data(self) -> pd.DataFrame | None:
        creds = self.settings.habitica
        user_id = creds.get("user_id")
        api_token = creds.get("api_token")

        if not user_id or not api_token:
            logger.error("Habitica credentials missing; skipping fetch")
            return None

        tag_dict = fetch_tags(user_id, api_token)
        df_habits_dailies = fetch_all_data(user_id, api_token)
        completed_todos = get_completed_todos(user_id, api_token, tag_dict)
        df_todos = create_dataframe(completed_todos) if completed_todos else pd.DataFrame()

        frames = [df for df in [df_habits_dailies, df_todos] if df is not None]
        if not frames:
            return None

        df = pd.concat(frames, ignore_index=True)

        if not df.empty and not self.state.is_full_load_completed():
            self.state.mark_full_load_completed()

        return df

    def write_data(self, payload: pd.DataFrame) -> Tuple[int, int]:
        return self.repository.write_items(payload)
