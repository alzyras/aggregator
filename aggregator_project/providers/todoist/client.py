from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from connectors.services import get_required_account
from workspaces.models import Workspace


class TodoistClient:
    def __init__(self, workspace: Workspace) -> None:
        self.credentials = self._load_credentials(workspace)

    def _load_credentials(self, workspace: Workspace) -> dict[str, Any]:
        account = get_required_account("todoist", workspace)
        return {"api_token": account.get_access_token()}

    def fetch_since(self, since: datetime | None = None) -> list[dict[str, Any]]:
        """
        TODO: Implement Todoist API calls.
        For now, return an empty list to keep the pipeline runnable.
        """
        _ = requests
        return []
