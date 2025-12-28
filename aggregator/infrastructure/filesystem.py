import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


class PluginState:
    """Small JSON-backed state store for plugins."""

    def __init__(self, plugin_name: str, base_dir: str = "aggregator/config") -> None:
        self.plugin_name = plugin_name
        self.path = Path(base_dir) / f"{plugin_name}.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except json.JSONDecodeError:
            return {}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self.state, indent=2))

    def is_full_load_completed(self) -> bool:
        return bool(self.state.get("full_load_completed"))

    def mark_full_load_completed(self) -> None:
        self.state["full_load_completed"] = True
        self.state["full_load_completed_at"] = datetime.utcnow().isoformat()
        self._save()

    def get_data_fetch_range_days(self) -> int:
        return 90 if self.is_full_load_completed() else 1825
