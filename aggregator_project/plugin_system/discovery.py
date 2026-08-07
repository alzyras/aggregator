from __future__ import annotations

import json
from pathlib import Path


def discover_plugin_apps(plugins_root: Path) -> list[str]:
    """Return AppConfig paths declared by self-contained plugin manifests."""
    app_configs: list[str] = []
    if not plugins_root.exists():
        return app_configs

    for manifest_path in sorted(plugins_root.glob("*/plugin.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Invalid plugin manifest: {manifest_path}") from exc

        app_config = manifest.get("app_config")
        if not isinstance(app_config, str) or not app_config.strip():
            raise RuntimeError(
                f"Plugin manifest is missing app_config: {manifest_path}"
            )
        app_configs.append(app_config.strip())

    return app_configs
