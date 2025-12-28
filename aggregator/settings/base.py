import os
from dataclasses import dataclass
from typing import List


@dataclass
class DatabaseSettings:
    host: str
    name: str
    user: str
    password: str


class Settings:
    """Django-inspired settings container with explicit configuration."""

    def __init__(self) -> None:
        self.environment = os.environ.get("AGGREGATOR_ENV", "base")
        self.interval_seconds = int(os.environ.get("INTERVAL_SECONDS", "3600"))
        self.log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

        self.database = DatabaseSettings(
            host=os.environ.get("MYSQL_HOST", ""),
            name=os.environ.get("MYSQL_DB", ""),
            user=os.environ.get("MYSQL_USER", ""),
            password=os.environ.get("MYSQL_PASSWORD", ""),
        )

        # Explicit plugin enablement (empty list = enable all known apps)
        enabled_plugins = os.environ.get("ENABLED_PLUGINS", "")
        self.enabled_plugins: List[str] = [
            plugin.strip().lower()
            for plugin in enabled_plugins.split(",")
            if plugin.strip()
        ]

        # Plugin-specific credentials (kept close to settings to avoid hidden globals)
        self.asana = {
            "personal_access_token": os.environ.get("ASANA_PERSONAL_ACCESS_TOKEN"),
            "workspace_gid": os.environ.get("ASANA_WORKSPACE_GID"),
        }
        self.habitica = {
            "user_id": os.environ.get("HABITICA_USER_ID"),
            "api_token": os.environ.get("HABITICA_API_TOKEN"),
        }
        self.toggl = {
            "api_token": os.environ.get("TOGGL_API_TOKEN"),
            "workspace_id": os.environ.get("TOGGL_WORKSPACE_ID"),
        }
        self.google_fit = {
            "client_id": os.environ.get("GOOGLE_FIT_CLIENT_ID"),
            "client_secret": os.environ.get("GOOGLE_FIT_CLIENT_SECRET"),
            "refresh_token": os.environ.get("GOOGLE_FIT_REFRESH_TOKEN"),
        }

        # Apps must be declared explicitly; no dynamic discovery.
        self.INSTALLED_APPS = [
            "aggregator.plugins.asana.apps.AsanaConfig",
            "aggregator.plugins.habitica.apps.HabiticaConfig",
            "aggregator.plugins.toggl.apps.TogglConfig",
            "aggregator.plugins.google_fit.apps.GoogleFitConfig",
        ]

    def is_app_enabled(self, app_label: str) -> bool:
        """Return whether the app is enabled by configuration."""
        return not self.enabled_plugins or app_label in self.enabled_plugins

    def validate(self) -> dict:
        """Validate configuration and return any errors."""
        errors = {}

        if not all(
            [
                self.database.host,
                self.database.name,
                self.database.user,
                self.database.password,
            ]
        ):
            errors["mysql"] = (
                "Missing MySQL configuration (MYSQL_HOST, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD)"
            )

        if "asana" in self.enabled_plugins or not self.enabled_plugins:
            if not all(self.asana.values()):
                errors["asana"] = (
                    "Missing Asana configuration (ASANA_PERSONAL_ACCESS_TOKEN, ASANA_WORKSPACE_GID)"
                )

        if "habitica" in self.enabled_plugins or not self.enabled_plugins:
            if not all(self.habitica.values()):
                errors["habitica"] = (
                    "Missing Habitica configuration (HABITICA_USER_ID, HABITICA_API_TOKEN)"
                )

        if "toggl" in self.enabled_plugins or not self.enabled_plugins:
            if not all(self.toggl.values()):
                errors["toggl"] = (
                    "Missing Toggl configuration (TOGGL_API_TOKEN, TOGGL_WORKSPACE_ID)"
                )

        if "google_fit" in self.enabled_plugins or not self.enabled_plugins:
            if not all(
                [
                    self.google_fit["client_id"],
                    self.google_fit["client_secret"],
                ]
            ):
                errors["google_fit"] = (
                    "Missing Samsung Health/Google Fit configuration (GOOGLE_FIT_CLIENT_ID, GOOGLE_FIT_CLIENT_SECRET, GOOGLE_FIT_REFRESH_TOKEN)"
                )

        return errors


settings = Settings()
