import os
import logging
from typing import Dict, Any, List


# Create a logger for this module
logger = logging.getLogger(__name__)


class Config:
    """Configuration class for the wellness statistics application."""

    def __init__(self):
        """Initialize configuration from environment variables."""
        # General settings
        self.interval_seconds = int(os.environ.get('INTERVAL_SECONDS', '3600'))  # Default to 1 hour
        
        # MySQL settings
        self.mysql_host = os.environ.get('MYSQL_HOST')
        self.mysql_db = os.environ.get('MYSQL_DB')
        self.mysql_user = os.environ.get('MYSQL_USER')
        self.mysql_password = os.environ.get('MYSQL_PASSWORD')
        
        # Plugin enablement - parse comma-separated list
        enabled_plugins = os.environ.get('ENABLED_PLUGINS', '')
        self.enabled_plugins: List[str] = [
            plugin.strip().lower() for plugin in enabled_plugins.split(',') if plugin.strip()
        ]
        
        # Asana settings (only needed if Asana is enabled)
        self.asana_personal_access_token = os.environ.get('ASANA_PERSONAL_ACCESS_TOKEN')
        self.asana_workspace_gid = os.environ.get('ASANA_WORKSPACE_GID')
        
        # Habitica settings (only needed if Habitica is enabled)
        self.habitica_user_id = os.environ.get('HABITICA_USER_ID')
        self.habitica_api_token = os.environ.get('HABITICA_API_TOKEN')
        
        # Toggl settings (only needed if Toggl is enabled)
        self.toggl_api_token = os.environ.get('TOGGL_API_TOKEN')
        self.toggl_workspace_id = os.environ.get('TOGGL_WORKSPACE_ID')

    def validate(self) -> Dict[str, Any]:
        """Validate configuration and return any errors."""
        errors = {}
        
        # Check MySQL configuration
        if not all([self.mysql_host, self.mysql_db, self.mysql_user, self.mysql_password]):
            errors["mysql"] = (
                "Missing MySQL configuration (MYSQL_HOST, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD)"
            )
        
        # Check Asana configuration if enabled
        if "asana" in self.enabled_plugins and not all(
            [
                self.asana_personal_access_token,
                self.asana_workspace_gid,
            ]
        ):
            errors["asana"] = (
                "Missing Asana configuration (ASANA_PERSONAL_ACCESS_TOKEN, ASANA_WORKSPACE_GID)"
            )
        
        # Check Habitica configuration if enabled
        if "habitica" in self.enabled_plugins and not all([self.habitica_user_id, self.habitica_api_token]):
            errors["habitica"] = (
                "Missing Habitica configuration (HABITICA_USER_ID, HABITICA_API_TOKEN)"
            )
        
        # Check Toggl configuration if enabled
        if "toggl" in self.enabled_plugins and not all([self.toggl_api_token, self.toggl_workspace_id]):
            errors["toggl"] = (
                "Missing Toggl configuration (TOGGL_API_TOKEN, TOGGL_WORKSPACE_ID)"
            )
        
        return errors

    def get_enabled_modules(self) -> list:
        """Return a list of enabled module names."""
        return self.enabled_plugins


# Global configuration instance
config = Config()
