class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""


class PluginLoadError(Exception):
    """Raised when a plugin cannot be loaded or initialized."""


class CommandError(Exception):
    """Raised for invalid manage.py commands."""
