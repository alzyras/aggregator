import logging
from typing import Dict, List
from aggregator.plugin_interface import PluginInterface
from aggregator.config import config


# Create a logger for this module
logger = logging.getLogger(__name__)


class PluginManager:
    """Manages loading and accessing plugins."""

    def __init__(self):
        self.plugins: Dict[str, PluginInterface] = {}
        self._load_plugins()

    def _load_plugins(self) -> None:
        """Dynamically load enabled plugins."""
        enabled_modules = config.get_enabled_modules()
        
        for module_name in enabled_modules:
            try:
                if module_name == "asana":
                    from aggregator.plugins.asana.plugin import AsanaPlugin
                    self.plugins[module_name] = AsanaPlugin()
                elif module_name == "habitica":
                    from aggregator.plugins.habitica.plugin import HabiticaPlugin
                    self.plugins[module_name] = HabiticaPlugin()
                elif module_name == "toggl":
                    from aggregator.plugins.toggl.plugin import TogglPlugin
                    self.plugins[module_name] = TogglPlugin()
                elif module_name == "google_fit":
                    from aggregator.plugins.google_fit.plugin import GoogleFitPlugin
                    self.plugins[module_name] = GoogleFitPlugin()
                logger.info(f"Loaded plugin: {module_name}")
            except ImportError as e:
                logger.error(f"Failed to load plugin {module_name}: {e}")
            except Exception as e:
                logger.error(f"Error initializing plugin {module_name}: {e}")

    def get_plugin(self, name: str) -> PluginInterface:
        """Get a plugin by name."""
        return self.plugins.get(name)

    def get_plugins(self) -> List[PluginInterface]:
        """Get all loaded plugins."""
        return list(self.plugins.values())

    def get_plugin_names(self) -> List[str]:
        """Get names of all loaded plugins."""
        return list(self.plugins.keys())
