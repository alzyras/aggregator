import logging
from typing import Dict, List
from uv_app.plugin_interface import PluginInterface
from uv_app.config import config


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
                    from uv_app.plugins.asana.plugin import AsanaPlugin
                    self.plugins[module_name] = AsanaPlugin()
                elif module_name == "habitica":
                    from uv_app.plugins.habitica.plugin import HabiticaPlugin
                    self.plugins[module_name] = HabiticaPlugin()
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
