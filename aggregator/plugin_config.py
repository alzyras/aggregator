import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

PLUGIN_CONFIG_DIR = ".plugin_configs"


class PluginConfig:
    """Manages plugin configuration and state tracking."""
    
    def __init__(self, plugin_name: str):
        self.plugin_name = plugin_name
        self.config_file = os.path.join(PLUGIN_CONFIG_DIR, f"{plugin_name}.json")
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load plugin configuration from file."""
        # Create config directory if it doesn't exist
        os.makedirs(PLUGIN_CONFIG_DIR, exist_ok=True)
        
        # Load existing config or create default
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading config for {self.plugin_name}: {e}")
                return {}
        else:
            return {}
    
    def _save_config(self) -> None:
        """Save plugin configuration to file."""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving config for {self.plugin_name}: {e}")
    
    def is_full_load_completed(self) -> bool:
        """Check if full load has been completed for this plugin."""
        return self.config.get('full_load_completed', False)
    
    def mark_full_load_completed(self) -> None:
        """Mark that full load has been completed for this plugin."""
        self.config['full_load_completed'] = True
        self.config['full_load_completed_at'] = json.dumps(
            __import__('datetime').datetime.now().isoformat()
        )
        self._save_config()
        logger.info(f"Marked full load as completed for {self.plugin_name}")
    
    def get_data_fetch_range_days(self) -> int:
        """Get the number of days to fetch data for.
        
        Returns:
            int: Number of days to fetch (548 for full load, 90 for incremental)
        """
        if self.is_full_load_completed():
            return 90  # Last 90 days for incremental loads
        else:
            return 548  # Full 1.5 years for initial load