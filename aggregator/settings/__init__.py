import importlib
import os

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def load_settings():
    """Load the configured settings module (mirrors Django's approach)."""
    module_path = os.environ.get(
        "AGGREGATOR_SETTINGS_MODULE",
        "aggregator.settings.base",
    )
    module = importlib.import_module(module_path)
    return getattr(module, "settings")


settings = load_settings()

__all__ = ["settings"]
