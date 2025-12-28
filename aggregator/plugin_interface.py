"""Backward compatibility shim for the new PluginService contract."""

from aggregator.core.apps import PluginService as PluginInterface

__all__ = ["PluginInterface"]
