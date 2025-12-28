"""Compatibility shim to reuse the new PluginState implementation."""

from aggregator.infrastructure.filesystem import PluginState as PluginConfig

__all__ = ["PluginConfig"]
