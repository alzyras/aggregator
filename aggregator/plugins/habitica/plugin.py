"""Backward-compatible alias to the new service implementation."""

from aggregator.plugins.habitica.services import HabiticaService as HabiticaPlugin

__all__ = ["HabiticaPlugin"]
