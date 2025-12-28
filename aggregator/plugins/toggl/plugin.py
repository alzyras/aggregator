"""Backward-compatible alias to the new service implementation."""

from aggregator.plugins.toggl.services import TogglService as TogglPlugin

__all__ = ["TogglPlugin"]
