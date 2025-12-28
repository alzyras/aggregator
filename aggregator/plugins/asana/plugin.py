"""Backward-compatible alias to the new service implementation."""

from aggregator.plugins.asana.services import AsanaService as AsanaPlugin

__all__ = ["AsanaPlugin"]
