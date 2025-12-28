"""Backward-compatible alias to the new service implementation."""

from aggregator.plugins.google_fit.services import GoogleFitService as GoogleFitPlugin

__all__ = ["GoogleFitPlugin"]
