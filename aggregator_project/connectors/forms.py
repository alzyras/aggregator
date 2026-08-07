"""Compatibility imports for callers that predate provider-owned forms."""

from providers.asana.forms import AsanaConnectForm
from providers.google_fit.forms import GoogleFitConnectForm
from providers.habitica.forms import HabiticaConnectForm


__all__ = ["AsanaConnectForm", "GoogleFitConnectForm", "HabiticaConnectForm"]
