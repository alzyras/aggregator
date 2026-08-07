from __future__ import annotations

from typing import Any

from connectors.services import sanitize_error
from ingestion.providers import get_provider_spec


def verify_credentials(source: str, credentials: dict[str, Any]) -> tuple[bool, str]:
    spec = get_provider_spec(source)
    if not spec or spec.connection_verifier is None:
        return False, "Unsupported provider."
    ok, message = spec.connection_verifier(credentials)
    return ok, sanitize_error(message)
