from __future__ import annotations

from typing import Any, Callable

from connectors.services import sanitize_error
from providers.asana.verify import verify_asana
from providers.google_fit.verify import verify_google_fit
from providers.habitica.verify import verify_habitica
from providers.todoist.verify import verify_todoist

Verifier = Callable[[dict[str, Any]], tuple[bool, str]]


VERIFY_MAP: dict[str, Verifier] = {
    "asana": verify_asana,
    "todoist": verify_todoist,
    "habitica": verify_habitica,
    "google_fit": verify_google_fit,
}


def verify_credentials(source: str, credentials: dict[str, Any]) -> tuple[bool, str]:
    verifier = VERIFY_MAP.get(source)
    if not verifier:
        return False, "Unsupported provider."
    ok, message = verifier(credentials)
    return ok, sanitize_error(message)
