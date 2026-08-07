from __future__ import annotations

from typing import Any

import requests

from providers.linear.api import LinearAPI


VIEWER_QUERY = "query Viewer { viewer { id name } }"


def verify_linear(credentials: dict[str, Any]) -> tuple[bool, str]:
    token = credentials.get("api_key")
    if not token:
        return False, "Missing Linear API key."
    try:
        data = LinearAPI(str(token)).request(VIEWER_QUERY)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status in {401, 403}:
            return False, "Invalid Linear API key or missing permissions."
        return False, "Linear verification failed."
    except requests.RequestException:
        return False, "Could not reach Linear."
    except ValueError:
        return False, "Linear rejected the verification request."
    if (data.get("viewer") or {}).get("id"):
        return True, "Connected."
    return False, "Linear verification failed."
