from __future__ import annotations

from typing import Any

import requests


def verify_asana(credentials: dict[str, Any]) -> tuple[bool, str]:
    token = credentials.get("access_token")
    if not token:
        return False, "Missing access token."
    try:
        response = requests.get(
            "https://app.asana.com/api/1.0/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
    except requests.RequestException:
        return False, "Could not reach Asana API."

    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid Asana token."
    return False, "Asana verification failed."
