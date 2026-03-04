from __future__ import annotations

from typing import Any

import requests


def verify_todoist(credentials: dict[str, Any]) -> tuple[bool, str]:
    token = credentials.get("api_token")
    if not token:
        return False, "Missing API token."
    try:
        response = requests.get(
            "https://api.todoist.com/rest/v2/projects",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
    except requests.RequestException:
        return False, "Could not reach Todoist API."

    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid Todoist token."
    return False, "Todoist verification failed."
