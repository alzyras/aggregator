from __future__ import annotations

import requests

from providers.clickup.client import BASE_URL


def verify_clickup(credentials: dict) -> tuple[bool, str]:
    token = credentials.get("api_token")
    if not token:
        return False, "Missing ClickUp API token."
    try:
        response = requests.get(
            f"{BASE_URL}/team",
            headers={"Authorization": str(token)},
            timeout=10,
        )
    except requests.RequestException:
        return False, "Could not reach ClickUp."
    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid ClickUp API token or missing permissions."
    return False, "ClickUp verification failed."
