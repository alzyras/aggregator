from __future__ import annotations

import requests

from providers.trello.client import BASE_URL


def verify_trello(credentials: dict) -> tuple[bool, str]:
    api_key = credentials.get("api_key")
    api_token = credentials.get("api_token")
    if not api_key:
        return False, "Missing Trello API key."
    if not api_token:
        return False, "Missing Trello API token."
    try:
        response = requests.get(
            f"{BASE_URL}/members/me",
            params={"key": str(api_key), "token": str(api_token), "fields": "id"},
            timeout=10,
        )
    except requests.RequestException:
        return False, "Could not reach Trello."
    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid Trello credentials or missing permissions."
    return False, "Trello verification failed."
