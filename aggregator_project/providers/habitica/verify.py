from __future__ import annotations

from typing import Any

import requests


def verify_habitica(credentials: dict[str, Any]) -> tuple[bool, str]:
    user_id = credentials.get("user_id")
    api_token = credentials.get("api_token")
    if not user_id or not api_token:
        return False, "Missing Habitica user id or API token."
    try:
        response = requests.get(
            "https://habitica.com/api/v3/user",
            headers={
                "x-api-user": user_id,
                "x-api-key": api_token,
                "x-client": f"aggregator-{user_id}",
            },
            timeout=10,
        )
    except requests.RequestException:
        return False, "Could not reach Habitica API."

    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid Habitica credentials."
    return False, "Habitica verification failed."
