from __future__ import annotations

from typing import Any

import requests

from providers.github_issues.client import API_HEADERS, BASE_URL


def verify_github(credentials: dict[str, Any]) -> tuple[bool, str]:
    token = credentials.get("api_token")
    if not token:
        return False, "Missing GitHub personal access token."
    try:
        response = requests.get(
            f"{BASE_URL}/user",
            headers={**API_HEADERS, "Authorization": f"Bearer {token}"},
            timeout=10,
        )
    except requests.RequestException:
        return False, "Could not reach GitHub."
    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid GitHub token or missing permissions."
    return False, "GitHub verification failed."
