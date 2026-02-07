from __future__ import annotations

from typing import Any

import requests


def verify_google_fit(credentials: dict[str, Any]) -> tuple[bool, str]:
    access_token = credentials.get("access_token")
    if not access_token:
        return False, "Access token required for live verification."
    try:
        response = requests.get(
            "https://www.googleapis.com/fitness/v1/users/me/dataSources",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
    except requests.RequestException:
        return False, "Could not reach Google Fit API."

    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid Google Fit access token."
    return False, "Google Fit verification failed."
