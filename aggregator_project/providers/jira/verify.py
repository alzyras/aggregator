from __future__ import annotations

from typing import Any

import requests
from requests.auth import HTTPBasicAuth


def verify_jira(credentials: dict[str, Any]) -> tuple[bool, str]:
    base_url = (credentials.get("base_url") or "").rstrip("/")
    auth_method = credentials.get("auth_method")
    deployment = credentials.get("deployment_type") or "cloud"
    if not base_url:
        return False, "Missing Jira base URL."

    api_version = "3" if deployment == "cloud" else "2"
    url = f"{base_url}/rest/api/{api_version}/myself"
    headers: dict[str, str] = {"Accept": "application/json"}
    auth: HTTPBasicAuth | None = None

    if auth_method == "cloud_api_token":
        email = credentials.get("email")
        token = credentials.get("api_token")
        if not email or not token:
            return False, "Missing Jira Cloud email or API token."
        auth = HTTPBasicAuth(email, token)
    elif auth_method == "personal_access_token":
        pat = credentials.get("pat_token")
        if not pat:
            return False, "Missing Jira PAT token."
        headers["Authorization"] = f"Bearer {pat}"
    elif auth_method == "oauth2":
        return False, "OAuth2 verification is not enabled in this build."
    else:
        return False, "Unsupported Jira auth method."

    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=15)
    except requests.exceptions.SSLError:
        return False, "SSL verification failed. Check Jira base URL certificate."
    except requests.exceptions.ConnectionError:
        return False, "Could not reach Jira. Check base URL and DNS."
    except requests.RequestException:
        return False, "Could not reach Jira API."

    if response.status_code == 200:
        return True, "Connected."
    if response.status_code in {401, 403}:
        return False, "Invalid Jira credentials or insufficient permissions."
    if response.status_code == 429:
        return False, "Jira rate limited verification. Try again shortly."
    return False, f"Jira verification failed (HTTP {response.status_code})."

