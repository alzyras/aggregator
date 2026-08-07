from __future__ import annotations

from typing import Any

import requests


GRAPHQL_URL = "https://api.linear.app/graphql"


class LinearAPI:
    def __init__(self, token: str, session: requests.Session | None = None) -> None:
        self.token = token
        self.session = session or requests.Session()

    def request(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        response = self.session.post(
            GRAPHQL_URL,
            headers={
                "Authorization": self.token,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables or {}},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json() or {}
        errors = payload.get("errors")
        if errors:
            messages = [
                str(error.get("message") or "Linear API error")
                for error in errors
                if isinstance(error, dict)
            ]
            raise ValueError("; ".join(messages) or "Linear API error.")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("Linear response did not contain data.")
        return data
