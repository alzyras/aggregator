from __future__ import annotations

from typing import Any


def repository_name(issue: dict[str, Any], fallback: str = "") -> str:
    if fallback:
        return fallback.strip().strip("/")
    explicit = issue.get("__github_repository")
    if explicit:
        return str(explicit).strip().strip("/")
    repository = issue.get("repository")
    if isinstance(repository, dict) and repository.get("full_name"):
        return str(repository["full_name"]).strip().strip("/")
    for key in ("repository_url", "url"):
        value = str(issue.get(key) or "")
        marker = "/repos/"
        if marker not in value:
            continue
        suffix = value.split(marker, 1)[1]
        parts = [part for part in suffix.split("/") if part]
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
    return ""


def issue_identity(issue: dict[str, Any]) -> str:
    repository = repository_name(issue)
    number = issue.get("number")
    if not repository or number in (None, ""):
        return ""
    return f"{repository}#{number}"


def parse_issue_identity(value: str) -> tuple[str, int]:
    repository, separator, raw_number = str(value or "").rpartition("#")
    if not separator or "/" not in repository:
        raise ValueError("Invalid GitHub issue identity.")
    try:
        number = int(raw_number)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid GitHub issue number.") from exc
    if number <= 0:
        raise ValueError("Invalid GitHub issue number.")
    return repository.strip("/"), number
