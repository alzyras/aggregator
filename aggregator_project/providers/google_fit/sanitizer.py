from __future__ import annotations

from typing import Any


KEEP_KEYS = {
    "id",
    "startTimeMillis",
    "endTimeMillis",
    "modifiedTimeMillis",
    "dataTypeName",
    "originDataSourceId",
    "value",
}


def sanitize_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
