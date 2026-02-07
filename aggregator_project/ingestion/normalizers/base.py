from __future__ import annotations

import hashlib
from typing import Any


def build_dedupe_hash(payload: dict[str, Any]) -> str:
    raw = "|".join(
        str(payload.get(key, ""))
        for key in [
            "source",
            "source_entity_type",
            "source_entity_id",
            "start_time",
            "end_time",
            "metric_type",
            "metric_value",
            "status",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
