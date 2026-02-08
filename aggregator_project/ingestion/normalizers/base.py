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
            "event_type",
            "source_event_version",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
