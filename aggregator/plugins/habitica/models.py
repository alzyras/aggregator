from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class HabiticaItem:
    item_id: str
    item_name: str
    item_type: str
    value: Optional[float]
    date_created: Optional[datetime]
    date_completed: Optional[datetime]
    notes: Optional[str]
    priority: Optional[float]
    tags: Optional[str]
    completed: bool
