from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StepsRecord:
    id: str
    user_id: str
    timestamp: datetime
    steps: float


@dataclass
class HeartRateRecord:
    id: str
    user_id: str
    timestamp: datetime
    heart_rate: float


@dataclass
class GeneralRecord:
    id: str
    user_id: str
    data_type: str
    value: float
    unit: str
    date: Optional[datetime] = None
