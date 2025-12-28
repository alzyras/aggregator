from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class TrendPoint:
    period: str
    value: float


@dataclass
class CategoryTrend:
    name: str
    source: str
    last_value: float
    prior_value: float
    change_pct: float
    direction: str  # emerging | declining | steady


@dataclass
class PluginSummary:
    plugin: str
    monthly: List[TrendPoint] = field(default_factory=list)
    recent_30d: Dict[str, Any] = field(default_factory=dict)
    recent_90d: Dict[str, Any] = field(default_factory=dict)
    categories: List[Dict[str, Any]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class ContextPayload:
    start_date: date
    end_date: date
    window_months: int
    summaries: List[PluginSummary] = field(default_factory=list)
    emerging: List[CategoryTrend] = field(default_factory=list)
    declining: List[CategoryTrend] = field(default_factory=list)
    anomalies: List[str] = field(default_factory=list)
    data_gaps: List[str] = field(default_factory=list)


@dataclass
class ChatMessage:
    role: str
    content: str


@dataclass
class ChatRequest:
    messages: List[ChatMessage]
    model: str
    temperature: float
    max_tokens: int


@dataclass
class ChatResponse:
    content: str
    raw: Dict[str, Any]
