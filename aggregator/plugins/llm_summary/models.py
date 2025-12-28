from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional
from enum import Enum


class Window(str, Enum):
    LAST_7_DAYS = "LAST_7_DAYS"
    LAST_30_DAYS = "LAST_30_DAYS"
    LAST_90_DAYS = "LAST_90_DAYS"
    PRIOR_30_DAYS = "PRIOR_30_DAYS"
    PRIOR_90_DAYS = "PRIOR_90_DAYS"
    LAST_12_MONTHS = "LAST_12_MONTHS"


@dataclass
class Metric:
    name: str
    source: str
    window: Window
    value: float
    unit: str
    coverage_days: int
    confidence: str = "medium"


@dataclass
class QueryIntent:
    primary_concepts: List[str]
    related_keywords: List[str]
    synonyms: List[str]
    excluded_terms: List[str]
    confidence: str = "medium"


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
