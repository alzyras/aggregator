from aggregator.plugins.llm_summary.models import PluginSummary, TrendPoint
from aggregator.plugins.llm_summary.services import LlmSummaryService


class DummySettings:
    llm_summary = {
        "base_url": "http://localhost",
        "model": "dummy",
        "temperature": 0.1,
        "max_tokens": 10,
        "timeout": 1,
        "months": 12,
        "top_n": 3,
        "max_context_chars": 1000,
        "emerge_threshold_pct": 50,
        "decline_threshold_pct": 30,
    }


def test_detect_trends_emerging():
    svc = LlmSummaryService(DummySettings())
    summary = PluginSummary(
        plugin="asana",
        monthly=[TrendPoint(period="2024-02", value=20), TrendPoint(period="2024-01", value=10)],
    )
    trends = svc._detect_trends(summary)
    assert trends
    assert trends[0].direction == "emerging"


def test_detect_trends_declining():
    svc = LlmSummaryService(DummySettings())
    summary = PluginSummary(
        plugin="asana",
        monthly=[TrendPoint(period="2024-02", value=5), TrendPoint(period="2024-01", value=20)],
    )
    trends = svc._detect_trends(summary)
    assert trends
    assert trends[0].direction == "declining"
