from datetime import date

from aggregator.plugins.llm_summary.models import ContextPayload, PluginSummary, TrendPoint
from aggregator.plugins.llm_summary.services import LlmSummaryService


class DummySettings:
    llm_summary = {
        "base_url": "http://localhost",
        "model": "dummy",
        "temperature": 0.1,
        "max_tokens": 10,
        "timeout": 1,
        "months": 12,
        "top_n": 2,
        "max_context_chars": 80,
        "emerge_threshold_pct": 50,
        "decline_threshold_pct": 30,
    }


def test_compact_context_truncates():
    svc = LlmSummaryService(DummySettings())
    payload = ContextPayload(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        window_months=12,
        summaries=[
            PluginSummary(
                plugin="asana",
                monthly=[TrendPoint(period="2024-01", value=1)] * 5,
                categories=[{"category": "a", "items": 1}, {"category": "b", "items": 2}],
            )
        ],
    )
    context = svc._compact_context(payload, top_n=2)
    assert len(context) <= svc.max_context_chars + 10  # allow suffix
