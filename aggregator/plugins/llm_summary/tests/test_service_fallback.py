from datetime import date

from aggregator.plugins.llm_summary.models import ContextPayload, PluginSummary
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
        "max_context_chars": 8000,
        "emerge_threshold_pct": 50,
        "decline_threshold_pct": 30,
    }


def test_fallback_summary_includes_plugin():
    svc = LlmSummaryService(DummySettings())
    payload = ContextPayload(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        window_months=12,
        summaries=[PluginSummary(plugin="asana")],
    )
    text = svc._fallback_summary(payload)
    assert "asana" in text
