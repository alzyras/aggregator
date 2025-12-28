from datetime import date

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


def test_date_range_last_month():
    svc = LlmSummaryService(DummySettings())
    start, end = svc._date_range("last_month")
    assert (end - start).days in (29, 30, 31)
