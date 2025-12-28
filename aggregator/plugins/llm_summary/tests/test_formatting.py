from aggregator.plugins.llm_summary.formatting import percent, minutes_str, count_str


def test_percent():
    assert percent(50, 200, "last 30d") == "25.0% (last 30d)"


def test_minutes_str():
    assert minutes_str(125, "last week") == "2h 5m (last week)"


def test_count_str():
    assert count_str(12, "tasks", "last 7d") == "12 tasks (last 7d)"
