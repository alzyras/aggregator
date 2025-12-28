def percent(value: float, denom: float, window: str) -> str:
    if denom == 0:
        return f"0% ({window})"
    pct = round((value / denom) * 100, 1)
    return f"{pct}% ({window})"


def minutes_str(total_minutes: float, window: str) -> str:
    hours = int(total_minutes // 60)
    minutes = int(total_minutes % 60)
    if hours > 0:
        return f"{hours}h {minutes}m ({window})"
    return f"{minutes}m ({window})"


def count_str(value: float, unit: str, window: str) -> str:
    return f"{int(value)} {unit} ({window})"
