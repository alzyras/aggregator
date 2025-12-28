# LLM Summary

Generates a concise, data-grounded summary across all plugins using precomputed signals (no raw-table analysis by the LLM). Use it for ad-hoc questions like “What changed this week?” or “Any notable shifts?”.

## How to run
- Default:  
  `python manage.py llm_summary "How did I do this week?"`

## What it does
- Loads explicit, validated metrics (time-windowed) from all enabled sources.
- Packs a compact context; the LLM only narrates provided signals.
- Includes sections: Executive Summary, What’s going well, Current focus, Trends (if present), Streaks, Options, Data confidence.

## Example output

1) **Executive Summary**  
Activity remained stable over the last 30 days, concentrated in a few recurring themes. Momentum is neutral; no material swings detected. Coverage is adequate on Toggl and Habitica; other sources are sparse.

2) **What’s going well**  
Consistent time tracking on core projects. Habitica completions show steady weekly engagement. Fragmentation is low; work is focused.

3) **Current focus**  
Top effort is in two themes (Toggl projects), both seeing similar attention over the last month.

4) **Trends**  
No strong emerging or declining themes versus the prior 30 days.

5) **Streaks & consistency**  
Time-tracking streaks are short but regular; no extended breaks.

6) **Options**  
- Deepen the two dominant themes.  
- Reintroduce light exploration if breadth matters.

7) **Data confidence**  
Confidence is medium: Toggl/Habitica coverage is solid; other sources are limited.
