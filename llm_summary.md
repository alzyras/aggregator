# LLM Summary

Generates a concise, data-grounded summary across all plugins using precomputed signals (no raw-table analysis by the LLM). Use it for ad-hoc questions like "What changed this week?" or "Any notable shifts?".

## How to run
- Default:  
  `python manage.py llm_summary "How did I do this week?"`

## What it does
- Loads explicit, validated metrics (time-windowed) from all enabled sources.
- Packs a compact context; the LLM only narrates provided signals.
- Outputs up to 3 bullets tied only to the question-related subset; each bullet is a single concrete fact per source/metric; absences use "no recorded X"; no speculation, interpretation, or mixing of platforms in one bullet; omit streaks unless exact start/end dates are present.

## Example output

- Toggl: 42 sessions / 1,860 minutes in the last 30d; coverage on 18 of 30 days; longest streak 4 days.
- Asana: 23 tasks completed across 6 projects (last 30d); similar volume to prior 30d.
- Habitica: 31 completions; steady spread across the month; longest gap 3 days.
- Google Fit: No recent steps data recorded; health signals absent for this window.
