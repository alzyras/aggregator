# LLM Progress

Generates a period-based progress report across all plugins using precomputed signals. Use it to review a period (e.g., last month) with clear sections.

## How to run
- Default period (last_month):  
  `python manage.py llm_progress --period last_month`
- Other periods:  
  `python manage.py llm_progress --period last_90_days`  
  `python manage.py llm_progress --period last_12_months`

## What it does
- Uses validated, windowed metrics (e.g., LAST_30 vs PRIOR_30) and coverage checks.
- LLM only narrates: Executive Summary, Current Focus & Trajectory, What’s Going Well, Recent Reality vs Baseline, Signals Worth Watching, Strategic Options, Data Confidence.

## Example output

1) **Executive Summary**  
Last month’s activity stayed concentrated on a few initiatives with steady completion. Momentum is stable; no major shifts versus the prior month.

2) **Current Focus & Trajectory**  
Two main themes account for most tracked effort; both are stable. No new themes are emerging.

3) **What’s Going Well**  
Low fragmentation, consistent completions on recurring tasks, and regular time tracking on core projects.

4) **Recent Reality vs Baseline**  
Recent behavior aligns with the prior 30-day baseline; no strong divergence.

5) **Signals Worth Watching**  
No strong emerging or declining signals this period.

6) **Strategic Options**  
- Maintain depth on the two dominant themes.  
- If desired, schedule a small exploration window to test a new theme.

7) **Data Confidence & Gaps**  
Confidence is medium: strong Toggl/Habitica coverage; Asana/Google Fit sparse.
