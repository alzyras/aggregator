# LLM Focus Analysis

Ask the system: “How am I doing on <topic>?” and get a focused, cross-platform summary. It interprets your topic, finds related activity across Asana, Toggl, Habitica, and Google Fit, and narrates only pre-computed signals (no raw-table analysis by the LLM).

## How to run
- Default window (last_90_days):  
  `python manage.py llm_focus "learning Portuguese"`
- Other windows:  
  `python manage.py llm_focus "programming" last_month`  
  `python manage.py llm_focus "client deadlines" last_12_months`

## What it does
- Interprets your query into concepts/keywords (query text only; no activity data sent).
- Searches Asana (projects/tasks/descriptions), Toggl (projects/clients/descriptions), Habitica (items/notes/tags), and Google Fit (data_type/source) for matches.
- Computes presence, volume, consistency, streaks, and momentum for matched activity over explicit windows.
- Builds a compact, structured context; the LLM only narrates those signals.

## Privacy
- Only the query text is sent to the LLM for intent interpretation; activity data remains local and only summarized context is shared with the local LLM endpoint.

## Example output

How you’re doing on: **programming**

1) **Overall Assessment**  
The user’s activity in programming has been consistently paused for the last observed period. While engagement is present, it appears to be inactive rather than actively progressing toward new goals.

2) **Evidence Across Platforms (only matched sources)**  
The data comes from two platforms: *Toggl* and *Habitica*.  
On Toggl, there’s no recent burst of time spent, with activity averaging low over the past month.  
Habitica shows brief engagement in completed tasks but lacks sustained daily interaction or visible progress.

3) **Momentum & Consistency**  
Both tools indicate a pause in momentum—no new projects or active development appear to be underway. Activity is limited, with a low ratio of active days (13% on Toggl, 33% on Habitica) and no streaks observed.

4) **Strengths**  
The user has demonstrated some consistent engagement by tracking time spent on programming-related tasks across platforms.  
Where activity exists, it appears to be focused rather than scattered, though clustered in brief bursts.

5) **Gaps or Risks (only if relevant)**  
No direct evidence links the user to *Asana* or *Google Fit*, so potential project management or fitness habits tied to programming cannot be assessed here. The lack of recent streaks on Toggl and Habitica suggests possible gaps in sustained effort.

6) **Next-Step Options**  
- **Reintroduce focus:** If the user wants to revisit programming, they could dedicate a short window (e.g., 1–2 weeks) to re-establishing daily practice.  
- **Explore new tools:** Since *Asana* and *Google Fit* show no activity, integrating tracking from those might help bridge gaps in workflow or motivation.

7) **Confidence & Data Limits**  
This summary is based on limited data from two platforms. Confidence is moderate due to insufficient coverage of other tools like *Asana* or *Google Fit*, which could provide additional context about the user’s broader habits and progress.
