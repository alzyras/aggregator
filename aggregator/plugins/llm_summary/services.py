import json
import logging
import math
import statistics
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Tuple, Optional

import requests

from aggregator.core.apps import PluginService
from aggregator.plugins.llm_summary.models import (
    CategoryTrend,
    ChatMessage,
    ChatRequest,
    ChatResponse,
    ContextPayload,
    PluginSummary,
    TrendPoint,
    Metric,
    Window,
)
from aggregator.plugins.llm_summary.repositories import LlmSummaryRepository
from aggregator.settings import settings
from aggregator.plugins.llm_summary.formatting import percent, minutes_str, count_str

logger = logging.getLogger(__name__)

STOPWORDS = {
    "the",
    "and",
    "of",
    "to",
    "a",
    "in",
    "for",
    "with",
    "on",
    "at",
    "by",
    "an",
    "is",
    "it",
    "from",
    "task",
    "project",
    "habit",
    "todo",
}


class LlmSummaryService(PluginService):
    name = "llm_summary"

    def __init__(self, project_settings=None) -> None:
        self.settings = project_settings or settings
        self.repo = LlmSummaryRepository()
        llm = self.settings.llm_summary
        self.base_url = llm["base_url"]
        self.model = llm["model"]
        self.temperature = llm["temperature"]
        self.max_tokens = llm["max_tokens"]
        self.timeout = llm["timeout"]
        self.months = llm["months"]
        self.top_n = llm["top_n"]
        self.max_context_chars = min(llm["max_context_chars"], 6000)
        self.emerge_threshold_pct = llm["emerge_threshold_pct"]
        self.decline_threshold_pct = llm["decline_threshold_pct"]

    def setup(self) -> bool:
        return True

    def fetch_data(self):
        return None

    def write_data(self, payload) -> Tuple[int, int]:
        return (0, 0)

    def build_context(
        self, start_date: date, end_date: date, months: int | None = None, top_n: int | None = None
    ) -> Tuple[str, ContextPayload]:
        window_months = months or self.months
        top_n = top_n or self.top_n
        windows = self._window_ranges(end_date)
        metrics: List[Metric] = []
        caveats: List[str] = []
        source_snapshots: Dict[str, Dict[str, Any]] = {}

        # Collect per-source metrics for canonical windows
        # Asana
        asana_30 = self.repo.asana_totals(windows["LAST_30_DAYS"][0], windows["LAST_30_DAYS"][1])
        asana_prior_30 = self.repo.asana_totals(windows["PRIOR_30_DAYS"][0], windows["PRIOR_30_DAYS"][1])
        if asana_30:
            metrics.append(Metric("tasks_completed", "asana", Window.LAST_30_DAYS, float(asana_30.get("completed", 0) or 0), "count", int(asana_30.get("coverage_days", 0)), "high"))
        if asana_prior_30:
            metrics.append(Metric("tasks_completed", "asana", Window.PRIOR_30_DAYS, float(asana_prior_30.get("completed", 0) or 0), "count", int(asana_prior_30.get("coverage_days", 0)), "medium"))

        # Toggl
        toggl_30 = self.repo.toggl_totals(windows["LAST_30_DAYS"][0], windows["LAST_30_DAYS"][1])
        toggl_prior_30 = self.repo.toggl_totals(windows["PRIOR_30_DAYS"][0], windows["PRIOR_30_DAYS"][1])
        if toggl_30:
            metrics.append(Metric("minutes_tracked", "toggl", Window.LAST_30_DAYS, float(toggl_30.get("minutes") or 0), "minutes", int(toggl_30.get("coverage_days", 0)), "high"))
        if toggl_prior_30:
            metrics.append(Metric("minutes_tracked", "toggl", Window.PRIOR_30_DAYS, float(toggl_prior_30.get("minutes") or 0), "minutes", int(toggl_prior_30.get("coverage_days", 0)), "medium"))
        toggl_90 = self.repo.toggl_totals(windows["LAST_90_DAYS"][0], windows["LAST_90_DAYS"][1])
        toggl_prior_90 = self.repo.toggl_totals(windows["PRIOR_90_DAYS"][0], windows["PRIOR_90_DAYS"][1])
        if toggl_90:
            metrics.append(Metric("minutes_tracked", "toggl", Window.LAST_90_DAYS, float(toggl_90.get("minutes") or 0), "minutes", int(toggl_90.get("coverage_days", 0)), "high"))
        if toggl_prior_90:
            metrics.append(Metric("minutes_tracked", "toggl", Window.PRIOR_90_DAYS, float(toggl_prior_90.get("minutes") or 0), "minutes", int(toggl_prior_90.get("coverage_days", 0)), "medium"))

        # Habitica
        habitica_30 = self.repo.habitica_totals(windows["LAST_30_DAYS"][0], windows["LAST_30_DAYS"][1])
        habitica_prior_30 = self.repo.habitica_totals(windows["PRIOR_30_DAYS"][0], windows["PRIOR_30_DAYS"][1])
        if habitica_30:
            metrics.append(Metric("completions", "habitica", Window.LAST_30_DAYS, float(habitica_30.get("completions") or 0), "count", int(habitica_30.get("coverage_days", 0)), "high"))
        if habitica_prior_30:
            metrics.append(Metric("completions", "habitica", Window.PRIOR_30_DAYS, float(habitica_prior_30.get("completions") or 0), "count", int(habitica_prior_30.get("coverage_days", 0)), "medium"))
        habitica_90 = self.repo.habitica_totals(windows["LAST_90_DAYS"][0], windows["LAST_90_DAYS"][1])
        habitica_prior_90 = self.repo.habitica_totals(windows["PRIOR_90_DAYS"][0], windows["PRIOR_90_DAYS"][1])
        if habitica_90:
            metrics.append(Metric("completions", "habitica", Window.LAST_90_DAYS, float(habitica_90.get("completions") or 0), "count", int(habitica_90.get("coverage_days", 0)), "high"))
        if habitica_prior_90:
            metrics.append(Metric("completions", "habitica", Window.PRIOR_90_DAYS, float(habitica_prior_90.get("completions") or 0), "count", int(habitica_prior_90.get("coverage_days", 0)), "medium"))

        # Google Fit
        fit_30 = self.repo.google_fit_totals(windows["LAST_30_DAYS"][0], windows["LAST_30_DAYS"][1])
        fit_prior_30 = self.repo.google_fit_totals(windows["PRIOR_30_DAYS"][0], windows["PRIOR_30_DAYS"][1])
        if fit_30:
            metrics.append(Metric("steps", "google_fit", Window.LAST_30_DAYS, float(fit_30.get("steps") or 0), "steps", int(fit_30.get("coverage_days", 0)), "high"))
        if fit_prior_30:
            metrics.append(Metric("steps", "google_fit", Window.PRIOR_30_DAYS, float(fit_prior_30.get("steps") or 0), "steps", int(fit_prior_30.get("coverage_days", 0)), "medium"))
        fit_90 = self.repo.google_fit_totals(windows["LAST_90_DAYS"][0], windows["LAST_90_DAYS"][1])
        fit_prior_90 = self.repo.google_fit_totals(windows["PRIOR_90_DAYS"][0], windows["PRIOR_90_DAYS"][1])
        if fit_90:
            metrics.append(Metric("steps", "google_fit", Window.LAST_90_DAYS, float(fit_90.get("steps") or 0), "steps", int(fit_90.get("coverage_days", 0)), "high"))
        if fit_prior_90:
            metrics.append(Metric("steps", "google_fit", Window.PRIOR_90_DAYS, float(fit_prior_90.get("steps") or 0), "steps", int(fit_prior_90.get("coverage_days", 0)), "medium"))

        metrics, caveat_metrics = self._validate_metrics(metrics)
        caveats.extend(caveat_metrics)

        # Derive per-source presence/consistency/streaks
        for source in ["asana", "toggl", "habitica", "google_fit"]:
            series_30 = self._daily_series(source, windows["LAST_30_DAYS"])
            series_90 = self._daily_series(source, windows["LAST_90_DAYS"])
            snapshot = {
                "presence": self._presence(series_30, window_days=30),
                "consistency": self._consistency(series_30, window_days=30),
                "streaks": self._streak_from_series(series_30),
                "best": self._best_from_series(series_30, windows["LAST_30_DAYS"]),
                "momentum": self._momentum(series_30, series_90),
            }
            source_snapshots[source] = snapshot

        # Build minimal summaries for compatibility
        summaries: List[PluginSummary] = []
        for source in ["asana", "toggl", "habitica", "google_fit"]:
            summaries.append(PluginSummary(plugin=source))

        payload = ContextPayload(
            start_date=start_date,
            end_date=end_date,
            window_months=window_months,
            summaries=summaries,
            emerging=[],
            declining=[],
            anomalies=[],
            data_gaps=caveats,
        )

        context_text = self._context_from_metrics(metrics, caveats, source_snapshots)
        return context_text, payload

    def generate_progress_summary(self, period: str = "last_month") -> str:
        start_date, end_date = self._date_range(period)
        context_text, payload = self.build_context(start_date, end_date)
        prompt = (
            "Produce a concise, data-grounded progress summary.\n"
            "Use this exact section order:\n"
            "1) Executive Summary (5–7 bullets, no fluff)\n"
            "2) Current Focus & Trajectory (top relevant themes only; label lifecycle: Active/Consolidated/Paused/Abandoned; trajectory: Rising/Stable/Fading)\n"
            "3) What’s Going Well (stable, low-friction patterns worth protecting)\n"
            "4) Recent Reality vs Baseline (recency-weighted; highlight divergence or say no strong divergence)\n"
            "5) Signals Worth Watching (emerging/declining; only if real growth; else say no strong signal)\n"
            "6) Strategic Options (2–3 choices tied to themes/trends; frame as options, not advice)\n"
            "7) Data Confidence & Caveats (what was strong, what was thin, what was omitted as low relevance)\n"
            "Rules: do NOT hallucinate. Cite sources (Asana, Toggl, Habitica, Google Fit). "
            "Inactivity is not failure; paused/abandoned are neutral. Do not highlight noise. "
            "Analyst tone; short, precise sentences; no prescriptions."
        )
        try:
            return self._ask_llm(context_text, prompt)
        except Exception as exc:
            logger.error("LLM call failed: %s", exc, exc_info=True)
            return self._fallback_summary(payload)

    def chat(self, question: str, period: str = "last_12_months") -> str:
        start_date, end_date = self._date_range(period)
        context_text, payload = self.build_context(start_date, end_date)
        prompt = question
        try:
            return self._ask_llm(context_text, prompt)
        except Exception as exc:
            logger.error("LLM call failed: %s", exc, exc_info=True)
            return self._fallback_summary(payload)

    def _build_plugin_summary(
        self,
        plugin: str,
        monthly_file: str,
        category_file: str,
        params: Dict[str, Any],
        top_n: int,
    ) -> PluginSummary:
        monthly = self.repo.get_monthly_summary(monthly_file, params)
        categories = self.repo.get_categories(category_file, {**params, "limit_rows": top_n * 2})
        monthly_points = [
            TrendPoint(
                period=row["period"],
                value=float(
                    row.get("entry_count")
                    or row.get("completed_tasks")
                    or row.get("steps_total")
                    or row.get("completed_items")
                    or 0
                ),
            )
            for row in monthly
        ]

        recent_30d, recent_90d = {}, {}
        if monthly:
            recent_30d = monthly[0]
            if len(monthly) > 1:
                recent_90d = monthly[1]

        return PluginSummary(
            plugin=plugin,
            monthly=monthly_points,
            recent_30d=recent_30d,
            recent_90d=recent_90d,
            categories=categories[:top_n],
            notes=[],
        )

    def _detect_trends(self, summary: PluginSummary) -> List[CategoryTrend]:
        trends: List[CategoryTrend] = []
        monthly = summary.monthly
        if len(monthly) < 2:
            return trends

        last = monthly[0].value
        prior = monthly[1].value
        if prior == 0 and last == 0:
            return trends
        change_pct = ((last - prior) / prior * 100) if prior else 100.0
        direction = "steady"
        if change_pct >= self.emerge_threshold_pct:
            direction = "emerging"
        elif change_pct <= -self.decline_threshold_pct:
            direction = "declining"

        trends.append(
            CategoryTrend(
                name=f"{summary.plugin}_overall",
                source=summary.plugin,
                last_value=last,
                prior_value=prior,
                change_pct=round(change_pct, 1),
                direction=direction,
            )
        )
        return trends

    def _normalize_tokens(self, text_val: str) -> List[str]:
        tokens = []
        for raw in text_val.replace("/", " ").replace("-", " ").replace("_", " ").split():
            t = raw.strip().lower()
            if not t or t in STOPWORDS:
                continue
            tokens.append(t)
        return tokens

    def _derive_themes(self, summaries: List[PluginSummary], top_n: int) -> List[Dict[str, Any]]:
        # Collect category names across plugins
        candidates = []
        plugin_recency = {s.plugin: self._recent_activity_factor(s) for s in summaries}
        for summary in summaries:
            for cat in summary.categories:
                name = str(cat.get("category") or cat.get("project") or cat.get("data_type") or "uncategorized")
                value = float(cat.get("items") or cat.get("total_minutes") or cat.get("records") or 0)
                candidates.append(
                    {
                        "name": name,
                        "value": value,
                        "source": summary.plugin,
                        "recency": plugin_recency.get(summary.plugin, 0.5),
                    }
                )

        # Simple token-based clustering
        clusters: List[Dict[str, Any]] = []
        for cand in sorted(candidates, key=lambda c: c["value"], reverse=True):
            tokens = set(self._normalize_tokens(cand["name"]))
            if not tokens:
                tokens = {cand["name"].lower()}
            placed = False
            for cluster in clusters:
                overlap = len(tokens & cluster["tokens"])
                union = len(tokens | cluster["tokens"])
                jaccard = overlap / union if union else 0
                if jaccard >= 0.3:
                    cluster["tokens"] |= tokens
                    cluster["total"] += cand["value"]
                    cluster["sources"].add(cand["source"])
                    cluster["names"].append(cand["name"])
                    cluster["recency"] = max(cluster["recency"], cand["recency"])
                    placed = True
                    break
            if not placed:
                clusters.append(
                    {
                        "tokens": tokens,
                        "total": cand["value"],
                        "sources": {cand["source"]},
                        "names": [cand["name"]],
                        "recency": cand["recency"],
                    }
                )

        themes = []
        total_value = sum(c["total"] for c in clusters) or 1
        for cluster in sorted(clusters, key=lambda c: c["total"], reverse=True):
            share = cluster["total"] / total_value
            relevance = self._relevance_score(share, cluster["recency"], consistency_weight=1.0)
            lifecycle, trajectory = self._classify_theme(share, cluster["recency"])
            if lifecycle == "noise":
                continue
            label = " / ".join(sorted(list(cluster["tokens"]))[:3])
            themes.append(
                {
                    "theme": label,
                    "share_pct": round(share * 100, 1),
                    "sources": sorted(list(cluster["sources"])),
                    "examples": cluster["names"][:3],
                    "relevance": round(relevance, 3),
                    "lifecycle": lifecycle,
                    "trajectory": trajectory,
                }
            )
        return sorted(themes, key=lambda t: t["relevance"], reverse=True)[:top_n]

    def _relevance_score(self, share: float, recency_weight: float, consistency_weight: float) -> float:
        # Favor recent and meaningful share; consistency weight currently neutral (1.0)
        return recency_weight * max(share, 0.01) * consistency_weight

    def _classify_theme(self, share: float, recency_weight: float) -> Tuple[str, str]:
        # lifecycle: active / consolidated / paused / abandoned / noise
        # trajectory: rising / stable / fading (based on recency)
        if share < 0.02:
            return "noise", "stable"
        if recency_weight >= 0.3:
            lifecycle = "active"
        elif recency_weight >= 0.1:
            lifecycle = "paused"
        elif recency_weight >= 0.05 and share >= 0.1:
            lifecycle = "consolidated"
        elif recency_weight < 0.05 and share >= 0.05:
            lifecycle = "abandoned"
        else:
            lifecycle = "noise"

        if recency_weight >= 0.25:
            trajectory = "rising"
        elif recency_weight <= 0.05:
            trajectory = "fading"
        else:
            trajectory = "stable"
        return lifecycle, trajectory

    def _recent_activity_factor(self, summary: PluginSummary) -> float:
        # Ratio of latest month to average of others
        if not summary.monthly:
            return 0.0
        last = summary.monthly[0].value
        prior_values = [p.value for p in summary.monthly[1:]] or [0]
        prior_avg = sum(prior_values) / len(prior_values)
        if prior_avg == 0:
            return 1.0 if last > 0 else 0.0
        return min(1.0, last / (prior_avg * 2))

    def _correlate_health_productivity(self, summaries: List[PluginSummary]) -> List[Dict[str, Any]]:
        def extract_series(plugin: str) -> List[Tuple[str, float]]:
            for s in summaries:
                if s.plugin == plugin:
                    return [(p.period, p.value) for p in s.monthly if p.value is not None]
            return []

        steps = extract_series("google_fit")
        tasks = extract_series("asana")
        time_entries = extract_series("toggl")

        correlations = []
        if steps and tasks:
            corr = self._pearson(steps, tasks)
            if corr is not None:
                correlations.append({"pair": "steps_vs_tasks", "r": corr, "source": ["google_fit", "asana"]})
        if steps and time_entries:
            corr = self._pearson(steps, time_entries)
            if corr is not None:
                correlations.append({"pair": "steps_vs_time", "r": corr, "source": ["google_fit", "toggl"]})
        return correlations

    def _pearson(self, a: List[Tuple[str, float]], b: List[Tuple[str, float]]) -> float | None:
        # align by period key
        b_map = {k: v for k, v in b}
        xs, ys = [], []
        for k, v in a:
            if k in b_map:
                xs.append(v)
                ys.append(b_map[k])
        n = len(xs)
        if n < 3:
            return None
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        den = math.sqrt(sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys))
        if den == 0:
            return None
        return round(num / den, 3)

    def _compact_context(
        self,
        payload: ContextPayload,
        top_n: int,
        themes: List[Dict[str, Any]],
        correlations: List[Dict[str, Any]],
        streaks: Dict[str, Any],
        coverage: List[Dict[str, Any]],
        best: Dict[str, Any],
    ) -> str:
        def clamp(text: str) -> str:
            if len(text) <= self.max_context_chars:
                return text
            return text[: self.max_context_chars] + "... (truncated)"

        summaries_block = []
        for summary in payload.summaries:
            cat_display = summary.categories[:top_n]
            summaries_block.append(
                {
                    "plugin": summary.plugin,
                    "monthly": [{tp.period: tp.value} for tp in summary.monthly[: payload.window_months]],
                    "categories": cat_display,
                    "recent_30d": summary.recent_30d,
                    "recent_90d": summary.recent_90d,
                }
            )

        context = {
            "date_range": {"start": str(payload.start_date), "end": str(payload.end_date)},
            "summaries": summaries_block,
            "themes": themes,
            "emerging": [trend.__dict__ for trend in payload.emerging],
            "declining": [trend.__dict__ for trend in payload.declining],
            "correlations": correlations,
            "streaks": streaks,
            "coverage": coverage,
            "highlights": best,
            "anomalies": payload.anomalies,
            "data_gaps": payload.data_gaps,
            "source_notes": "All numbers are read-only rollups from plugin tables; source keys match plugin names. Only top relevant themes are surfaced; noise omitted.",
        }

        def _encode(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            return str(obj)

        context_text = json.dumps(context, ensure_ascii=False, default=_encode)
        return clamp(context_text)

    def _ask_llm(self, context_text: str, prompt: str) -> str:
        messages = [
            ChatMessage(
                role="system",
                content=(
                    "You are a reporting layer, not an analyst. Do NOT infer trends beyond the provided context.\n"
                    "Only narrate the context; do not think aloud. If information is missing, say so plainly.\n"
                    "Do not repeat metrics across sections. Tone: senior analyst, calm, supportive.\n"
                ),
            ),
            ChatMessage(
                role="user",
                content=(
                    f"Context:\n{context_text}\n\n"
                    "Output format (fixed):\n"
                    "Output structure (each ≤5 sentences):\n"
                    "1) Executive Summary (3–5 bullets, insight-driven)\n"
                    "2) Activity by Source (short paragraph per source)\n"
                    "3) Momentum & Phase Interpretation\n"
                    "4) What’s Going Well\n"
                    "5) What Changed Recently\n"
                    "6) Strategic Options (2–3, as choices, not prescriptions)\n"
                    "7) Data Confidence & Gaps\n"
                    "Never analyze raw metrics; only narrate explicit signals. Every number must include unit and window.\n"
                    f"Question: {prompt}"
                ),
            ),
        ]
        req = ChatRequest(
            messages=messages,
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        payload = {
            "model": req.model,
            "messages": [m.__dict__ for m in req.messages],
            "temperature": req.temperature,
            "max_tokens": req.max_tokens,
            "stream": False,
        }
        start = time.time()
        resp = requests.post(self.base_url, json=payload, timeout=self.timeout)
        elapsed = time.time() - start
        resp.raise_for_status()
        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        logger.info("LLM call complete (%.2fs)", elapsed)
        return ChatResponse(content=content, raw=data).content

    def _fallback_summary(self, payload: ContextPayload) -> str:
        lines = ["LLM unavailable; local fallback summary."]
        for summary in payload.summaries:
            lines.append(
                f"- {summary.plugin}: {len(summary.monthly)} months, "
                f"latest={summary.monthly[0].value if summary.monthly else 0}, "
                f"top categories: {summary.categories[:3]}"
            )
        if payload.emerging:
            lines.append("Emerging: " + ", ".join([t.name for t in payload.emerging]))
        if payload.declining:
            lines.append("Declining: " + ", ".join([t.name for t in payload.declining]))
        if payload.data_gaps:
            lines.append("Data gaps: " + "; ".join(payload.data_gaps))
        return "\n".join(lines)

    def _date_range(self, period: str) -> Tuple[date, date]:
        end = datetime.utcnow().date()
        if period == "last_month":
            start = end - timedelta(days=30)
        elif period == "last_90_days":
            start = end - timedelta(days=90)
        else:
            start = end - timedelta(days=365)
        return start, end

    def _daily_series(self, source: str, window: Tuple[date, date]) -> List[Dict[str, Any]]:
        start, end = window
        if source == "asana":
            return self.repo.asana_daily_series(start, end)
        if source == "toggl":
            return self.repo.toggl_daily_series(start, end)
        if source == "habitica":
            return self.repo.habitica_daily_series(start, end)
        if source == "google_fit":
            return self.repo.fit_daily_series(start, end)
        return []

    def _presence(self, series: List[Dict[str, Any]], window_days: int) -> Dict[str, Any]:
        days_active = len(series)
        longest_gap = self._longest_gap(series, window_days)
        return {"days_active": days_active, "window_days": window_days, "longest_gap_days": longest_gap}

    def _longest_gap(self, series: List[Dict[str, Any]], window_days: int) -> int:
        if not series:
            return window_days
        days_sorted = sorted([self._as_date(r["day"]) for r in series])
        longest = 0
        prev = days_sorted[0]
        for current in days_sorted[1:]:
            gap = (current - prev).days - 1
            if gap > longest:
                longest = gap
            prev = current
        return max(longest, 0)

    def _as_date(self, val) -> date:
        if isinstance(val, date):
            return val
        return datetime.fromisoformat(str(val)).date()

    def _consistency(self, series: List[Dict[str, Any]], window_days: int) -> Dict[str, Any]:
        values = [float(v.get("completed") or v.get("minutes") or v.get("steps") or 0) for v in series]
        if not values:
            return {"active_ratio": 0, "median": 0, "cv": None, "burstiness": None}
        active_ratio = len(values) / window_days
        median = statistics.median(values)
        mean = sum(values) / len(values)
        cv = (statistics.pstdev(values) / mean) if mean else None
        burstiness = "spread" if cv and cv < 0.5 else "clustered"
        return {"active_ratio": round(active_ratio, 2), "median": median, "cv": cv, "burstiness": burstiness}

    def _streak_from_series(self, series: List[Dict[str, Any]], threshold: float = 1.0) -> Dict[str, int]:
        streak = longest = 0
        last_day = None
        for row in sorted(series, key=lambda r: self._as_date(r["day"])):
            current = self._as_date(row["day"])
            meets = float(row.get("completed") or row.get("minutes") or row.get("steps") or 0) >= threshold
            if not meets:
                streak = 0
                last_day = current
                continue
            if last_day and (current - last_day).days == 1:
                streak += 1
            else:
                streak = 1
            longest = max(longest, streak)
            last_day = current
        return {"current": streak, "longest": longest}

    def _best_from_series(self, series: List[Dict[str, Any]], window: Tuple[date, date]) -> Dict[str, Any]:
        if not series:
            return {}
        best_day = max(series, key=lambda r: float(r.get("completed") or r.get("minutes") or r.get("steps") or 0))
        return {"day": str(best_day["day"]), "value": float(best_day.get("completed") or best_day.get("minutes") or best_day.get("steps") or 0)}

    def _momentum(self, series_30: List[Dict[str, Any]], series_90: List[Dict[str, Any]]) -> str:
        total_30 = sum(float(v.get("completed") or v.get("minutes") or v.get("steps") or 0) for v in series_30)
        total_90 = sum(float(v.get("completed") or v.get("minutes") or v.get("steps") or 0) for v in series_90)
        if total_90 == 0:
            return "paused"
        ratio = total_30 / total_90
        if ratio >= 0.6:
            return "rising"
        if ratio >= 0.3:
            return "stable"
        return "cooling"

    def _derive_phase_and_signals(self, source: str, metrics: Dict[str, Any], snap: Dict[str, Any]) -> Dict[str, Any]:
        presence = snap.get("presence", {})
        consistency = snap.get("consistency", {})
        momentum = snap.get("momentum", "paused")

        # Phase classification
        active_ratio = consistency.get("active_ratio", 0) or 0
        if momentum == "rising" and active_ratio >= 0.5:
            phase = "execution"
        elif momentum == "stable" and active_ratio >= 0.3:
            phase = "maintenance"
        elif momentum == "cooling":
            phase = "recovery"
        else:
            phase = "paused"

        # Engagement quality
        burst = consistency.get("burstiness")
        if active_ratio >= 0.6 and burst == "spread":
            engagement = "steady"
        elif active_ratio >= 0.3 and burst != "clustered":
            engagement = "focused"
        elif active_ratio > 0:
            engagement = "fragmented"
        else:
            engagement = "sparse"

        key_facts = []
        if presence:
            key_facts.append(f"Active {presence.get('days_active',0)}/{presence.get('window_days',0)} days; longest gap {presence.get('longest_gap_days',0)} days")
        if consistency:
            cv = consistency.get("cv")
            key_facts.append(f"Median daily value {consistency.get('median',0)}; variability {cv if cv is not None else 'n/a'}")
        if snap.get("streaks"):
            key_facts.append(f"Current streak {snap['streaks'].get('current',0)} days; longest {snap['streaks'].get('longest',0)}")

        return {
            "phase": phase,
            "momentum": momentum,
            "engagement": engagement,
            "presence": presence,
            "consistency": consistency,
            "streaks": snap.get("streaks", {}),
            "best": snap.get("best", {}),
            "key_facts": key_facts[:3],
        }

    def _highlights(self, derived: Dict[str, Any]) -> Dict[str, List[str]]:
        wins, streaks, stability = [], [], []
        for src, val in derived.items():
            if val.get("engagement") in ("steady", "focused") and val.get("phase") in ("execution", "maintenance"):
                wins.append(f"{src}: {val['engagement']} pattern with {val['phase']} phase")
            st = val.get("streaks", {})
            if st.get("longest", 0) >= 5:
                streaks.append(f"{src}: longest streak {st['longest']} days")
            if val.get("engagement") == "steady":
                stability.append(f"{src}: stable engagement")
        return {"wins": wins[:3], "streaks": streaks[:3], "stability": stability[:3]}

    def _changes(self, metrics: List[Metric]) -> Dict[str, List[str]]:
        increases, decreases, no_change = [], [], []
        # Simple delta using last30 vs prior30
        def fetch(source, name, window):
            for m in metrics:
                if m.source == source and m.name == name and m.window == window:
                    return m
            return None

        for source in {"asana", "toggl", "habitica", "google_fit"}:
            last = fetch(source, "minutes_tracked" if source == "toggl" else ("steps" if source == "google_fit" else "tasks_completed" if source == "asana" else "completions"), Window.LAST_30_DAYS)
            prior = fetch(source, last.name if last else "", Window.PRIOR_30_DAYS) if last else None
            if last and prior and prior.value:
                change = (last.value - prior.value) / prior.value
                if change >= 0.2:
                    increases.append(f"{source}: increased vs prior 30d")
                elif change <= -0.2:
                    decreases.append(f"{source}: decreased vs prior 30d")
                else:
                    no_change.append(f"{source}: stable vs prior 30d")
        return {"meaningful_increases": increases[:5], "meaningful_decreases": decreases[:5], "no_change": no_change[:5]}

    def _window_ranges(self, end_date: date) -> Dict[str, Tuple[date, date]]:
        return {
            "LAST_7_DAYS": (end_date - timedelta(days=7), end_date),
            "LAST_30_DAYS": (end_date - timedelta(days=30), end_date),
            "PRIOR_30_DAYS": (end_date - timedelta(days=60), end_date - timedelta(days=30)),
            "LAST_90_DAYS": (end_date - timedelta(days=90), end_date),
            "PRIOR_90_DAYS": (end_date - timedelta(days=180), end_date - timedelta(days=90)),
        }

    def _validate_metrics(self, metrics: List[Metric]) -> Tuple[List[Metric], List[str]]:
        caveats: List[str] = []
        filtered: List[Metric] = []

        def find(source: str, window: Window, name: str) -> Optional[Metric]:
            for m in metrics:
                if m.source == source and m.window == window and m.name == name:
                    return m
            return None

        for m in metrics:
            if m.value is None:
                continue
            if m.value < 0:
                caveats.append(f"Dropped negative metric {m.name} {m.source} {m.window}")
                continue
            filtered.append(m)

        final: List[Metric] = []
        for m in filtered:
            last30 = find(m.source, Window.LAST_30_DAYS, m.name)
            last90 = find(m.source, Window.LAST_90_DAYS, m.name)
            if last30 and last90 and last30.value > last90.value and m.window == Window.LAST_30_DAYS:
                caveats.append(f"Dropped {m.source} {m.name} last 30 metrics due to exceeding last 90 window.")
                continue
            final.append(m)
        return final, caveats

    def _context_from_metrics(self, metrics: List[Metric], caveats: List[str], snapshots: Dict[str, Any]) -> str:
        sources: Dict[str, Any] = {}
        for m in metrics:
            src = sources.setdefault(m.source, {"metrics": {}, "snapshots": snapshots.get(m.source, {})})
            src["metrics"][m.window.value.lower()] = {
                "name": m.name,
                "value": m.value,
                "unit": m.unit,
                "coverage_days": m.coverage_days,
                "confidence": m.confidence,
            }

        # Derive phases and engagement quality per source
        derived = {}
        for source, data in sources.items():
            snap = data.get("snapshots", {})
            derived[source] = self._derive_phase_and_signals(source, data.get("metrics", {}), snap)

        context = {
            "period": "last_30_days",
            "sources": derived,
            "highlights": self._highlights(derived),
            "changes": self._changes(metrics),
            "uncertainties": caveats,
        }
        context_text = json.dumps(context, ensure_ascii=False)
        if len(context_text) > self.max_context_chars:
            # Drop lower-priority sections
            context.pop("changes", None)
            context.pop("uncertainties", None)
            context_text = json.dumps(context, ensure_ascii=False)[: self.max_context_chars]
        return context_text

    def _streaks(self, start_date: date, end_date: date) -> Dict[str, Any]:
        habitica_days = self.repo.habitica_daily(start_date, end_date)
        fit_days = self.repo.google_fit_daily_steps(start_date, end_date)

        def longest_streak(days: List[Dict[str, Any]], threshold: float = 1.0) -> Tuple[int, int]:
            # days sorted desc; compute streak
            streak = longest = 0
            last_day = None
            for row in sorted(days, key=lambda r: r["day"]):
                current_val = row["day"]
                if isinstance(current_val, str):
                    current = datetime.fromisoformat(str(current_val)).date()
                else:
                    current = current_val
                meets = float(row.get("completions") or row.get("steps") or 0) >= threshold
                if not meets:
                    streak = 0
                    last_day = current
                    continue
                if last_day and (current - last_day).days == 1:
                    streak += 1
                else:
                    streak = 1
                longest = max(longest, streak)
                last_day = current
            return streak, longest

        habitica_current, habitica_longest = longest_streak(habitica_days, threshold=1)
        fit_current, fit_longest = longest_streak(fit_days, threshold=2000)

        return {
            "habitica": {"current": habitica_current, "longest": habitica_longest},
            "google_fit": {"current": fit_current, "longest": fit_longest},
        }

    def _best_periods(self) -> Dict[str, Any]:
        # Placeholder that could be extended; return empty for now
        return {}
