"""
Vercel serverless function — Ask CI

Receives:  POST /api/ask-ci { message: str, history: [{role, content}, ...], lens?: str }
Returns:   { message: str, dashboard_action?: dict }

Reads the precomputed semantic layer from GitHub raw (cached daily build),
builds a focused context payload, calls DeepSeek V4-Flash, returns the answer.

Replaces the Render-hosted Flask backend (assistant_backend.py) and Groq.
"""

import json
import os
import time
import urllib.request
from http.server import BaseHTTPRequestHandler

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL   = "deepseek-chat"
DEEPSEEK_URL     = "https://api.deepseek.com/chat/completions"

# Public GitHub raw URL — semantic_layer.json updated daily by CI
SEMANTIC_LAYER_URL = "https://raw.githubusercontent.com/hardikkaushik-a11y/CI-scraper/main/data/semantic_layer.json"

# In-memory cache (per cold start; warm function reuses)
_cache: dict = {"semantic": None, "fetched_at": 0.0}
_CACHE_TTL = 3600  # 1 hour


SYSTEM_PROMPT = """You are an elite competitive intelligence analyst embedded in Actian's CI dashboard. \
You have a precomputed semantic layer with metrics for every tracked competitor.

Actian competes in: AI Analyst (Cortex/Genie alternative), Data Intelligence (catalog/governance), \
Data Observability, VectorAI. Reason against the metrics directly — do not fabricate data.

KEY METRICS (per company):
- hiring_velocity: 100=same pace, >100=accelerating. e.g. 180 = hiring 80% faster than 30d ago.
- ai_investment_pct: % roles AI/ML-related. Industry avg ~12%. >30% = heavy AI bet.
- competitive_overlap_pct: % roles in Actian-direct product areas (ETL/Integration, Governance, \
Observability, Vector/AI). >50% = building directly into Actian's market.
- senior_pct: % Director+/Principal/Senior. >50% + high volume = strategic expansion, not backfill.
- engineering_pct vs gtm_pct: Eng-heavy = product build phase. GTM-heavy = market push.
- country_top + country_recent_30d: Geographic footprint and active expansion. \
A country in recent_30d that's not top of all-time = NEW geographic push.

KEY DATA AVAILABLE:
- per_company: metrics for every competitor
- verdicts_by_co: latest analyst verdict per company (what's happening, why it matters, recommended action, themes)
- roadmaps_by_co: published or inferred strategic roadmap per company (pillars, timeline, Actian impact)
- recent_comp_signals: last 25 launches/events
- recent_news: last 25 news items
- market: market_pressure_index, top_velocity, ai_leaders, top_countries (overall + 30d)

HOW TO ANSWER:
- Lead with the most important number or insight. No preamble. No "Great question."
- Use markdown formatting (bold, bullet lists) for clarity.
- For comparisons, show metrics side by side.
- For geographic/expansion questions, use country_top vs country_recent_30d.
- For "what's next" questions, use roadmaps_by_co + recent comp_signals.
- If something isn't in the data, say so — never invent."""


# ──────────────────────────────────────────────────────────────────────────

def _fetch_semantic_layer() -> dict:
    now = time.time()
    if _cache["semantic"] and (now - _cache["fetched_at"]) < _CACHE_TTL:
        return _cache["semantic"]
    try:
        with urllib.request.urlopen(SEMANTIC_LAYER_URL, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        _cache["semantic"] = data
        _cache["fetched_at"] = now
        return data
    except Exception as e:
        # Last-good fallback — keep serving stale data on transient fetch failure
        if _cache["semantic"]:
            return _cache["semantic"]
        raise RuntimeError(f"Failed to fetch semantic layer: {e}")


def _build_context(query: str, sl: dict) -> str:
    q = (query or "").lower()
    snapshot = sl.get("snapshot", "unknown")
    totals = sl.get("totals", {})
    market = sl.get("market", {})
    per_co = sl.get("per_company", {})

    # Identify mentioned companies for targeted depth
    mentioned = [c for c in per_co if c.lower() in q]

    parts = [
        f"ACTIAN CI — semantic layer @ {snapshot}",
        f"Coverage: {totals.get('jobs', 0)} jobs across {totals.get('companies', 0)} companies, "
        f"{totals.get('comp_signals', 0)} comp signals, {totals.get('news', 0)} news items, "
        f"{totals.get('verdicts', 0)} verdicts, {totals.get('roadmaps', 0)} roadmaps.",
        "",
        "━━━ MARKET ROLLUPS ━━━",
        f"Market pressure index: {market.get('market_pressure_index', 0)}",
        f"Top hiring velocity: {json.dumps(market.get('top_velocity', []))}",
        f"Top AI investors: {json.dumps(market.get('top_ai_leaders', []))}",
        f"Top Actian-overlap: {json.dumps(market.get('top_overlap', []))}",
        f"Top hiring countries (all-time): {json.dumps(market.get('top_countries', {}))}",
        f"Top hiring countries (last 30d): {json.dumps(market.get('top_countries_30d', {}))}",
        "",
    ]

    # If specific companies mentioned: deep-dive
    if mentioned:
        parts.append("━━━ COMPANY DEEP-DIVE ━━━")
        for company in mentioned[:3]:
            m = per_co.get(company, {})
            v = sl.get("verdicts_by_co", {}).get(company, {})
            r = sl.get("roadmaps_by_co", {}).get(company, {})
            parts.append(f"\n[{company}]")
            parts.append(f"Metrics: {json.dumps(m)}")
            if v:
                parts.append(f"Verdict: {json.dumps(v)}")
            if r:
                parts.append(f"Roadmap: {json.dumps(r)}")
        parts.append("")

    # Always include trimmed priority snapshot for context (without overflowing)
    if not mentioned:
        priority = sorted(per_co.items(),
                          key=lambda x: (
                              {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(x[1].get("threat_level"), 4),
                              -x[1].get("total_roles", 0),
                          ))[:11]
        parts.append("━━━ PRIORITY ROSTER (top 11 by threat) ━━━")
        parts.append(json.dumps({c: m for c, m in priority}, indent=None))
        parts.append("")
        # Recent activity helps generic "what's new" questions
        parts.append("━━━ RECENT ACTIVITY (last 8 each) ━━━")
        parts.append(f"Launches/events: {json.dumps(sl.get('recent_comp_signals', [])[:8])}")
        parts.append(f"News: {json.dumps(sl.get('recent_news', [])[:8])}")

    return "\n".join(parts)


def _call_deepseek(messages: list[dict]) -> str:
    body = json.dumps({
        "model": DEEPSEEK_MODEL,
        "max_tokens": 1200,
        "messages": messages,
    }).encode("utf-8")
    req = urllib.request.Request(
        DEEPSEEK_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["choices"][0]["message"]["content"].strip()


# ──────────────────────────────────────────────────────────────────────────
# Vercel handler — BaseHTTPRequestHandler shape
# ──────────────────────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self._send_json(204, {})

    def do_GET(self):
        # Health check
        self._send_json(200, {"ok": True, "model": DEEPSEEK_MODEL, "key_set": bool(DEEPSEEK_API_KEY)})

    def do_POST(self):
        if not DEEPSEEK_API_KEY:
            self._send_json(500, {"error": "DEEPSEEK_API_KEY not configured"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        except Exception:
            self._send_json(400, {"error": "Invalid JSON body"})
            return

        message = (body.get("message") or "").strip()
        history = body.get("history") or []
        if not message:
            self._send_json(400, {"error": "message is required"})
            return

        try:
            sl = _fetch_semantic_layer()
        except Exception as e:
            self._send_json(503, {"error": str(e)})
            return

        context = _build_context(message, sl)
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT + "\n\n" + context},
            *history,
            {"role": "user", "content": message},
        ]
        try:
            reply = _call_deepseek(messages)
        except Exception as e:
            self._send_json(503, {"error": f"DeepSeek call failed: {e}"})
            return

        self._send_json(200, {"message": reply})
