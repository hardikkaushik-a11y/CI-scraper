"""
assistant_backend.py — Actian Intelligence Assistant Backend
─────────────────────────────────────────────────────────────
Lightweight Flask server that:
  • Loads jobs_enriched.csv + signals.json on startup (cached by mtime)
  • Builds dynamic data context from real data
  • Calls Groq (Llama 3.3 70B) for analyst-grade responses — free tier
  • Retries with exponential backoff on rate-limit
  • Returns optional dashboard_action for session-based UI control

Usage:
  export GROQ_API_KEY=gsk_...
  python assistant_backend.py
  → Runs on http://localhost:5001
"""

import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import httpx
from flask import Flask, request, jsonify
from flask_cors import CORS

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.3-70b-versatile"
DATA_DIR = Path(__file__).parent.parent / "data"
EXCLUDED_FUNCTIONS = {"Legal", "People/HR"}

app = Flask(__name__)
CORS(app)  # Allow dashboard to call from file:// or localhost

# ══════════════════════════════════════════════════════════════════════════
# DATA CACHE
# ══════════════════════════════════════════════════════════════════════════

_cache: dict = {"rows": None, "signals": None, "loaded_at": 0.0}


def _load_csv(path: Path) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("Function", "") in EXCLUDED_FUNCTIONS:
                continue
            try:
                row["_relevancy"] = float(row.get("Relevancy_to_Actian") or 0)
            except ValueError:
                row["_relevancy"] = 0.0
            try:
                row["_days"] = int(float(row.get("Days Since Posted") or 9999))
            except ValueError:
                row["_days"] = 9999
            rows.append(row)
    return rows


def load_data() -> tuple[list[dict], list[dict]]:
    """Load data from disk, refresh only when files are newer than cache."""
    csv_path = DATA_DIR / "jobs_enriched.csv"
    sig_path = DATA_DIR / "signals.json"

    if not csv_path.exists():
        raise FileNotFoundError(f"jobs_enriched.csv not found in {DATA_DIR}")

    mtime = csv_path.stat().st_mtime
    if sig_path.exists():
        mtime = max(mtime, sig_path.stat().st_mtime)

    if _cache["rows"] is not None and mtime <= _cache["loaded_at"]:
        return _cache["rows"], _cache["signals"]

    rows = _load_csv(csv_path)
    signals: list[dict] = []
    if sig_path.exists():
        with open(sig_path, encoding="utf-8") as f:
            signals = json.load(f)

    _cache.update({"rows": rows, "signals": signals, "loaded_at": mtime})
    return rows, signals


# ══════════════════════════════════════════════════════════════════════════
# CONTEXT BUILDER
# ══════════════════════════════════════════════════════════════════════════

def _count(rows: list[dict], key: str) -> dict:
    counts: dict[str, int] = defaultdict(int)
    for r in rows:
        v = r.get(key, "")
        if v:
            counts[v] += 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def build_semantic_layer(rows: list[dict], signals: list[dict]) -> dict:
    """
    Compute the Semantic Layer — defined business metrics per company.
    These are the ground-truth numbers the AI agent reasons against,
    not raw data dumps.
    """
    company_counts = _count(rows, "Company")
    sig_by_company = {s.get("company", ""): s for s in signals}

    metrics = {}
    for company, total in company_counts.items():
        c_rows = [r for r in rows if r["Company"] == company]

        # ── Hiring Velocity ──────────────────────────────────────────────
        # Normalized daily rate: recent 30 days vs prior 60 days
        # 100 = same pace, >100 = accelerating, <100 = slowing
        recent_30  = [r for r in c_rows if r["_days"] <= 30]
        prior_60   = [r for r in c_rows if 30 < r["_days"] <= 90]
        daily_recent = len(recent_30) / 30
        daily_prior  = len(prior_60) / 60
        if daily_prior == 0:
            # No prior data — new entrant or data gap; cap at 200 to avoid nonsense
            velocity_score = min(200, round(daily_recent * 100)) if daily_recent > 0 else 100
        else:
            velocity_score = min(500, round(daily_recent / daily_prior * 100))

        # ── AI Investment Ratio ──────────────────────────────────────────
        ai_roles = [r for r in c_rows if r.get("Function") in ("AI/ML & Vector", "Engineering")
                    and any(k in (r.get("Job Title", "") + r.get("Product_Focus", "")).lower()
                            for k in ["ai", "ml", "llm", "vector", "machine learning", "deep learning", "rag"])]
        ai_ratio = round(len(ai_roles) / total * 100) if total else 0

        # ── Competitive Overlap Score ────────────────────────────────────
        # % of roles in product areas that directly compete with Actian
        direct_focus = {"ETL/Integration", "Data Governance", "Data Observability", "Data Quality", "Vector / AI"}
        overlap_roles = [r for r in c_rows if r.get("Product_Focus") in direct_focus]
        overlap_score = round(len(overlap_roles) / total * 100) if total else 0

        # ── Seniority Composition ────────────────────────────────────────
        senior_levels = {"Director+", "Principal/Staff", "Manager", "Senior"}
        senior_count  = len([r for r in c_rows if r.get("Seniority") in senior_levels])
        senior_ratio  = round(senior_count / total * 100) if total else 0

        # ── Engineering Concentration ────────────────────────────────────
        eng_roles  = [r for r in c_rows if r.get("Function") == "Engineering"]
        eng_ratio  = round(len(eng_roles) / total * 100) if total else 0

        # ── GTM Concentration ────────────────────────────────────────────
        gtm_roles  = [r for r in c_rows if r.get("Function") in ("Sales", "Marketing", "Customer Success")]
        gtm_ratio  = round(len(gtm_roles) / total * 100) if total else 0

        # ── Mean Relevancy ────────────────────────────────────────────────
        mean_rel   = round(sum(r["_relevancy"] for r in c_rows) / total, 1) if total else 0
        high_rel   = len([r for r in c_rows if r["_relevancy"] >= 10])

        # ── Dominant Function & Product ──────────────────────────────────
        dom_fn  = list(_count(c_rows, "Function").keys())[0]  if c_rows else "—"
        dom_pf  = list(_count(c_rows, "Product_Focus").keys())[0] if c_rows else "—"

        # ── Threat Signal ────────────────────────────────────────────────
        sig    = sig_by_company.get(company, {})
        threat = sig.get("threat_level", "low").lower()

        metrics[company] = {
            "total_roles":        total,
            "threat_level":       threat,
            "hiring_velocity":    velocity_score,      # 100 = same pace, >100 = accelerating
            "ai_investment_pct":  ai_ratio,            # % of roles AI/ML related
            "competitive_overlap_pct": overlap_score,  # % of roles in Actian-competitive areas
            "senior_pct":         senior_ratio,        # % Director+ / Senior
            "engineering_pct":    eng_ratio,
            "gtm_pct":            gtm_ratio,
            "mean_relevancy":     mean_rel,
            "high_signal_roles":  high_rel,
            "dominant_function":  dom_fn,
            "dominant_product":   dom_pf,
            "recent_30d":         len(recent_30),
            "company_group":      sig.get("company_group", ""),
        }

    return metrics


def build_context(query: str, rows: list[dict], signals: list[dict]) -> str:
    """
    Assemble focused, data-grounded context for the LLM.
    Payload-optimized: limits semantic layer to top threats + query-relevant companies.
    """
    total          = len(rows)
    company_counts = _count(rows, "Company")
    seniority_counts = _count(rows, "Seniority")
    function_counts  = _count(rows, "Function")
    product_counts   = _count(rows, "Product_Focus")
    group_counts     = _count(rows, "Company_Group")
    query_lower      = query.lower()

    # ── Semantic Layer ────────────────────────────────────────────────────
    sem = build_semantic_layer(rows, signals)

    # ── Market Pressure Index ─────────────────────────────────────────────
    # Weighted sum: CRITICAL=3, HIGH=2, MEDIUM=1
    threat_weights = {"critical": 3, "high": 2, "medium": 1, "low": 0}
    market_pressure = sum(threat_weights.get(m["threat_level"], 0) * m["total_roles"]
                          for m in sem.values())

    # ── Top Movers (highest velocity) ────────────────────────────────────
    top_velocity = sorted(sem.items(), key=lambda x: -x[1]["hiring_velocity"])[:5]

    # ── AI Leaders ───────────────────────────────────────────────────────
    ai_leaders = sorted(sem.items(), key=lambda x: -x[1]["ai_investment_pct"])[:5]

    # ── High Overlap (direct Actian threats) ──────────────────────────────
    overlap_leaders = sorted(sem.items(), key=lambda x: -x[1]["competitive_overlap_pct"])[:5]

    # ── Signal summaries — TRUNCATED to top 15 signals ─────────────────────
    signal_summary = [
        {
            "company": s.get("company", ""),
            "threat":  s.get("threat_level", ""),
            "roles":   s.get("total_postings", 0),
        }
        for s in signals[:15]
    ]

    # ── Top high-relevancy roles — REDUCED to 6 ──────────────────────────
    high_rel_rows = sorted([r for r in rows if r["_relevancy"] >= 10],
                           key=lambda x: -x["_relevancy"])[:6]
    top_roles = [
        {
            "company":   r["Company"],
            "title":     r["Job Title"][:50],  # Truncate long titles
            "relevancy": r["_relevancy"],
        }
        for r in high_rel_rows
    ]

    # ── Recent postings ───────────────────────────────────────────────────
    recent = [r for r in rows if r["_days"] <= 7]
    recent_by_company = _count(recent, "Company")

    # ── Query-aware analytics ─────────────────────────────────────────────
    specific_context = ""

    # COMPANY DRILL-DOWN: if query names a specific company
    mentioned = [c for c in company_counts if c.lower() in query_lower]

    if len(mentioned) == 1:
        # Single company deep-dive
        company   = mentioned[0]
        c_rows    = [r for r in rows if r["Company"] == company]
        c_sem     = sem.get(company, {})
        c_sig     = next((s for s in signals if s.get("company") == company), {})
        c_top     = sorted(c_rows, key=lambda x: -x["_relevancy"])[:5]  # Reduced from 10 to 5

        specific_context = f"""
━━━ COMPANY DEEP-DIVE: {company.upper()} ━━━
Metrics: total={c_sem.get('total_roles', 0)}, velocity={c_sem.get('hiring_velocity', 0)}, threat={c_sem.get('threat_level', '')}
AI investment: {c_sem.get('ai_investment_pct', 0)}%, Competitive overlap: {c_sem.get('competitive_overlap_pct', 0)}%
Signal: {c_sig.get('narrative', '')[:200] if c_sig.get('narrative') else 'N/A'}
Top {len(c_top)} roles: {json.dumps([{"title": r["Job Title"][:40], "seniority": r.get("Seniority", "")} for r in c_top])}
"""

    elif len(mentioned) >= 2:
        # COMPARISON: two or more companies side-by-side
        comparison = {}
        for company in mentioned[:2]:  # Limit to 2 for comparison
            m = sem.get(company, {})
            comparison[company] = {
                "total": m.get("total_roles", 0),
                "velocity": m.get("hiring_velocity", 0),
                "threat": m.get("threat_level", ""),
                "ai_pct": m.get("ai_investment_pct", 0),
                "overlap_pct": m.get("competitive_overlap_pct", 0),
            }
        specific_context = f"""
━━━ COMPANY COMPARISON ━━━
{json.dumps(comparison, indent=2)}
"""

    # SEGMENT QUERY: if asking about a segment/group
    segments = {
        "etl": "ETL/Connectors", "integration": "ETL/Connectors", "fivetran": "ETL/Connectors",
        "governance": "Data Intelligence", "catalog": "Data Intelligence", "alation": "Data Intelligence",
        "vector": "Vector DB / AI", "ai": "Vector DB / AI", "pinecone": "Vector DB / AI",
        "observability": "Data Observability", "warehouse": "Warehouse/Processing",
        "snowflake": "Warehouse/Processing", "databricks": "Warehouse/Processing",
    }
    matched_segment = next((v for k, v in segments.items() if k in query_lower), None)
    if matched_segment and not mentioned:
        seg_rows = [r for r in rows if r.get("Company_Group") == matched_segment]
        seg_companies = {c: sem[c] for c in _count(seg_rows, "Company") if c in sem}
        # Limit segment companies to top 8 by threat level
        top_seg = sorted(seg_companies.items(),
                        key=lambda x: {"critical": 3, "high": 2, "medium": 1, "low": 0}.get(x[1].get("threat_level", ""), 0),
                        reverse=True)[:8]
        specific_context = f"""
━━━ SEGMENT: {matched_segment.upper()} ━━━
Companies ({len(top_seg)}): {', '.join(c for c, _ in top_seg)}
"""

    # ── PAYLOAD-OPTIMIZED Semantic Layer ──────────────────────────────────
    # Instead of all 63 companies, include: top threats + top by velocity/overlap
    critical_companies = {c: m for c, m in sem.items() if m["threat_level"] == "critical"}
    high_companies = {c: m for c, m in sem.items() if m["threat_level"] == "high"}
    top_sem = {c: m for c, m in dict(top_velocity + ai_leaders + overlap_leaders).items() if c in sem}

    # Merge and deduplicate (max 20 companies)
    priority_sem = {**critical_companies, **high_companies, **top_sem}
    priority_sem = dict(list(priority_sem.items())[:20])

    context = f"""
ACTIAN COMPETITIVE HIRING INTELLIGENCE — LIVE DATASET
Snapshot: {datetime.now().strftime("%B %d, %Y")}
Data: {total} roles across {len(company_counts)} companies (365-day rolling window)

━━━ SEMANTIC LAYER — TOP THREATS & MOVERS ━━━
Market Pressure Index: {market_pressure}

Top 5 Hiring Velocity:
{json.dumps([{"company": c, "velocity": m["hiring_velocity"], "threat": m["threat_level"]} for c,m in top_velocity])}

Top 5 AI Investment:
{json.dumps([{"company": c, "ai_pct": m["ai_investment_pct"]} for c,m in ai_leaders])}

Top 5 Competitive Overlap:
{json.dumps([{"company": c, "overlap_pct": m["competitive_overlap_pct"]} for c,m in overlap_leaders])}

Priority Companies (CRITICAL + HIGH + Top movers):
{json.dumps(priority_sem, indent=2)}

━━━ MARKET SNAPSHOT ━━━
Total roles: {total}, Companies: {len(company_counts)}, New (7d): {len(recent)}
Top functions: {json.dumps(dict(list(function_counts.items())[:3]))}
Top products: {json.dumps(dict(list(product_counts.items())[:3]))}

━━━ HIGH-SIGNAL ROLES ━━━
{json.dumps(top_roles)}

━━━ STRATEGIC SIGNALS (Top 15) ━━━
{json.dumps(signal_summary)}

━━━ RELEVANCY SCORING ━━━
Scale 0–17.5: +3 skill match, +5 direct compete, +3 Director+, +2 AI title

━━━ COMPETITIVE LANDSCAPE ━━━
ETL/Connectors → Fivetran, Boomi | Data Intelligence → Collibra, Alation | Warehouse → Snowflake, Databricks | Vector/AI → Pinecone, Weaviate
{specific_context}
"""
    return context


# ══════════════════════════════════════════════════════════════════════════
# CLAUDE API CALLER WITH RETRY
# ══════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are an elite competitive intelligence analyst embedded in Actian's internal hiring-signal dashboard. You have real-time access to competitor hiring data across 30+ companies in the data infrastructure, ETL, governance, and AI/ML space.

Actian is a data integration and management platform competing in ETL/connectors, data management, and AI-powered data pipelines and vector search.

━━━ SEMANTIC LAYER — HOW TO REASON WITH THE DATA ━━━
You have access to pre-computed business metrics for every company. Reason with these directly:

• hiring_velocity: Index where 100 = same pace as prior period, >100 = accelerating, <100 = slowing.
  A company at 180 is hiring 80% faster than 30 days ago — that's a strategic signal.

• ai_investment_pct: % of roles that are AI/ML-related. Industry average is ~12%.
  A company at 35% is betting heavily on AI — interpret what product bet that implies.

• competitive_overlap_pct: % of roles in product areas that directly compete with Actian
  (ETL/Integration, Data Governance, Data Observability, Vector/AI).
  A company at 60%+ overlap is building directly into Actian's market.

• senior_pct: % of Director+/Principal/Senior roles. >50% = building leadership, new product lines.
  High senior ratio + high volume = strategic expansion, not headcount backfill.

• engineering_pct vs gtm_pct: Engineering-heavy = product build phase. GTM-heavy = go-to-market push.
  A shift from engineering to GTM in a competitor = they're about to sell, not just build.

• mean_relevancy: Average relevancy score to Actian (0–17.5). >8.0 = high competitive pressure.

• Market Pressure Index: Weighted threat score across all companies. Use to frame urgency.

━━━ HOW TO ANSWER ━━━
— Lead with the most important number or insight. No preamble.
— When comparing companies, use the semantic metrics side-by-side.
— When asked about a trend, reason from velocity + recent_30d + senior_pct together.
— When asked who poses the biggest threat, use competitive_overlap_pct + threat_level + velocity together.
— Be concise, direct, confident. Sound like a senior analyst briefing the C-suite.
— Format with markdown for clarity (bold key points, bullet lists for breakdowns).
— Do NOT make up data. If something isn't in the context, say so.
— Do NOT repeat back the user's question. Get straight to the answer.

━━━ DASHBOARD CONTROL ━━━
Sometimes filtering the dashboard makes the response more useful. Rules:
— NEVER include a dashboard action for comparison queries (2+ companies). A comparison needs both.
— NEVER include a dashboard action for segment or broad market queries.
— ONLY include an action for single-company drill-downs or explicit "show me X function" requests.
— Never include more than ONE action.

If and only if appropriate, append ONE action at the very end:

Filter by company (single-company only):
```dashboard_action
{"type": "filter", "field": "Company", "value": "ExactCompanyName"}
```
Filter by segment:
```dashboard_action
{"type": "filter", "field": "Company_Group", "value": "ETL/Connectors"}
```
Filter by function:
```dashboard_action
{"type": "filter", "field": "Function", "value": "Engineering"}
```
Filter by seniority:
```dashboard_action
{"type": "filter", "field": "Seniority", "value": "Director+"}
```
Highlight a section:
```dashboard_action
{"type": "highlight", "section": "signals"}
```"""


def call_llm(messages: list[dict], max_retries: int = 3) -> str:
    """Call Groq (Llama 3.3 70B) with exponential backoff on rate-limit."""
    if not GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY not set. Run: export GROQ_API_KEY=gsk_...")

    groq_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    # Estimate payload size (rough check: Groq has ~2.5MB limit for free tier)
    payload_estimate = len(json.dumps(groq_messages))
    if payload_estimate > 2000000:  # 2MB safety threshold
        print(f"⚠️  Payload estimate: {payload_estimate / 1024:.1f}KB — may exceed limits")

    last_error: Exception = RuntimeError("Unknown error")
    for attempt in range(max_retries):
        try:
            with httpx.Client(timeout=90) as client:
                resp = client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "content-type": "application/json",
                    },
                    json={
                        "model": GROQ_MODEL,
                        "max_tokens": 1200,
                        "messages": groq_messages,
                    },
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"]

        except httpx.HTTPStatusError as e:
            last_error = e
            status = e.response.status_code
            if status == 413:
                # Payload Too Large — reduce future contexts
                raise ValueError(
                    "Payload too large (413). Context is being optimized. "
                    "Try a more specific query (single company, product, segment) for better results."
                ) from e
            if status == 429 and attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            raise

        except httpx.TimeoutException as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise

        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise

    raise last_error


# ══════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════

@app.route("/health", methods=["GET"])
def health():
    try:
        rows, signals = load_data()
        return jsonify({
            "status": "ok",
            "model": GROQ_MODEL,
            "roles": len(rows),
            "signals": len(signals),
            "api_key_set": bool(GROQ_API_KEY),
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/dashboard/v2/", methods=["GET"])
def serve_dashboard_v2():
    """Serve the regenerated dashboard_v2.html with embedded data."""
    dashboard_path = Path(__file__).parent.parent / "dashboard" / "v2" / "dashboard_v2.html"
    try:
        if not dashboard_path.exists():
            return jsonify({"error": "Dashboard not found"}), 404
        with open(dashboard_path, "r") as f:
            html = f.read()
        return html, 200, {"Content-Type": "text/html; charset=utf-8"}
    except Exception as e:
        return jsonify({"error": f"Failed to load dashboard: {str(e)}"}), 500


@app.route("/context", methods=["GET"])
def get_context_summary():
    """Returns live data summary for dynamic suggested prompts."""
    try:
        rows, signals = load_data()

        company_counts = _count(rows, "Company")
        recent = [r for r in rows if r["_days"] <= 7]

        critical = [s["company"] for s in signals if s.get("threat_level") == "CRITICAL"]
        high = [s["company"] for s in signals if s.get("threat_level") == "HIGH"]
        top_threat = (critical + high + ["N/A"])[0]

        top_product = list(_count(rows, "Product_Focus").keys())
        top_company = list(company_counts.keys())[0] if company_counts else "N/A"

        ai_roles = len([r for r in rows if r.get("Function") == "AI/ML & Vector"])
        high_rel = [r for r in rows if r["_relevancy"] >= 10]

        # Seniority senior+ ratio
        senior_levels = {"Senior", "Principal/Staff", "Director+", "Manager"}
        senior_count = len([r for r in rows if r.get("Seniority") in senior_levels])
        senior_ratio = round(senior_count / len(rows) * 100) if rows else 0

        return jsonify({
            "total_roles": len(rows),
            "total_companies": len(company_counts),
            "top_company": top_company,
            "top_company_count": company_counts.get(top_company, 0),
            "top_threat": top_threat,
            "critical_companies": critical,
            "high_companies": high[:4],
            "high_relevancy_count": len(high_rel),
            "recent_count": len(recent),
            "ai_roles": ai_roles,
            "top_product_focus": top_product[:3],
            "senior_ratio": senior_ratio,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/chat", methods=["POST"])
def chat():
    """Main chat endpoint. Accepts message + conversation history."""
    if not GROQ_API_KEY:
        return jsonify({
            "error": "GROQ_API_KEY not set.",
            "hint": "Run: export GROQ_API_KEY=gsk_... then restart the backend."
        }), 400

    body = request.json or {}
    user_message = (body.get("message") or "").strip()
    history: list[dict] = body.get("history") or []

    if not user_message:
        return jsonify({"error": "No message provided"}), 400

    try:
        rows, signals = load_data()
        context = build_context(user_message, rows, signals)

        # Build message array — inject context only on first user turn
        messages: list[dict] = []
        for h in history[-8:]:  # Keep last 8 turns for coherence
            messages.append({"role": h["role"], "content": h["content"]})

        # If no history, include full context; if follow-up, keep it lighter
        if not history:
            content = f"DATA CONTEXT:\n{context}\n\nQUESTION: {user_message}"
        else:
            # Re-include context so model stays grounded even in multi-turn
            content = f"[Updated context snapshot]\n{context}\n\nQUESTION: {user_message}"

        messages.append({"role": "user", "content": content})

        response_text = call_llm(messages)

        # Parse optional dashboard_action block
        dashboard_action = None
        if "```dashboard_action" in response_text:
            try:
                parts = response_text.split("```dashboard_action")
                action_raw = parts[1].split("```")[0].strip()
                dashboard_action = json.loads(action_raw)
                response_text = parts[0].strip()
            except Exception:
                pass  # Malformed action block — just drop it silently

        return jsonify({
            "message": response_text,
            "dashboard_action": dashboard_action,
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Assistant error: {str(e)}"}), 500


# ══════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("━" * 60)
    print("  Actian Intelligence Assistant Backend")
    print("━" * 60)

    if not GROQ_API_KEY:
        print("⚠  GROQ_API_KEY not set.")
        print("   Run: export GROQ_API_KEY=gsk_...")
    else:
        print(f"✓  API key set ({GROQ_API_KEY[:12]}...)")

    try:
        rows, signals = load_data()
        print(f"✓  Loaded {len(rows)} roles from {len(_count(rows, 'Company'))} companies")
        print(f"✓  Loaded {len(signals)} strategic signals")
    except Exception as e:
        print(f"⚠  Data load warning: {e}")
        print("   Place jobs_enriched.csv and signals.json in the same directory.")

    print(f"✓  Model: {GROQ_MODEL}")
    print(f"✓  Running at http://localhost:5001")
    print("━" * 60)

    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
