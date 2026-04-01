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
DATA_DIR = Path(__file__).parent
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


def build_context(query: str, rows: list[dict], signals: list[dict]) -> str:
    """Assemble focused, data-grounded context for the LLM."""
    total = len(rows)
    company_counts = _count(rows, "Company")
    seniority_counts = _count(rows, "Seniority")
    function_counts = _count(rows, "Function")
    product_counts = _count(rows, "Product_Focus")
    group_counts = _count(rows, "Company_Group")

    top_companies = dict(list(company_counts.items())[:10])

    # Top high-relevancy roles
    high_rel = sorted([r for r in rows if r["_relevancy"] >= 10], key=lambda x: -x["_relevancy"])[:15]
    top_roles = [
        {
            "company": r["Company"],
            "title": r["Job Title"],
            "function": r["Function"],
            "seniority": r["Seniority"],
            "product": r["Product_Focus"],
            "relevancy": r["_relevancy"],
            "location": r.get("Location", ""),
        }
        for r in high_rel
    ]

    # Signal summaries
    signal_summary = [
        {
            "company": s.get("company", ""),
            "threat": s.get("threat_level", ""),
            "roles": s.get("total_postings", 0),
            "focus": s.get("dominant_product", ""),
            "group": s.get("company_group", ""),
            "implications": s.get("implications", [])[:3],
            "actions": s.get("recommended_actions", [])[:2],
        }
        for s in signals[:25]
    ]

    # Query-specific company drill-down
    specific_context = ""
    query_lower = query.lower()
    for company in company_counts.keys():
        if company.lower() in query_lower:
            c_rows = [r for r in rows if r["Company"] == company]
            c_seniority = _count(c_rows, "Seniority")
            c_function = _count(c_rows, "Function")
            c_product = _count(c_rows, "Product_Focus")
            c_top = sorted(c_rows, key=lambda x: -x["_relevancy"])[:8]
            specific_context = f"""
━━━ DETAILED DRILL-DOWN: {company.upper()} ━━━
Total roles: {len(c_rows)}
Seniority breakdown: {json.dumps(c_seniority)}
Function breakdown: {json.dumps(c_function)}
Product focus: {json.dumps(c_product)}
Top roles by relevancy:
{json.dumps([{"title": r["Job Title"], "seniority": r["Seniority"], "relevancy": r["_relevancy"], "location": r.get("Location","")} for r in c_top], indent=2)}
"""
            break

    # Recent postings (last 7 days)
    recent = [r for r in rows if r["_days"] <= 7]
    recent_by_company = _count(recent, "Company")

    context = f"""
ACTIAN COMPETITIVE HIRING INTELLIGENCE — LIVE DATASET
Snapshot date: {datetime.now().strftime("%B %d, %Y")}
Data source: Scraped career pages of 61 competitor companies (365-day rolling window)
Excluded: Legal, People/HR roles (not competitively relevant)

━━━ MARKET OVERVIEW ━━━
Total tracked roles: {total}
Companies tracked: {len(company_counts)}
New this week (≤7 days): {len(recent)} roles across {len(recent_by_company)} companies

Top companies by volume: {json.dumps(top_companies)}
Seniority distribution: {json.dumps(seniority_counts)}
Function breakdown: {json.dumps(function_counts)}
Product focus (top 10): {json.dumps(dict(list(product_counts.items())[:10]))}
Company segments: {json.dumps(group_counts)}
Recent hiring surge (last 7 days): {json.dumps(dict(list(recent_by_company.items())[:8]))}

━━━ HIGH-SIGNAL ROLES (Relevancy ≥ 10 / max 17.5) ━━━
{json.dumps(top_roles, indent=2)}

━━━ STRATEGIC THREAT SIGNALS ━━━
{json.dumps(signal_summary, indent=2)}

━━━ RELEVANCY SCORING SYSTEM ━━━
Each job is scored 0–17.5 based on:
  +3 per Actian-relevant skill match (ETL, SQL, Kafka, Python, Spark, dbt, governance, vector, RAG, LLM, MLOps, etc.)
  +5 for high-relevance product focus (ETL/Integration, Data Governance, Data Observability, Vector/AI)
  +2 for medium-relevance product focus (Platform/Infra, ML/AI infra)
  +3.0 Director+ seniority, +2.5 Principal/Staff, +1.5 Senior/Manager, +0.8 Mid
  +2 if AI/ML keywords appear in title/description
  +2 if job is in Actian's key markets (US, UK, Germany, India, etc.)
  +3 if company is a direct competitor (ETL/Connectors, Data Intelligence, Observability)
  +1.5 if company is adjacent threat (Warehouse/Processing, Monitoring)

━━━ COMPETITIVE SEGMENTS ━━━
  ETL/Connectors     → Fivetran, Boomi, Talend, MuleSoft — DIRECT Actian competitors
  Data Intelligence  → Collibra, Alation, Atlan — data governance & catalog space
  Warehouse/Processing → Snowflake, Databricks, MongoDB — adjacent, strategic
  Data Observability → Acceldata, Monte Carlo, Collate — data quality adjacent
  Vector DB / AI     → Pinecone, Weaviate, Qdrant, Zilliz — strategic priority (Actian launching vector)
  Enterprise         → Salesforce, SAP — platform adjacency
{specific_context}
"""
    return context


# ══════════════════════════════════════════════════════════════════════════
# CLAUDE API CALLER WITH RETRY
# ══════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are an elite competitive intelligence analyst embedded in Actian's internal hiring-signal dashboard. You have real-time access to competitor hiring data across 34 companies in the data infrastructure, ETL, governance, and AI/ML space.

Actian is a data integration and management platform competing primarily in ETL/connectors, data management, and increasingly AI-powered data pipelines and vector search.

YOUR ROLE:
— Provide sharp, data-grounded analysis. Reason from the specific numbers provided.
— Be concise, direct, and confident. No hedging. No generic statements.
— Sound like a senior analyst briefing a C-suite executive.
— When the data supports a conclusion, state it clearly.
— Format with markdown when it aids clarity (bold key points, bullet lists for breakdowns).
— Do NOT repeat back the user's question. Get straight to the answer.
— Do NOT make up data. If something isn't in the context, say so.

DASHBOARD CONTROL:
If your response would be significantly more useful with the dashboard filtered to specific data, append a dashboard action at the very end of your response in this exact format (nothing after it):
```dashboard_action
{"type": "filter", "field": "Company", "value": "ExactCompanyName"}
```
or to highlight a page section:
```dashboard_action
{"type": "highlight", "section": "market"}
```
Only include this if it genuinely helps. Do not include it for general questions."""


def call_llm(messages: list[dict], max_retries: int = 3) -> str:
    """Call Groq (Llama 3.3 70B) with exponential backoff on rate-limit."""
    if not GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY not set. Run: export GROQ_API_KEY=gsk_...")

    groq_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

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
