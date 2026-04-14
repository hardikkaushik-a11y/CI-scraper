"""
verdict_engine.py — Phase 2.5: Combined Intelligence Verdict Layer
──────────────────────────────────────────────────────────────────
• Reads data/signals.json (hiring intelligence, Claude Opus)
• Reads data/competitive_signals.json (launch/event intelligence, Phase 2)
• For each of the 11 V2 companies: calls Claude Sonnet with both inputs
• Freshness rule: skips regeneration if neither input changed since last run
• Outputs data/intelligence_verdicts.json
"""

import hashlib
import json
import os
from datetime import date

import httpx

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SONNET_MODEL = "claude-sonnet-4-6"

SIGNALS_PATH = "data/signals.json"
COMPETITIVE_SIGNALS_PATH = "data/competitive_signals.json"
VERDICTS_PATH = "data/intelligence_verdicts.json"

# V2 allowed companies — exactly matches dashboard_v2 allowlist
V2_PRODUCT_AREA_MAP = {
    "Atlan":       "Data Intelligence",
    "Collibra":    "Data Intelligence",
    "Alation":     "Data Intelligence",
    "Monte Carlo": "Data Observability",
    "Bigeye":      "Data Observability",
    "Acceldata":   "Data Observability",
    "Pinecone":    "VectorAI",
    "Qdrant":      "VectorAI",
    "Milvus":      "VectorAI",
    "Snowflake":   "AI Analyst",
    "Databricks":  "AI Analyst",
}

VERDICT_SYSTEM = """You are Actian's competitive intelligence analyst. Your job is to synthesize
hiring signals and product launch signals into one sharp, actionable verdict per competitor.

You write for a senior audience (VP, CPO, CMO). Be specific, direct, and ruthless about what
matters to Actian. Do not hedge. Do not say "monitor closely" as an action. Give a concrete move.

You will receive:
1. Hiring signals for a company (from job postings — what they are building and staffing)
2. Product/event signals (recent launches, announcements, events — what they shipped or announced)

Return ONLY a valid JSON object matching this exact schema — no markdown, no extra text:
{
  "verdict": "<1 sharp sentence — what is happening right now>",
  "what_is_happening": "<specific evidence from hiring + launches combined, 2-3 sentences>",
  "why_it_matters": "<specific to Actian's competitive position, 1-2 sentences>",
  "actian_action": "<concrete action for Actian — who does it, what they do, by when>",
  "threat_level": "<low | medium | high | critical>",
  "top_signals": ["<signal 1>", "<signal 2>", "<signal 3>"],
  "team_routing": ["<product | pmm | sdrs | marketing | executives>"]
}

threat_level rules:
- critical: imminent threat to Actian revenue or positioning, requires executive action this week
- high: significant competitive move underway, requires action this quarter
- medium: worth watching, plan a response within 6 months
- low: minimal immediate impact

team_routing rules (can be multiple):
- product: roadmap direction, engineering investment, technical launches
- pmm: messaging shifts, launches, battlecard needs
- sdrs: events (outreach opportunity), expansion signals, GTM hires
- marketing: events, campaigns, messaging plays
- executives: critical or high threats with revenue implications"""


# ══════════════════════════════════════════════════════════════════════════
# CLAUDE CALL (exact pattern from enrich.py)
# ══════════════════════════════════════════════════════════════════════════

def _call_claude(model: str, system: str, user_msg: str, max_tokens: int = 1500) -> str:
    if not ANTHROPIC_API_KEY:
        return ""
    try:
        r = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": max_tokens,
                "system": system,
                "messages": [{"role": "user", "content": user_msg}],
            },
            timeout=90,
        )
        r.raise_for_status()
        return r.json()["content"][0]["text"].strip()
    except Exception as e:
        print(f"  [WARN] Claude API call failed ({model}): {e}")
        return ""


# ══════════════════════════════════════════════════════════════════════════
# FRESHNESS — hash input signals to detect changes
# ══════════════════════════════════════════════════════════════════════════

def _signal_hash(hiring_signal: dict | None, comp_signals: list[dict]) -> str:
    """Stable hash of input signals — used to skip unchanged companies."""
    payload = {
        "hiring": hiring_signal or {},
        "competitive": sorted(comp_signals, key=lambda x: x.get("url", "")),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════
# VERDICT GENERATION
# ══════════════════════════════════════════════════════════════════════════

def _build_user_prompt(company: str, product_area: str,
                       hiring_signal: dict | None,
                       comp_signals: list[dict]) -> str:
    lines = [f"Company: {company}", f"Product Area: {product_area}", ""]

    if hiring_signal:
        lines.append("=== HIRING SIGNALS ===")
        lines.append(f"Job count: {hiring_signal.get('posting_count', 0)}")
        lines.append(f"Signal summary: {hiring_signal.get('signal_summary', 'N/A')}")
        lines.append(f"Dominant function: {hiring_signal.get('dominant_function', 'N/A')}")
        lines.append(f"Dominant product focus: {hiring_signal.get('dominant_product_focus', 'N/A')}")
        lines.append(f"Hiring intensity: {hiring_signal.get('hiring_intensity', 'N/A')}")
        lines.append(f"Existing threat level (hiring only): {hiring_signal.get('threat_level', 'N/A')}")

        implications = hiring_signal.get("implications", [])
        if implications:
            lines.append("Key implications:")
            for imp in implications[:4]:
                lines.append(f"  - {imp}")

        watch_for = hiring_signal.get("watch_for", [])
        if watch_for:
            lines.append("Watch for:")
            for w in watch_for[:3]:
                lines.append(f"  - {w}")

        roadmap = hiring_signal.get("roadmap", {})
        if roadmap:
            lines.append(f"Roadmap direction: {roadmap.get('direction', 'N/A')}")
            lines.append(f"Roadmap confidence: {roadmap.get('confidence', 'N/A')}")
    else:
        lines.append("=== HIRING SIGNALS ===")
        lines.append("No hiring signal data available for this company.")

    lines.append("")
    lines.append("=== COMPETITIVE SIGNALS (launches, events, announcements) ===")

    if comp_signals:
        for sig in comp_signals:
            lines.append(f"[{sig.get('type', 'unknown').upper()}] {sig.get('title', 'N/A')}")
            lines.append(f"  Date: {sig.get('published_date', 'N/A')}")
            lines.append(f"  Relevance: {sig.get('actian_relevance', 'N/A')}")
            lines.append(f"  Summary: {sig.get('summary', 'N/A')}")
            if sig.get("event_date"):
                lines.append(f"  Event date: {sig['event_date']}")
            lines.append("")
    else:
        lines.append("No recent launch or event signals found.")

    lines.append("")
    lines.append("Based on the above, produce a combined intelligence verdict for Actian's leadership team.")
    return "\n".join(lines)


def _fallback_verdict(company: str, product_area: str,
                      hiring_signal: dict | None,
                      comp_signals: list[dict]) -> dict:
    """
    Rule-based verdict generation (fallback when no API key).
    Synthesizes hiring signals + competitive signals using heuristics.
    """
    # Extract hiring threat level (already computed by enrich.py)
    hiring_threat = hiring_signal.get("threat_level", "medium") if hiring_signal else "low"
    threat_map = {"low": 1, "medium": 2, "high": 3, "critical": 4}
    threat_score = threat_map.get(hiring_threat, 2)

    # Boost threat if there are recent competitive signals
    if comp_signals:
        high_relevance_sigs = [s for s in comp_signals if s.get("actian_relevance") == "high"]
        threat_score += len(high_relevance_sigs)

    # Determine final threat level
    if threat_score >= 5:
        final_threat = "critical"
    elif threat_score >= 4:
        final_threat = "high"
    elif threat_score >= 2:
        final_threat = "medium"
    else:
        final_threat = "low"

    # Build top signals (combine hiring + competitive)
    top_signals = []
    if hiring_signal:
        implications = hiring_signal.get("implications", [])
        if implications:
            top_signals.append(implications[0][:80])

    for sig in comp_signals[:2]:
        top_signals.append(sig.get("title", "Unknown signal")[:80])

    # Determine team routing based on threat and signals
    team_routing = ["product"]
    if final_threat in ["critical", "high"]:
        team_routing.extend(["pmm", "sdrs"])
    if any(s.get("type") == "event" for s in comp_signals):
        team_routing.append("marketing")
    if final_threat == "critical":
        team_routing.append("executives")

    team_routing = list(set(team_routing))  # dedupe

    # Build what_is_happening
    what_happening = ""
    if hiring_signal:
        posting_count = hiring_signal.get("posting_count", 0)
        dominant_fn = hiring_signal.get("dominant_function", "Unknown")
        dominant_pf = hiring_signal.get("dominant_product_focus", "Unknown")
        what_happening = f"{company} has {posting_count} open roles, heavily focused on {dominant_fn} and {dominant_pf}."

    if comp_signals:
        recent_launches = [s for s in comp_signals if s.get("type") in ["product_launch", "event"]]
        if recent_launches:
            what_happening += f" Recently announced {len(recent_launches)} product launches/events."

    # Build why_it_matters
    why_matters = f"{company} ({product_area}) is a direct competitor to Actian in this market segment."
    if final_threat == "critical":
        why_matters += " Immediate action required."
    elif final_threat == "high":
        why_matters += " Significant competitive pressure expected this quarter."

    # Build actian_action
    if final_threat == "critical":
        actian_action = "Immediate: Brief sales team + prepare competitive battlecard. Product: Assess roadmap impact."
    elif final_threat == "high":
        actian_action = "This quarter: Add to competitive review. Product: Monitor for feature parity gaps."
    else:
        actian_action = "Quarterly review cycle. Monitor for escalation signals."

    # Build verdict
    if final_threat == "critical":
        verdict = f"{company} is executing a major competitive play in {product_area} — immediate threat to Actian's market position."
    elif final_threat == "high":
        verdict = f"{company} is accelerating investment in {product_area} — significant competitive pressure building."
    elif final_threat == "medium":
        verdict = f"{company} is actively investing in {product_area} — monitor closely for escalation."
    else:
        verdict = f"{company} maintains steady hiring in {product_area} — not an immediate threat."

    return {
        "verdict": verdict,
        "what_is_happening": what_happening,
        "why_it_matters": why_matters,
        "actian_action": actian_action,
        "threat_level": final_threat,
        "top_signals": top_signals[:3],
        "team_routing": team_routing,
    }


def generate_verdict(company: str, product_area: str,
                     hiring_signal: dict | None,
                     comp_signals: list[dict]) -> dict | None:
    """Generate verdict via Claude if API key available, else use fallback."""
    if not ANTHROPIC_API_KEY:
        print(f"  [FALLBACK] {company} — using rule-based verdict logic")
        return _fallback_verdict(company, product_area, hiring_signal, comp_signals)

    prompt = _build_user_prompt(company, product_area, hiring_signal, comp_signals)
    raw = _call_claude(SONNET_MODEL, VERDICT_SYSTEM, prompt)
    if not raw:
        print(f"  [FALLBACK] {company} — Claude call failed, using rule-based logic")
        return _fallback_verdict(company, product_area, hiring_signal, comp_signals)

    # Strip markdown fences if Claude wrapped it
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  [FALLBACK] {company} — JSON parse failed, using rule-based logic: {e}")
        return _fallback_verdict(company, product_area, hiring_signal, comp_signals)

    return parsed


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    today = date.today().isoformat()

    # Load inputs
    signals_by_company: dict[str, dict] = {}
    if os.path.exists(SIGNALS_PATH):
        with open(SIGNALS_PATH) as f:
            for sig in json.load(f):
                signals_by_company[sig["company"]] = sig
    print(f"[verdict_engine] Loaded {len(signals_by_company)} hiring signals")

    comp_signals_by_company: dict[str, list[dict]] = {}
    if os.path.exists(COMPETITIVE_SIGNALS_PATH):
        with open(COMPETITIVE_SIGNALS_PATH) as f:
            for sig in json.load(f):
                comp_signals_by_company.setdefault(sig["company"], []).append(sig)
    total_comp = sum(len(v) for v in comp_signals_by_company.values())
    print(f"[verdict_engine] Loaded {total_comp} competitive signals across {len(comp_signals_by_company)} companies")

    # Load existing verdicts (for freshness check)
    existing_verdicts: dict[str, dict] = {}
    if os.path.exists(VERDICTS_PATH):
        with open(VERDICTS_PATH) as f:
            for v in json.load(f):
                existing_verdicts[v["company"]] = v
    print(f"[verdict_engine] Found {len(existing_verdicts)} existing verdicts")

    # No API key check needed — fallback logic handles both cases

    output_verdicts = []
    regenerated = 0
    skipped = 0

    for company, product_area in V2_PRODUCT_AREA_MAP.items():
        hiring_signal = signals_by_company.get(company)
        comp_signals = comp_signals_by_company.get(company, [])
        new_hash = _signal_hash(hiring_signal, comp_signals)

        existing = existing_verdicts.get(company, {})
        stored_hash = existing.get("_input_hash", "")

        if stored_hash == new_hash and existing:
            print(f"  [SKIP] {company} — no signal changes since last run")
            output_verdicts.append(existing)
            skipped += 1
            continue

        print(f"  [GEN]  {company} ({product_area}) — {len(comp_signals)} competitive signals")
        verdict_data = generate_verdict(company, product_area, hiring_signal, comp_signals)

        if verdict_data:
            verdict = {
                "company": company,
                "product_area": product_area,
                "verdict": verdict_data.get("verdict", ""),
                "what_is_happening": verdict_data.get("what_is_happening", ""),
                "why_it_matters": verdict_data.get("why_it_matters", ""),
                "actian_action": verdict_data.get("actian_action", ""),
                "threat_level": verdict_data.get("threat_level", "medium"),
                "top_signals": verdict_data.get("top_signals", []),
                "team_routing": verdict_data.get("team_routing", []),
                "last_updated": today,
                "_input_hash": new_hash,
            }
            output_verdicts.append(verdict)
            regenerated += 1
            print(f"         threat_level={verdict['threat_level']} | routing={verdict['team_routing']}")
        else:
            # Keep old verdict if generation failed
            if existing:
                output_verdicts.append(existing)
                print(f"  [KEEP] {company} — generation failed, keeping previous verdict")
            else:
                print(f"  [FAIL] {company} — no verdict generated and no previous verdict to keep")

    with open(VERDICTS_PATH, "w") as f:
        json.dump(output_verdicts, f, indent=2, ensure_ascii=False)

    print(f"\n[verdict_engine] Done — {regenerated} regenerated, {skipped} skipped (unchanged)")
    print(f"[verdict_engine] Wrote {len(output_verdicts)} verdicts → {VERDICTS_PATH}")


if __name__ == "__main__":
    main()
