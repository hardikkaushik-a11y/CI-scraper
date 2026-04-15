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

# Bump this to force all verdicts to regenerate when scoring logic changes
VERDICT_VERSION = "2"

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
    """Stable hash of input signals — used to skip unchanged companies.
    Include VERDICT_VERSION so logic changes force regeneration.
    """
    payload = {
        "hiring": hiring_signal or {},
        "competitive": sorted(comp_signals, key=lambda x: x.get("url", "")),
        "_version": VERDICT_VERSION,
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
    Evidence-based verdict. Threat level requires compound evidence.
    Blog posts never drive threat level. No generic templates.
    """
    import re as _re

    # ── HIRING EVIDENCE ───────────────────────────────────────────────────────
    posting_count  = hiring_signal.get("posting_count", 0)  if hiring_signal else 0
    dominant_fn    = hiring_signal.get("dominant_function", "")  if hiring_signal else ""
    dominant_pf    = hiring_signal.get("dominant_product_focus", "")  if hiring_signal else ""
    hiring_intensity = hiring_signal.get("hiring_intensity", "low") if hiring_signal else "low"

    # Volume score 0-3 based on posting count
    if posting_count >= 40:
        hiring_volume_score = 3
    elif posting_count >= 20:
        hiring_volume_score = 2
    elif posting_count >= 10:
        hiring_volume_score = 1
    else:
        hiring_volume_score = 0

    gtm_fns = {"Sales", "Marketing", "Go-to-Market", "Customer Success",
               "Revenue", "Business Development"}
    has_gtm_hiring = dominant_fn in gtm_fns

    # ── COMPETITIVE SIGNAL EVIDENCE ───────────────────────────────────────────
    # Weighted scoring — blog_posts contribute 0, product launches are primary
    SIGNAL_WEIGHTS = {
        "product_launch":     3,
        "open_source_release": 2,
        "partnership":         2,
        "funding":             2,
        "event":               1,  # GTM signal, not a product threat on its own
        "blog_post":           0,  # never drives threat level
    }
    RELEVANCE_MULT = {"high": 1.0, "medium": 0.5, "low": 0.0}

    comp_signal_score = 0.0
    for sig in comp_signals:
        w = SIGNAL_WEIGHTS.get(sig.get("type", "blog_post"), 0)
        m = RELEVANCE_MULT.get(sig.get("actian_relevance", "low"), 0.0)
        comp_signal_score += w * m

    has_product_launch = any(
        s.get("type") == "product_launch"
        and s.get("actian_relevance") in ("high", "medium")
        for s in comp_signals
    )
    has_event = any(s.get("type") == "event" for s in comp_signals)
    has_funding = any(s.get("type") == "funding" for s in comp_signals)

    # ── THREAT CLASSIFICATION — COMPOUND EVIDENCE REQUIRED ───────────────────
    # critical: large scale + product movement + GTM expansion together
    # high:     strong hiring + product signal, or strong comp signals alone
    # medium:   one moderate signal present
    # low:      insufficient evidence
    if (
        hiring_volume_score >= 3
        and comp_signal_score >= 3
        and has_product_launch
    ):
        final_threat = "critical"
    elif (
        (hiring_volume_score >= 2 and comp_signal_score >= 2)
        or (hiring_volume_score >= 3 and (has_gtm_hiring or hiring_intensity == "high"))
        or (comp_signal_score >= 5 and has_product_launch)
    ):
        final_threat = "high"
    elif hiring_volume_score >= 1 or comp_signal_score >= 1:
        final_threat = "medium"
    else:
        final_threat = "low"

    # ── TOP SIGNALS — FILTERED FOR QUALITY ───────────────────────────────────
    # Prefer launches > partnerships > events; skip blog posts and malformed titles
    SIGNAL_PRIORITY_ORDER = [
        "product_launch", "funding", "open_source_release",
        "partnership", "event", "blog_post",
    ]
    sorted_comp = sorted(
        comp_signals,
        key=lambda s: (
            SIGNAL_PRIORITY_ORDER.index(s.get("type", "blog_post"))
            if s.get("type", "blog_post") in SIGNAL_PRIORITY_ORDER else 99,
            {"high": 0, "medium": 1, "low": 2}.get(s.get("actian_relevance", "low"), 2),
        )
    )

    top_signals = []
    for sig in sorted_comp:
        if len(top_signals) >= 3:
            break
        title = (sig.get("title") or "").strip()
        # Filter malformed or truncated titles
        if len(title) < 20:
            continue
        if title.endswith("…") or title.endswith("..."):
            continue
        # Filter "Word Month Day" patterns (e.g. "Product Apr 15")
        if _re.match(r'^\w+\s+[A-Z][a-z]{2}\s+\d+', title) and len(title) < 35:
            continue
        top_signals.append(title[:120])

    # Fall back to hiring implications if no usable comp signals
    if not top_signals and hiring_signal:
        for imp in (hiring_signal.get("implications") or [])[:2]:
            if imp and len(imp) > 20:
                # Truncate at word boundary to avoid mid-word cuts
                if len(imp) > 150:
                    cut = imp[:150].rsplit(" ", 1)[0]
                    top_signals.append(cut + "…")
                else:
                    top_signals.append(imp)

    # ── OUTPUT GATING — downgrade if evidence is thin ────────────────────────
    if not top_signals:
        if final_threat == "critical":
            final_threat = "high"
        elif final_threat == "high":
            final_threat = "medium"

    # ── WHAT IS HAPPENING ────────────────────────────────────────────────────
    what_parts = []
    if posting_count > 0:
        signal_summary = hiring_signal.get("signal_summary", "") if hiring_signal else ""
        if signal_summary:
            # Use the pre-computed summary from enrich.py — it's specific
            what_parts.append(signal_summary)
        else:
            fn_pf = f" ({dominant_pf})" if dominant_pf and dominant_pf != dominant_fn else ""
            what_parts.append(
                f"{company} has {posting_count} open roles concentrated in {dominant_fn}{fn_pf}."
            )

    if comp_signals:
        launches = [
            s for s in comp_signals
            if s.get("type") == "product_launch"
            and s.get("actian_relevance") != "low"
        ]
        events = [s for s in comp_signals if s.get("type") == "event"]
        partnerships = [s for s in comp_signals if s.get("type") == "partnership"]

        if launches:
            names = "; ".join(
                s.get("title", "")[:60] for s in launches[:2] if s.get("title")
            )
            what_parts.append(f"Recent product moves: {names}.")
        if events:
            what_parts.append(f"{len(events)} upcoming event(s) signaling customer-facing GTM activity.")
        if partnerships:
            what_parts.append("New partnership announced — potential integration play in this space.")

    what_happening = " ".join(what_parts)

    # Downgrade if what_is_happening is empty — not enough to produce a real verdict
    if not what_happening.strip():
        if final_threat in ("critical", "high"):
            final_threat = "medium"

    # ── WHY IT MATTERS — PRODUCT-AREA SPECIFIC ───────────────────────────────
    PA_FRAMING = {
        "Data Intelligence":  "data catalog and governance — directly overlaps Actian's lineage, metadata, and integration surface",
        "Data Observability": "pipeline monitoring and data quality — adjacent to Actian's observability layer",
        "VectorAI":           "vector databases and AI-native retrieval — Actian's Vector capability is in direct competition",
        "AI Analyst":         "AI-powered analytics and data lakehouse — directly challenges Actian's analytics and integration platform",
    }
    framing = PA_FRAMING.get(product_area, f"the {product_area} market")
    why_parts = [f"{company} competes with Actian in {framing}."]
    if has_product_launch:
        why_parts.append("New product capabilities narrow Actian's differentiation window.")
    elif has_funding:
        why_parts.append("Fresh capital signals accelerated product investment ahead.")
    why_matters = " ".join(why_parts)

    # ── ACTIAN ACTION — EVIDENCE-DRIVEN ──────────────────────────────────────
    action_parts = []
    if final_threat == "critical":
        action_parts.append(f"Executives: trigger immediate {product_area} competitive review.")
        if has_product_launch:
            action_parts.append("PMM: update battlecard with new capability gaps within 2 weeks.")
        action_parts.append(f"SDRs: use {company}'s product moves as displacement trigger in active deals.")
    elif final_threat == "high":
        if has_product_launch:
            action_parts.append(f"PMM: refresh {company} battlecard — new product signals identified.")
        if has_event:
            action_parts.append(f"SDRs: monitor {company} events for attendee outreach opportunity.")
        action_parts.append(
            f"Product: review {dominant_pf or product_area} overlap against Actian roadmap for gaps."
        )
    elif final_threat == "medium":
        action_parts.append(
            f"Product: include {company}'s {dominant_pf or product_area} trajectory in next quarterly review."
        )
        if has_product_launch:
            action_parts.append("PMM: verify new launch does not affect current deal positioning.")
    else:
        action_parts.append(
            f"No immediate action needed. Flag if {company} hiring or launch volume increases."
        )
    actian_action = " ".join(action_parts)

    # ── VERDICT SENTENCE — SPECIFIC TO ACTUAL DATA ───────────────────────────
    if final_threat == "critical":
        if has_product_launch:
            verdict = (
                f"{company} is shipping new {product_area} capabilities while scaling to "
                f"{posting_count} open roles — compounding competitive pressure on Actian."
            )
        else:
            verdict = (
                f"{company} is aggressively scaling ({posting_count} roles) in {product_area} "
                f"with GTM expansion — direct threat to Actian's pipeline."
            )
    elif final_threat == "high":
        if has_product_launch:
            verdict = (
                f"{company} is pairing {posting_count}-role hiring with new product launches "
                f"in {product_area} — significant and directional."
            )
        else:
            verdict = (
                f"{company} is building out {dominant_fn} at scale ({posting_count} roles) "
                f"in {product_area} — competitive presence is growing."
            )
    elif final_threat == "medium":
        if comp_signal_score > 0 and posting_count > 0:
            verdict = (
                f"{company} shows early {product_area} signals ({posting_count} roles + "
                f"recent activity) — not urgent but worth tracking."
            )
        elif posting_count > 0:
            verdict = (
                f"{company} has {posting_count} open roles in {product_area}; "
                f"hiring-only signal — no product movement confirmed yet."
            )
        else:
            verdict = (
                f"{company} is showing early {product_area} signals — "
                f"insufficient volume for immediate concern."
            )
    else:
        if posting_count > 0:
            verdict = (
                f"{company} has minimal {product_area} activity ({posting_count} roles, "
                f"low signal) — not a current competitive priority."
            )
        else:
            verdict = (
                f"{company} shows no significant {product_area} signals in this window."
            )

    # ── TEAM ROUTING ─────────────────────────────────────────────────────────
    team_routing = []
    if final_threat in ("critical", "high", "medium"):
        team_routing.append("product")
    if final_threat in ("critical", "high"):
        if has_product_launch or dominant_pf:
            team_routing.append("pmm")
        team_routing.append("sdrs")
    if has_event:
        if "marketing" not in team_routing:
            team_routing.append("marketing")
        if "sdrs" not in team_routing:
            team_routing.append("sdrs")
    if final_threat == "critical":
        team_routing.append("executives")
    # dedupe preserving order
    seen: set[str] = set()
    team_routing = [x for x in team_routing if not (x in seen or seen.add(x))]

    return {
        "verdict":          verdict,
        "what_is_happening": what_happening,
        "why_it_matters":   why_matters,
        "actian_action":    actian_action,
        "threat_level":     final_threat,
        "top_signals":      top_signals[:3],
        "team_routing":     team_routing,
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
