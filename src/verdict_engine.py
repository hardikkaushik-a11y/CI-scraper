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

from team_routing import (
    route_verdict,
    compute_team_relevance,
    TEAM_ORDER,
)
from themes import (
    classify_themes,
    aggregate_themes,
    derive_product_areas,
)

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = "deepseek-chat"  # V4-Flash

# Bump this to force all verdicts to regenerate when scoring logic changes
VERDICT_VERSION = "8"  # v8: tightened theme→product_area mapping (no more cross-pollution)

SIGNALS_PATH = "data/signals.json"
COMPETITIVE_SIGNALS_PATH = "data/competitive_signals.json"
NEWS_PATH = "data/news.json"
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

VERDICT_SYSTEM = """You are Actian's competitive intelligence analyst. Synthesize hiring, newsroom,
and product signals into a structured, evidence-based competitive verdict. Write for VP / CPO / CMO level.
Be specific. Reference actual data (role counts, launch names, dates). No generic phrases.

You will receive:
1. Hiring signals (job postings — what they are building and staffing)
2. Newsroom intelligence (press releases, blogs — official company messaging and announcements)
3. Product/event signals (launches, announcements, partnerships — what they shipped)

## STEP 1: SIGNAL VALIDATION
Only use signals that represent real strategic triggers: product launches, GA, pricing changes,
partnerships, acquisitions, funding, leadership changes. Newsroom items include official press
releases and announcements (not blog posts). Blog posts only count if they explicitly announce
a strategic trigger. Ignore: tutorials, trend pieces, thought leadership, minor UI updates.

Classify impact:
- feature: minor capability addition, low strategic value
- product: meaningful new capability or meaningful expansion
- platform: major architectural shift, new platform layer, category entry
- market: pricing change, acquisition, major partnership, funding round

## STEP 2: HIRING ↔ EVENT CORRELATION
Determine if hiring reinforces the event signal:
- strong: hiring function directly supports the launch (e.g., infra hires + GA, GTM hires + launch)
- moderate: some overlap but indirect (e.g., AI hires + partner announcement)
- weak: signals exist but don't reinforce each other
- none: only one data source present

## STEP 3: STRATEGIC INTERPRETATION
what_is_happening: Grounded ONLY in actual signals. Cite specific roles, launch names, dates.
why_it_matters: How this affects Actian's product positioning, enterprise readiness, deal risk.
primary_interpretation: Most probable strategic explanation for the observed pattern.
alternative_interpretation: MANDATORY — at least one plausible alternative reading.

## STEP 4: COMPETITIVE IMPACT (ACTIAN-SPECIFIC)
- overlap_with_actian: yes/no — and exactly where (data catalog, observability, vector DB, analytics)
- at_risk_segments: be specific (mid-market, enterprise financial services, DACH, etc.)
- type_of_move: defensive | expansion | platform | GTM

## STEP 5: CONFIDENCE
- high: strong evidence from both hiring + events, clear directionality
- medium: one strong signal or multiple weak ones
- low: sparse signals, uncertain direction

## STEP 6: TEAM ROUTING (WHO NEEDS TO SEE THIS)
Based on the signal composition, select which Actian internal teams should be alerted.
Teams: Product, PMM, Marketing, SDRs, Executives.

Routing rules:
- product_launch / open_source_release present → Product, PMM, Marketing
- event signals present → Marketing, SDRs, PMM
- partnership signals present → PMM, SDRs
- funding / acquisition → Executives, PMM
- leadership change → Executives, PMM
- pricing change → SDRs, Marketing, PMM
- Engineering hiring surge (≥20 roles) → Product
- GTM/Sales hiring surge (≥20 roles) → SDRs
- impact_level = platform or market → add Executives
- actian_relevance = high → always include PMM

A verdict can route to multiple teams. Return as array in canonical order:
["Product", "PMM", "Marketing", "SDRs", "Executives"].

Also produce team_relevance — how relevant this verdict is to each team on a 0–5 scale
(0 = ignore, 5 = immediate action). Teams in team_routing get higher scores.

Return ONLY valid JSON — no markdown, no commentary. Use straight ASCII quotes (").
Escape any double quotes inside string values with backslash. No trailing commas.
Do not include any text outside the JSON object.
{
  "company": "<company name>",
  "signal_type": "hiring + event | hiring only | event only | none",
  "impact_level": "feature | product | platform | market",
  "what_is_happening": "<2-3 sentences — specific evidence only>",
  "why_it_matters": "<1-2 sentences — Actian-specific competitive impact>",
  "primary_interpretation": "<1-2 sentences — most probable strategic explanation>",
  "alternative_interpretation": "<1-2 sentences — only include if genuine ambiguity exists; if evidence is unambiguous, state the residual timing/execution uncertainty instead>",
  "hiring_event_correlation": {
    "strength": "strong | moderate | weak | none",
    "explanation": "<1 sentence — include hiring volume in the reasoning, e.g. '22 engineering roles + GA launch = strong'>"
  },
  "competitive_impact": {
    "overlap_with_actian": "<yes/no + specific area>",
    "at_risk_segments": "<specific segments or verticals>",
    "type_of_move": "defensive | expansion | platform | GTM"
  },
  "confidence": "high | medium | low",
  "confidence_reasoning": "<1-2 sentences — signal strength and gaps>",
  "recommended_action": "<1 sentence — who does what, against which specific threat; cite Actian's differentiator; no generic statements>",
  "team_routing": ["Product", "PMM", "Marketing", "SDRs", "Executives"],
  "team_relevance": {
    "product": 0,
    "pmm": 0,
    "marketing": 0,
    "sdrs": 0,
    "executives": 0
  }
}

RULES:
- NO generic phrases like "typically indicates" or "positions them well"
- ALWAYS reference specific evidence ("9 integration roles", "Cortex GA", "Series C $200M")
- correlation strength = strong ONLY when hiring volume ≥ 20 roles AND function matches launch type
- If signals are weak, say so explicitly in confidence_reasoning
- recommended_action must be specific enough to assign to a team and act on this week
- Fewer, stronger insights > more coverage"""


# ══════════════════════════════════════════════════════════════════════════
# DEEPSEEK CALL
# ══════════════════════════════════════════════════════════════════════════

def _call_deepseek(system: str, user_msg: str, max_tokens: int = 1500) -> str:
    if not DEEPSEEK_API_KEY:
        return ""
    try:
        r = httpx.post(
            "https://api.deepseek.com/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEEPSEEK_MODEL,
                "max_tokens": max_tokens,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_msg},
                ],
            },
            timeout=90,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"  [WARN] DeepSeek API call failed: {e}")
        return ""


# ══════════════════════════════════════════════════════════════════════════
# FRESHNESS — hash input signals to detect changes
# ══════════════════════════════════════════════════════════════════════════

def _signal_hash(hiring_signal: dict | None, comp_signals: list[dict], news_items: list[dict]) -> str:
    """Stable hash of input signals — used to skip unchanged companies.
    Include VERDICT_VERSION so logic changes force regeneration.
    """
    payload = {
        "hiring": hiring_signal or {},
        "competitive": sorted(comp_signals, key=lambda x: x.get("url", "")),
        "news": sorted(news_items, key=lambda x: x.get("url", "")),
        "_version": VERDICT_VERSION,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════
# VERDICT GENERATION
# ══════════════════════════════════════════════════════════════════════════

def _build_user_prompt(company: str, product_area: str,
                       hiring_signal: dict | None,
                       comp_signals: list[dict],
                       news_items: list[dict]) -> str:
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
    lines.append("=== NEWSROOM INTELLIGENCE (press releases, official announcements) ===")

    if news_items:
        for news in news_items:
            lines.append(f"[{news.get('news_type', 'unknown').upper()}] {news.get('title', 'N/A')}")
            lines.append(f"  Date: {news.get('published_date', 'N/A')}")
            lines.append(f"  Relevance: {news.get('actian_relevance', 'N/A')}")
            lines.append(f"  Tags: {', '.join(news.get('tags', []))}")
            if news.get('summary'):
                lines.append(f"  Summary: {news.get('summary', 'N/A')}")
            lines.append("")
    else:
        lines.append("No recent newsroom intelligence found.")

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
                      comp_signals: list[dict],
                      news_items: list[dict]) -> dict:
    """
    Rule-based verdict using 6-step competitive intelligence framework.
    Produces the same schema as the Claude prompt. No templates, evidence-driven.
    """
    import re as _re

    # ── STEP 1: HIRING EVIDENCE ───────────────────────────────────────────────
    posting_count    = hiring_signal.get("posting_count", 0)    if hiring_signal else 0
    dominant_fn      = hiring_signal.get("dominant_function", "") if hiring_signal else ""
    dominant_pf      = hiring_signal.get("dominant_product_focus", "") if hiring_signal else ""
    hiring_intensity = hiring_signal.get("hiring_intensity", "low") if hiring_signal else "low"
    signal_summary   = (hiring_signal.get("signal_summary", "") if hiring_signal else "").strip()

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
    eng_fns = {"Engineering", "Product", "Research", "Infrastructure", "Data"}
    has_gtm_hiring = dominant_fn in gtm_fns
    has_eng_hiring = dominant_fn in eng_fns

    # ── STEP 1: COMPETITIVE SIGNAL VALIDATION ────────────────────────────────
    # Weighted by strategic value — blog_posts excluded.
    # News and comp_signals both contribute — they're both "external signals."
    SIGNAL_WEIGHTS = {
        "product_launch":      3,
        "open_source_release": 2,
        "partnership":         2,
        "funding":             2,
        "acquisition":         3,
        "leadership":          1,
        "pricing":             2,
        "event":               1,
        "blog_post":           0,
        "feature":             1,
        "layoff":              1,
    }
    RELEVANCE_MULT = {"high": 1.0, "medium": 0.5, "low": 0.0}

    # Fold news items into the signal bucket — a funding news item is as
    # strategically meaningful as a funding signal. Normalize key: news uses
    # "news_type", comp signals use "type".
    merged_signals: list[dict] = list(comp_signals)
    for n in news_items:
        merged_signals.append({
            "type":             n.get("news_type", "blog_post"),
            "title":            n.get("title", ""),
            "summary":          n.get("summary", ""),
            "actian_relevance": n.get("actian_relevance", "medium"),
            "tags":             n.get("tags", []),
        })

    comp_signal_score = 0.0
    for sig in merged_signals:
        w = SIGNAL_WEIGHTS.get(sig.get("type", "blog_post"), 0)
        m = RELEVANCE_MULT.get(sig.get("actian_relevance", "low"), 0.0)
        comp_signal_score += w * m

    # Classify valid signals by type (from merged source)
    product_launches  = [s for s in merged_signals if s.get("type") == "product_launch"
                         and s.get("actian_relevance") in ("high", "medium")]
    events            = [s for s in merged_signals if s.get("type") == "event"]
    partnerships      = [s for s in merged_signals if s.get("type") == "partnership"]
    funding_signals   = [s for s in merged_signals if s.get("type") in ("funding", "acquisition")]
    oss_releases      = [s for s in merged_signals if s.get("type") == "open_source_release"]
    leadership_moves  = [s for s in merged_signals if s.get("type") == "leadership"]

    has_product_launch = bool(product_launches)
    has_event          = bool(events)
    has_funding        = bool(funding_signals)
    has_partnership    = bool(partnerships)

    # GA + high-impact terms → platform. GA alone (e.g. "new dashboard filters GA") → product.
    _GA_RE = _re.compile(r'\bga\b|general\s+availability', _re.I)
    _HIGH_IMPACT_RE = _re.compile(
        r'platform|governance|catalog|lineage|observability|vector|embedding|'
        r'data\s+quality|lakehouse|warehouse|pipeline|integration\s+layer|'
        r'unified|enterprise\s+grade|data\s+mesh|semantic\s+layer',
        _re.I,
    )
    has_ga_signal = any(
        _GA_RE.search(s.get("title","") + " " + s.get("summary",""))
        and _HIGH_IMPACT_RE.search(s.get("title","") + " " + s.get("summary",""))
        for s in product_launches
    )

    # ── STEP 1: IMPACT LEVEL ─────────────────────────────────────────────────
    # market: funding, acquisition, pricing change, major partnership at scale
    # platform: GA of a high-impact platform layer, or very strong comp signal
    # product: product launches with real capability expansion
    # feature: minor launches, no strategic shift
    has_pricing = any(
        s.get("type") == "pricing" or "pricing" in (s.get("tags") or [])
        for s in merged_signals
    )
    if has_funding or has_pricing:
        impact_level = "market"
    elif has_ga_signal or (has_product_launch and comp_signal_score >= 6):
        impact_level = "platform"
    elif has_product_launch and comp_signal_score >= 2:
        impact_level = "product"
    elif comp_signal_score > 0 or posting_count >= 10:
        impact_level = "feature"
    else:
        impact_level = "feature"  # default — weak signal bucket

    # ── STEP 2: HIRING ↔ EVENT CORRELATION ───────────────────────────────────
    # strong requires both: correct function type AND meaningful hiring volume (≥20 roles)
    # 2 engineers + launch ≠ strong; 20 engineers + launch = strong
    if has_product_launch and has_eng_hiring and hiring_volume_score >= 2:
        corr_strength = "strong"
        corr_explanation = (
            f"{posting_count} {dominant_fn} roles (volume score {hiring_volume_score}/3) directly support "
            f"{len(product_launches)} product launch(es) — substantive engineering buildout behind this release."
        )
    elif has_ga_signal and has_eng_hiring and hiring_volume_score >= 2:
        corr_strength = "strong"
        corr_explanation = (
            f"{posting_count} engineering roles align with a GA-level platform launch — "
            f"signals production-scale infrastructure investment, not just a soft release."
        )
    elif has_product_launch and has_gtm_hiring and hiring_volume_score >= 2:
        corr_strength = "strong"
        corr_explanation = (
            f"{posting_count} GTM roles ({dominant_fn}) paired with product launch — "
            f"coordinated market-entry motion: product ships and sales scales simultaneously."
        )
    elif (has_product_launch or has_ga_signal or has_partnership) and posting_count >= 10:
        corr_strength = "moderate"
        corr_explanation = (
            f"{posting_count} open roles + product/event signal present. "
            f"Hiring function ({dominant_fn or 'unknown'}) is plausibly related but volume "
            f"({hiring_volume_score}/3) is insufficient to confirm at-scale deployment."
        )
    elif has_product_launch and 0 < posting_count < 10:
        corr_strength = "weak"
        corr_explanation = (
            f"Product launch detected but only {posting_count} open roles — "
            f"hiring volume too low to confirm this is a scaled strategic investment vs. a small team release."
        )
    elif posting_count > 0 and comp_signal_score > 0:
        corr_strength = "weak"
        corr_explanation = (
            f"Both dimensions present ({posting_count} roles + event signals) but don't reinforce each other "
            f"— {dominant_fn or 'hiring function'} doesn't directly map to the event type detected."
        )
    elif posting_count > 0 or comp_signal_score > 0:
        corr_strength = "none"
        corr_explanation = (
            "Only one data source present — "
            f"{'hiring data only, no confirmed product events' if posting_count > 0 else 'event signals only, no supporting hiring pattern'}."
        )
    else:
        corr_strength = "none"
        corr_explanation = "Insufficient signals in both dimensions to establish correlation."

    # ── STEP 3: WHAT IS HAPPENING ─────────────────────────────────────────────
    what_parts = []
    if signal_summary:
        what_parts.append(signal_summary)
    elif posting_count > 0:
        fn_pf = f" focused on {dominant_pf}" if dominant_pf and dominant_pf != dominant_fn else ""
        what_parts.append(
            f"{company} has {posting_count} open {dominant_fn} roles{fn_pf}."
        )

    if product_launches:
        launch_names = "; ".join(s.get("title", "")[:70] for s in product_launches[:2] if s.get("title"))
        what_parts.append(f"Product launches detected: {launch_names}.")
    if funding_signals:
        fd = funding_signals[0]
        what_parts.append(
            f"Funding event: {fd.get('title','')[:80] or 'funding round announced'}."
        )
    if partnerships:
        pt = partnerships[0]
        what_parts.append(
            f"Partnership: {pt.get('title','')[:80] or 'new partnership announced'}."
        )
    if events:
        what_parts.append(
            f"{len(events)} upcoming event(s) confirmed — active GTM motion."
        )
    if oss_releases:
        what_parts.append(
            f"{len(oss_releases)} open source release(s) — community/developer expansion signal."
        )

    what_happening = " ".join(what_parts).strip()

    # ── STEP 3: WHY IT MATTERS ────────────────────────────────────────────────
    PA_OVERLAP = {
        "Data Intelligence":  "Actian's data catalog, lineage, and metadata management surface",
        "Data Observability": "Actian's pipeline monitoring and data quality layer",
        "VectorAI":           "Actian Vector — direct competition in enterprise vector retrieval",
        "AI Analyst":         "Actian's analytics and data integration platform",
    }
    overlap_area = PA_OVERLAP.get(product_area, f"Actian's {product_area} surface")
    why_parts = [f"Overlaps {overlap_area}."]
    if impact_level in ("platform", "market"):
        why_parts.append(
            "A platform- or market-level move narrows Actian's differentiation window "
            "and raises the switching cost for shared customers."
        )
    elif has_product_launch:
        why_parts.append(
            "New capability reduces the gap in feature parity — affects Actian's "
            "positioning in competitive evaluations."
        )
    elif has_funding:
        why_parts.append("Fresh capital accelerates their roadmap and GTM capacity.")
    why_matters = " ".join(why_parts)

    # ── STEP 3: PRIMARY INTERPRETATION ───────────────────────────────────────
    if has_product_launch and hiring_volume_score >= 2:
        if has_gtm_hiring:
            primary = (
                f"{company} is executing a coordinated product + GTM expansion in {product_area}: "
                f"{posting_count} open roles (led by {dominant_fn}) paired with recent product launches "
                f"indicate a deliberate market entry push, not just internal tooling."
            )
        else:
            primary = (
                f"{company} is deepening {product_area} product capability: "
                f"{posting_count} {dominant_fn} roles combined with "
                f"{len(product_launches)} product launch(es) signal sustained R&D investment."
            )
    elif has_funding:
        primary = (
            f"{company} received new capital, which typically accelerates both product velocity "
            f"and GTM expansion. With {posting_count} open roles, headcount growth is likely imminent."
        )
    elif has_product_launch:
        primary = (
            f"{company} has shipped new {product_area} capability. "
            f"{'Hiring lags behind the launch — may be pre-revenue or early-access stage.' if posting_count < 10 else f'{posting_count} open roles suggest they are scaling around this launch.'}"
        )
    elif posting_count >= 20:
        primary = (
            f"{company} is growing its {dominant_fn} function aggressively ({posting_count} roles) "
            f"without confirmed product launches — suggests internal platform buildout or "
            f"upcoming launch not yet public."
        )
    else:
        primary = (
            f"{company} shows limited {product_area} activity in this window. "
            f"Signals are insufficient to confirm a strategic directional move."
        )

    # ── STEP 3: ALTERNATIVE INTERPRETATION ──────────────────────────────────
    # Only include when genuine ambiguity exists. When evidence is unambiguous
    # (strong correlation + high hiring volume + confirmed launch), don't force a weak alternative.
    _evidence_is_unambiguous = (
        corr_strength == "strong"
        and hiring_volume_score >= 2
        and has_product_launch
        and has_ga_signal
    )

    if _evidence_is_unambiguous:
        # Evidence is clear — note the one real residual uncertainty rather than a forced alternative
        alternative = (
            f"The main uncertainty is timing, not direction: the GA and hiring pattern confirm "
            f"the strategic intent, but Actian's window to respond depends on how quickly "
            f"{company} converts this into enterprise deals in shared accounts."
        )
    elif has_product_launch and hiring_volume_score >= 2 and not has_ga_signal:
        alternative = (
            f"The hiring surge may reflect post-acquisition integration or backfill, not a net-new "
            f"product investment — without explicit GA or release language, the launch could be "
            f"a beta or limited-access release not yet at enterprise scale."
        )
    elif has_funding:
        alternative = (
            f"Funding could be runway extension rather than growth acceleration — "
            f"if {company} is managing burn, capital may not translate to near-term hiring "
            f"or product velocity. Monitor headcount growth over the next 60 days to confirm."
        )
    elif posting_count >= 20 and not has_product_launch:
        alternative = (
            f"{posting_count} roles without a confirmed product event could indicate "
            f"attrition backfill or a platform migration rather than net-new investment — "
            f"hiring composition (senior vs. mid-level) would clarify intent."
        )
    elif has_product_launch and posting_count < 10:
        alternative = (
            f"Low hiring volume ({posting_count} roles) relative to the launch scale suggests "
            f"this may be a small-team or partner-led release, not a full product investment — "
            f"watch for follow-on hiring in the next 30 days as a confirmation signal."
        )
    else:
        # Weak overall signal — the 'alternative' is that there's simply nothing happening
        alternative = (
            f"Sparse signals may reflect deliberate stealth ahead of a major launch, "
            f"or simply low activity in this window — insufficient data to distinguish between the two."
        )

    # ── STEP 4: COMPETITIVE IMPACT ────────────────────────────────────────────
    PA_OVERLAP_SHORT = {
        "Data Intelligence":  "data catalog and governance",
        "Data Observability": "data quality and pipeline monitoring",
        "VectorAI":           "vector database and AI retrieval",
        "AI Analyst":         "AI-powered analytics and data integration",
    }
    overlap_short = PA_OVERLAP_SHORT.get(product_area, product_area)
    overlap_str = f"yes — {overlap_short}"

    # At-risk segments based on product area + hiring signals
    if product_area == "Data Intelligence":
        at_risk = "enterprise data governance teams, regulated industries (finance, healthcare)"
    elif product_area == "Data Observability":
        at_risk = "mid-market and enterprise data engineering teams running production pipelines"
    elif product_area == "VectorAI":
        at_risk = "enterprise ML/AI teams evaluating vector DB for production RAG workflows"
    elif product_area == "AI Analyst":
        at_risk = "enterprise analytics buyers evaluating cloud-native platforms vs. embedded/edge"
    else:
        at_risk = "enterprise data platform buyers"

    # Type of move
    if has_funding or (has_product_launch and has_gtm_hiring):
        move_type = "expansion"
    elif impact_level == "platform":
        move_type = "platform"
    elif has_gtm_hiring and not has_product_launch:
        move_type = "GTM"
    elif has_product_launch and not has_gtm_hiring:
        move_type = "defensive"
    else:
        move_type = "expansion"

    # ── STEP 5: CONFIDENCE SCORING ────────────────────────────────────────────
    evidence_count = (
        (1 if posting_count >= 20 else 0) +
        (1 if has_product_launch else 0) +
        (1 if has_funding or has_partnership else 0) +
        (1 if corr_strength in ("strong", "moderate") else 0)
    )

    if evidence_count >= 3:
        confidence = "high"
        conf_reasoning = (
            f"Strong compound evidence: {posting_count} open roles + "
            f"{len([s for s in comp_signals if s.get('type') != 'blog_post'])} non-blog event signals "
            f"with {corr_strength} correlation between hiring and events."
        )
    elif evidence_count >= 2:
        confidence = "medium"
        conf_reasoning = (
            f"Moderate evidence: "
            f"{'hiring volume supports the signal' if posting_count >= 10 else 'event signals present without strong hiring support'}. "
            f"Correlation is {corr_strength} — one dimension is weaker."
        )
    else:
        confidence = "low"
        conf_reasoning = (
            f"Sparse signals: "
            f"{'only hiring data available, no confirmed events' if posting_count > 0 and not comp_signals else 'event signals present but hiring volume too low to confirm strategic direction' if comp_signals else 'insufficient data in both dimensions'}. "
            f"Directional hypothesis only."
        )

    # ── STEP 6: RECOMMENDED ACTION ───────────────────────────────────────────
    # One specific line: who does what, against which threat. No generic statements.
    PA_ACTIAN_STRENGTH = {
        "Data Intelligence":  "embedded lineage and governance at the data source",
        "Data Observability": "real-time DQ monitoring with push-based alerting",
        "VectorAI":           "hybrid vector + relational queries without a separate vector DB",
        "AI Analyst":         "edge and embedded analytics without cloud lock-in",
    }
    actian_differentiator = PA_ACTIAN_STRENGTH.get(product_area, f"Actian's {product_area} differentiation")

    if impact_level == "market" and has_funding:
        recommended_action = (
            f"Product: assess roadmap gaps vs. {company}'s funded capabilities within 30 days; "
            f"reinforce Actian's {actian_differentiator} in any joint accounts before {company} accelerates GTM."
        )
    elif impact_level == "platform" and corr_strength == "strong":
        recommended_action = (
            f"PMM + SDRs: refresh {company} battlecard within 2 weeks — "
            f"new platform capability directly challenges Actian's {actian_differentiator}; "
            f"lead with Actian's deployment flexibility as the counter-position."
        )
    elif impact_level == "product" and has_gtm_hiring:
        recommended_action = (
            f"SDRs: treat {company}'s GTM expansion as a displacement trigger — "
            f"prioritize outreach to shared accounts and lead with Actian's {actian_differentiator}."
        )
    elif impact_level == "product":
        recommended_action = (
            f"PMM: verify {company}'s new capability does not undercut Actian's {actian_differentiator} "
            f"in active competitive evaluations; update battlecard if feature gap confirmed."
        )
    elif corr_strength in ("strong", "moderate") and posting_count >= 20:
        recommended_action = (
            f"Product: track {company}'s {dominant_pf or product_area} roadmap over next quarter; "
            f"flag if hiring accelerates or product launch confirmed — Actian's {actian_differentiator} "
            f"is the primary differentiator to reinforce."
        )
    else:
        recommended_action = (
            f"No immediate action required. Monitor {company} for hiring acceleration or "
            f"product launch signals over the next 60 days before escalating."
        )

    # ── SIGNAL TYPE ───────────────────────────────────────────────────────────
    if posting_count > 0 and comp_signals:
        signal_type_str = "hiring + event"
    elif posting_count > 0:
        signal_type_str = "hiring only"
    elif comp_signals:
        signal_type_str = "event only"
    else:
        signal_type_str = "none"

    # ── TEAM ROUTING ──────────────────────────────────────────────────────────
    # Pull everything — news types, comp signal types, hiring function — into
    # a single team_routing list. This is the intelligence layer's final output:
    # "which teams need to see this company's verdict."
    news_types_present = [n.get("news_type", "") for n in news_items if n.get("news_type")]
    comp_types_present = [s.get("type", "") for s in comp_signals if s.get("type")]

    # Pick the strongest Actian relevance across all signals (drives escalation)
    relevance_order = {"high": 3, "medium": 2, "low": 1}
    all_relevances = (
        [n.get("actian_relevance", "low") for n in news_items]
        + [s.get("actian_relevance", "low") for s in comp_signals]
    )
    top_relevance = "low"
    for r in all_relevances:
        if relevance_order.get(r, 0) > relevance_order.get(top_relevance, 0):
            top_relevance = r

    team_routing = route_verdict(
        news_types=news_types_present,
        comp_signal_types=comp_types_present,
        hiring_function=dominant_fn,
        posting_count=posting_count,
        impact_level=impact_level,
        actian_relevance=top_relevance,
    )

    # Threat level — reuse hiring signal's threat_level if available, else derive
    hiring_threat = (hiring_signal or {}).get("threat_level", "")
    if hiring_threat:
        threat_for_relevance = hiring_threat
    elif impact_level == "market":
        threat_for_relevance = "critical"
    elif impact_level == "platform":
        threat_for_relevance = "high"
    elif impact_level == "product":
        threat_for_relevance = "medium"
    else:
        threat_for_relevance = "low"

    team_relevance = compute_team_relevance(
        team_routing=team_routing,
        impact_level=impact_level,
        threat_level=threat_for_relevance,
        posting_count=posting_count,
    )

    return {
        "company":                company,
        "signal_type":            signal_type_str,
        "impact_level":           impact_level,
        "what_is_happening":      what_happening,
        "why_it_matters":         why_matters,
        "primary_interpretation": primary,
        "alternative_interpretation": alternative,
        "hiring_event_correlation": {
            "strength":    corr_strength,
            "explanation": corr_explanation,
        },
        "competitive_impact": {
            "overlap_with_actian": overlap_str,
            "at_risk_segments":    at_risk,
            "type_of_move":        move_type,
        },
        "confidence":           confidence,
        "confidence_reasoning": conf_reasoning,
        "recommended_action":   recommended_action,
        "team_routing":         team_routing,
        "team_relevance":       team_relevance,
    }


def generate_verdict(company: str, product_area: str,
                     hiring_signal: dict | None,
                     comp_signals: list[dict],
                     news_items: list[dict]) -> dict | None:
    """Generate verdict via Claude if API key available, else use fallback."""
    if not DEEPSEEK_API_KEY:
        print(f"  [FALLBACK] {company} — using rule-based verdict logic")
        return _fallback_verdict(company, product_area, hiring_signal, comp_signals, news_items)

    prompt = _build_user_prompt(company, product_area, hiring_signal, comp_signals, news_items)
    raw = _call_deepseek(VERDICT_SYSTEM, prompt)
    if not raw:
        print(f"  [FALLBACK] {company} — DeepSeek call failed, using rule-based logic")
        return _fallback_verdict(company, product_area, hiring_signal, comp_signals, news_items)

    # Strip markdown fences if model wrapped it
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

    # Extract first {...} block in case there's preamble/trailing prose
    import re as _re
    m = _re.search(r"\{[\s\S]*\}", text)
    if m:
        text = m.group(0)

    parsed = None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Best-effort repair: trailing commas, smart quotes, unescaped newlines in strings
        repaired = text
        repaired = _re.sub(r",(\s*[}\]])", r"\1", repaired)            # trailing commas
        repaired = repaired.replace("“", '"').replace("”", '"')  # smart quotes
        repaired = repaired.replace("‘", "'").replace("’", "'")
        try:
            parsed = json.loads(repaired)
            print(f"  [REPAIR] {company} — JSON repaired after retry")
        except json.JSONDecodeError as e:
            print(f"  [FALLBACK] {company} — JSON parse failed, using rule-based logic: {e}")
            return _fallback_verdict(company, product_area, hiring_signal, comp_signals, news_items)

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

    news_by_company: dict[str, list[dict]] = {}
    if os.path.exists(NEWS_PATH):
        with open(NEWS_PATH) as f:
            for news in json.load(f):
                news_by_company.setdefault(news["company"], []).append(news)
    total_news = sum(len(v) for v in news_by_company.values())
    print(f"[verdict_engine] Loaded {total_news} newsroom items across {len(news_by_company)} companies")

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
        news_items = news_by_company.get(company, [])
        new_hash = _signal_hash(hiring_signal, comp_signals, news_items)

        existing = existing_verdicts.get(company, {})
        stored_hash = existing.get("_input_hash", "")

        if stored_hash == new_hash and existing:
            print(f"  [SKIP] {company} — no signal changes since last run")
            output_verdicts.append(existing)
            skipped += 1
            continue

        print(f"  [GEN]  {company} ({product_area}) — {len(comp_signals)} competitive signals, {len(news_items)} news items")
        verdict_data = generate_verdict(company, product_area, hiring_signal, comp_signals, news_items)

        if verdict_data:
            # ── Team routing — guarantee it exists even if Claude omitted it ──
            impact_level = verdict_data.get("impact_level", "feature")
            model_routing = verdict_data.get("team_routing") or []
            # Validate model routing: must be subset of allowed teams
            model_routing = [t for t in model_routing if t in TEAM_ORDER]

            if not model_routing:
                # Claude didn't return routing — compute deterministically
                news_types_present = [n.get("news_type", "") for n in news_items if n.get("news_type")]
                comp_types_present = [s.get("type", "") for s in comp_signals if s.get("type")]
                relevance_order = {"high": 3, "medium": 2, "low": 1}
                all_relevances = (
                    [n.get("actian_relevance", "low") for n in news_items]
                    + [s.get("actian_relevance", "low") for s in comp_signals]
                )
                top_relevance = "low"
                for r in all_relevances:
                    if relevance_order.get(r, 0) > relevance_order.get(top_relevance, 0):
                        top_relevance = r
                model_routing = route_verdict(
                    news_types=news_types_present,
                    comp_signal_types=comp_types_present,
                    hiring_function=(hiring_signal or {}).get("dominant_function", ""),
                    posting_count=(hiring_signal or {}).get("posting_count", 0),
                    impact_level=impact_level,
                    actian_relevance=top_relevance,
                )

            # Team relevance — same fallback pattern
            model_relevance = verdict_data.get("team_relevance") or {}
            if not model_relevance or not all(k in model_relevance for k in ("product","pmm","marketing","sdrs","executives")):
                hiring_threat = (hiring_signal or {}).get("threat_level", "")
                threat_for_relevance = hiring_threat or {
                    "market": "critical", "platform": "high",
                    "product": "medium", "feature": "low",
                }.get(impact_level, "low")
                model_relevance = compute_team_relevance(
                    team_routing=model_routing,
                    impact_level=impact_level,
                    threat_level=threat_for_relevance,
                    posting_count=(hiring_signal or {}).get("posting_count", 0),
                )

            # Aggregate themes from all signals + news for this company,
            # then derive multi-area product list (primary + theme-driven).
            all_signal_items = list(comp_signals) + list(news_items)
            verdict_themes = aggregate_themes(all_signal_items)
            # If no themes from signals, also try classifying the verdict text itself
            if not verdict_themes and verdict_data.get("what_is_happening"):
                verdict_themes = classify_themes(
                    verdict_data.get("what_is_happening", ""),
                    verdict_data.get("primary_interpretation", ""),
                )
            multi_areas = derive_product_areas(product_area, verdict_themes)

            verdict = {
                "company":                    company,
                "product_area":               product_area,
                "product_areas":              multi_areas,
                "themes":                     verdict_themes,
                "signal_type":                verdict_data.get("signal_type", "none"),
                "impact_level":               impact_level,
                "what_is_happening":          verdict_data.get("what_is_happening", ""),
                "why_it_matters":             verdict_data.get("why_it_matters", ""),
                "primary_interpretation":     verdict_data.get("primary_interpretation", ""),
                "alternative_interpretation": verdict_data.get("alternative_interpretation", ""),
                "hiring_event_correlation":   verdict_data.get("hiring_event_correlation", {
                    "strength": "none", "explanation": ""
                }),
                "competitive_impact":         verdict_data.get("competitive_impact", {
                    "overlap_with_actian": "", "at_risk_segments": "", "type_of_move": ""
                }),
                "confidence":                 verdict_data.get("confidence", "low"),
                "confidence_reasoning":       verdict_data.get("confidence_reasoning", ""),
                "recommended_action":         verdict_data.get("recommended_action", ""),
                "team_routing":               model_routing,
                "team_relevance":             model_relevance,
                "last_updated": today,
                "_input_hash": new_hash,
            }
            output_verdicts.append(verdict)
            regenerated += 1
            print(f"         impact={verdict['impact_level']} | confidence={verdict['confidence']} | correlation={verdict['hiring_event_correlation'].get('strength','?')} | teams={','.join(model_routing)}")
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
