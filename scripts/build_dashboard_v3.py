#!/usr/bin/env python3
"""
build_dashboard_v3.py — Actian CI Platform v3 build script

Reads:
  data/signals.json
  data/intelligence_verdicts.json
  data/competitive_signals.json
  data/news.json

Writes:
  dashboard/v3/dashboard_v3.html
"""

import json
import re
import sys
import csv
from pathlib import Path
from datetime import datetime, date
from collections import defaultdict

# Pull shared country normalizer from src/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from geo import country_from_location

# ── Repo root (script lives in scripts/) ────────────────────────────────────
REPO = Path(__file__).parent.parent
DATA_DIR = REPO / "data"
TEMPLATE = REPO / "dashboard" / "v3" / "template_v3.html"
OUTPUT = REPO / "dashboard" / "v3" / "dashboard_v3.html"

# ── Product area map ─────────────────────────────────────────────────────────
V2_PRODUCT_AREA_MAP = {
    "Atlan": "Data Intelligence",
    "Collibra": "Data Intelligence",
    "Alation": "Data Intelligence",
    "Monte Carlo": "Data Observability",
    "Bigeye": "Data Observability",
    "Acceldata": "Data Observability",
    "Pinecone": "VectorAI",
    "Qdrant": "VectorAI",
    "Milvus": "VectorAI",
    "Snowflake": "AI Analyst",
    "Databricks": "AI Analyst",
}

# ── Hardcoded design tokens (from data2.js) ──────────────────────────────────
THREAT_TOKENS = {
    "CRITICAL": {"label": "CRITICAL", "fg": "oklch(0.48 0.18 25)",  "bg": "oklch(0.96 0.04 25)",  "bd": "oklch(0.88 0.10 25)",  "dot": "oklch(0.58 0.20 25)",  "rail": "oklch(0.58 0.18 25)"},
    "HIGH":     {"label": "HIGH",     "fg": "oklch(0.50 0.13 65)",  "bg": "oklch(0.97 0.035 70)", "bd": "oklch(0.88 0.08 70)",  "dot": "oklch(0.66 0.15 65)",  "rail": "oklch(0.70 0.14 65)"},
    "MEDIUM":   {"label": "MEDIUM",   "fg": "oklch(0.44 0.13 245)", "bg": "oklch(0.97 0.03 245)", "bd": "oklch(0.88 0.06 245)", "dot": "oklch(0.58 0.15 245)", "rail": "oklch(0.62 0.13 245)"},
    "LOW":      {"label": "LOW",      "fg": "oklch(0.48 0.01 250)", "bg": "oklch(0.97 0.005 250)","bd": "oklch(0.90 0.01 250)", "dot": "oklch(0.68 0.01 250)", "rail": "oklch(0.80 0.01 250)"},
}

AREA_TOKENS = {
    "Data Intelligence":  {"fg": "oklch(0.42 0.14 270)", "bg": "oklch(0.96 0.03 270)", "bd": "oklch(0.88 0.06 270)", "dot": "oklch(0.55 0.16 270)"},
    "Data Observability": {"fg": "oklch(0.42 0.10 195)", "bg": "oklch(0.96 0.03 195)", "bd": "oklch(0.88 0.06 195)", "dot": "oklch(0.55 0.12 195)"},
    "VectorAI":           {"fg": "oklch(0.44 0.14 310)", "bg": "oklch(0.96 0.03 310)", "bd": "oklch(0.88 0.06 310)", "dot": "oklch(0.56 0.16 310)"},
    "AI Analyst":         {"fg": "oklch(0.44 0.13 245)", "bg": "oklch(0.96 0.03 245)", "bd": "oklch(0.88 0.06 245)", "dot": "oklch(0.56 0.14 245)"},
}

TEAMS = ["All", "Product", "PMM", "Marketing", "SDRs", "Executives"]

TARGET_FUNCTIONS = ["Sales", "Engineering", "Product Management", "Solution Engineering", "Partners/Alliances"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def fmt_date(date_str):
    """Format a date string as 'Apr 8, 2026'."""
    if not date_str:
        return ""
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return d.strftime("%b %-d, %Y")
    except (ValueError, AttributeError):
        return date_str


def extract_team_action(recommended_actions, team_keyword):
    """
    Find the first recommended_action entry containing team_keyword.
    Strips the [Tag / Tag] prefix and returns the rest.
    Falls back to first action or empty string.
    """
    if not recommended_actions:
        return ""
    kw_lower = team_keyword.lower()
    for action in recommended_actions:
        if kw_lower in action.lower():
            # Strip prefix like "[Immediate / GTM] " or "[Next Quarter / Product] "
            stripped = re.sub(r"^\[[^\]]+\]\s*", "", action)
            return stripped
    # Fallback: return first action stripped of prefix
    return re.sub(r"^\[[^\]]+\]\s*", "", recommended_actions[0])


def build_signals(implications):
    """
    Take first 4 implications and create signal chips. Use the full clause
    before the em-dash separator (the "what they're doing" half), but never
    cut mid-word — the dashboard handles overflow with wrap, not truncation.
    """
    chips = []
    for impl in (implications or [])[:4]:
        # Extract text before em dash (drop the "why it matters" half).
        # Comma is NOT a separator (cuts mid-clause).
        label = impl
        for sep in [" — ", " – "]:
            if sep in label:
                label = label.split(sep)[0]
                break
        label = label.strip()
        chips.append({
            "label": label,
            "weight": {"All": 1, "Product": 1}
        })
    return chips


def derive_routes(signal, verdict):
    """
    Determine which team routes are relevant for a competitor card.
    Prefer the verdict's team_routing (intelligence-driven) when present.
    Fall back to threat + posting count derivation for legacy data.
    """
    # Primary: verdict.team_routing — computed from hiring + news + launches + events
    verdict_routing = verdict.get("team_routing") or []
    allowed = {"Product", "PMM", "Marketing", "SDRs", "Executives"}
    verdict_routing = [r for r in verdict_routing if r in allowed]
    if verdict_routing:
        order = ["Product", "PMM", "Marketing", "SDRs", "Executives"]
        return [t for t in order if t in verdict_routing]

    # Fallback: legacy threat + hiring-volume logic (keeps older data usable)
    threat = (signal.get("threat_level") or "low").upper()
    posting_count = signal.get("posting_count", 0)
    routes = ["Product"]
    if threat in ("CRITICAL", "HIGH"):
        routes += ["PMM", "Executives"]
    if threat == "CRITICAL" or (threat in ("HIGH", "MEDIUM") and posting_count > 20):
        routes.append("Marketing")
    if threat in ("CRITICAL", "HIGH"):
        routes.append("SDRs")
    seen = set()
    unique = []
    for r in routes:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return unique


def relative_updated(last_updated_str):
    """Convert a date string to a relative 'X days ago' label."""
    if not last_updated_str:
        return "recently"
    try:
        d = datetime.strptime(last_updated_str[:10], "%Y-%m-%d").date()
        today = date.today()
        diff = (today - d).days
        if diff == 0:
            return "today"
        if diff == 1:
            return "1 day ago"
        if diff < 7:
            return f"{diff}d ago"
        if diff < 30:
            return f"{diff // 7}w ago"
        return f"{diff // 30}mo ago"
    except (ValueError, AttributeError):
        return "recently"


def derive_event_teams(relevance):
    r = (relevance or "").upper()
    if r == "HIGH":
        return ["PMM", "Marketing", "SDRs"]
    if r == "MEDIUM":
        return ["PMM", "Marketing"]
    return ["Product"]


def load_function_breakdown(csv_path, allowed_companies):
    """Read jobs_enriched_v2.csv and compute per-company Function counts."""
    per_company = defaultdict(lambda: defaultdict(int))
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('Company', '').strip()
                func = row.get('Function', '').strip()
                # Normalize legacy value from before Solution Engineering/Partners were added
                if func == 'Product':
                    func = 'Product Management'
                if company in allowed_companies and func:
                    per_company[company][func] += 1
    except FileNotFoundError:
        pass
    return per_company


def load_country_breakdown(csv_path, allowed_companies):
    """Read jobs_enriched_v2.csv and return per-company country distributions.

    Returns:
      {company: {"top": [(country, count), ...], "recent": [(country, count_30d), ...]}}
    """
    per_company = defaultdict(lambda: {"all": defaultdict(int), "recent": defaultdict(int)})
    overall = defaultdict(int)
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company = row.get('Company', '').strip()
                if company not in allowed_companies:
                    continue
                country = country_from_location(row.get('Location', ''))
                per_company[company]["all"][country] += 1
                overall[country] += 1
                try:
                    days = int(float(row.get("Days Since Posted") or 9999))
                except (ValueError, TypeError):
                    days = 9999
                if days <= 30:
                    per_company[company]["recent"][country] += 1
    except FileNotFoundError:
        pass

    out = {}
    for company, dists in per_company.items():
        # Drop "Unknown" from headline lists unless it's everything
        named_all = {k: v for k, v in dists["all"].items() if k != "Unknown"}
        named_recent = {k: v for k, v in dists["recent"].items() if k != "Unknown"}
        all_sorted = sorted((named_all or dists["all"]).items(), key=lambda x: -x[1])[:5]
        recent_sorted = sorted((named_recent or dists["recent"]).items(), key=lambda x: -x[1])[:5]
        out[company] = {"top": all_sorted, "recent": recent_sorted}

    overall_named = {k: v for k, v in overall.items() if k != "Unknown"}
    out["__overall__"] = {
        "top": sorted((overall_named or overall).items(), key=lambda x: -x[1])[:10],
    }
    return out


def build_function_trends(per_company, allowed_companies):
    """Cross-company count for each target function."""
    trends = {}
    for func in TARGET_FUNCTIONS:
        total = sum(per_company[c].get(func, 0) for c in allowed_companies)
        companies = sorted(
            [{"name": c, "count": per_company[c].get(func, 0)} for c in allowed_companies if per_company[c].get(func, 0) > 0],
            key=lambda x: -x["count"]
        )[:6]
        trends[func] = {"total": total, "companies": companies}
    return trends


def derive_team_actions(company, threat, verdict, recommended_actions, fallback):
    """Derive per-team action strings from verdict fields."""
    ci = verdict.get("competitive_impact", {})
    if not isinstance(ci, dict):
        ci = {}
    overlap = ci.get("overlap_with_actian", "")
    at_risk = ci.get("at_risk_segments", "")
    primary = str(verdict.get("primary_interpretation", ""))
    what = str(verdict.get("what_is_happening", ""))
    why = str(verdict.get("why_it_matters", ""))
    # Try to get a specific recommended action
    specific = ""
    for act in (recommended_actions or []):
        if len(act) > 40 and "monitor" not in act.lower():
            specific = re.sub(r"^\[[^\]]+\]\s*", "", act)
            break
    base = specific or fallback

    # No truncation — render full sentences. UI handles wrapping.
    def s(x):
        return (x or "").strip()

    return {
        "All": s(base),
        "Product": s(primary or what or base),
        "PMM": s(f"Update {company} battlecard — {overlap or why or base}") if overlap or why else s(base),
        "Marketing": s(why or what or base),
        "SDRs": s(f"At-risk accounts: {at_risk}. {base}" if at_risk else base),
        "Executives": s(why or base) if threat in ("CRITICAL", "HIGH") else f"Monitor {company} — {threat.title()} threat.",
    }


def compute_score(threat, posting_count):
    """Composite score: threat level base + hiring volume."""
    base = {"CRITICAL": 40, "HIGH": 20, "MEDIUM": 10, "LOW": 5}.get(threat, 5)
    return base + min(posting_count, 60)


# ── Data loading ─────────────────────────────────────────────────────────────

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Transform signals → COMPETITORS ─────────────────────────────────────────

def build_sources(company, comp_signals, news):
    """Collect source URLs backing a company's signals and news, newest first."""
    TYPE_LABEL = {
        "product_launch": "Launch",
        "event": "Event",
        "partnership": "Partnership",
        "funding": "Funding",
        "acquisition": "Acquisition",
        "leadership": "Leadership",
        "open_source_release": "OSS",
        "feature": "Feature",
        "pricing": "Pricing",
        "layoff": "Layoff",
        "blog_post": "Blog",
    }
    sources = []
    seen_urls = set()
    for s in (comp_signals or []):
        if s.get("company") != company:
            continue
        url = s.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        sources.append({
            "type": TYPE_LABEL.get(s.get("type", ""), s.get("type", "").replace("_", " ").title()),
            "title": s.get("title", "")[:80],
            "url": url,
            "date": s.get("published_date") or s.get("event_date") or "",
            "sourceType": s.get("source_type", "web"),
        })
    for n in (news or []):
        if n.get("company") != company:
            continue
        url = n.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        sources.append({
            "type": TYPE_LABEL.get(n.get("news_type", ""), n.get("news_type", "").replace("_", " ").title()),
            "title": n.get("title", "")[:80],
            "url": url,
            "date": n.get("published_date", ""),
            "sourceType": n.get("source", "company_newsroom"),
        })
    sources.sort(key=lambda x: x.get("date", ""), reverse=True)
    return sources[:8]


def build_recent_activity(company, comp_signals, news):
    """Build real recent activity for a company from signals + news, newest first."""
    TYPE_LABEL = {
        "product_launch": "Product launch",
        "event": "Event",
        "partnership": "Partnership",
        "funding": "Funding",
        "acquisition": "Acquisition",
        "leadership": "Leadership change",
        "open_source_release": "OSS release",
        "feature": "Feature shipped",
        "pricing": "Pricing change",
        "layoff": "Layoff",
        "blog_post": "Blog post",
    }
    items = []
    for s in comp_signals:
        if s.get("company") != company: continue
        # For events use event_date (when it happens), for launches use published_date (when announced)
        if s.get("type") == "event":
            raw_date = s.get("event_date") or s.get("published_date") or ""
        else:
            raw_date = s.get("published_date") or s.get("event_date") or ""
        if not raw_date: continue
        try:
            from datetime import date as _date
            d = _date.fromisoformat(raw_date)
            label = d.strftime("%b %-d")
        except Exception:
            label = raw_date[:7]
        ltype = TYPE_LABEL.get(s.get("type",""), s.get("type","").replace("_"," ").title())
        title = s.get("title","")[:60]
        items.append({"date": label, "text": f"{ltype}: {title}", "_sort": raw_date})
    for n in news:
        if n.get("company") != company: continue
        raw_date = n.get("published_date","")
        if not raw_date: continue
        try:
            from datetime import date as _date
            d = _date.fromisoformat(raw_date)
            label = d.strftime("%b %-d")
        except Exception:
            label = raw_date[:7]
        ltype = TYPE_LABEL.get(n.get("news_type",""), n.get("news_type","").replace("_"," ").title())
        title = n.get("title","")[:60]
        items.append({"date": label, "text": f"{ltype}: {title}", "_sort": raw_date})
    items.sort(key=lambda x: x["_sort"], reverse=True)
    for item in items:
        del item["_sort"]
    return items[:5]


def load_battlecards(csv_path):
    """Read competitors.csv → {company: battlecard_url}. Empty values dropped."""
    out = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                url = (row.get("Battlecard_URL") or "").strip()
                if url:
                    out[row["Company"]] = url
    except FileNotFoundError:
        pass
    return out


def build_competitors(signals, verdicts, per_company=None, comp_signals=None, news=None, battlecards=None, country_breakdown=None, roadmaps_by_co=None):
    battlecards = battlecards or {}
    country_breakdown = country_breakdown or {}
    roadmaps_by_co = roadmaps_by_co or {}
    # Index by lower-cased name for fuzzy matching
    sig_index = {s["company"].lower(): s for s in signals}
    vrd_index = {}
    for v in verdicts:
        name = v.get("company", "").lower()
        if name not in vrd_index:
            vrd_index[name] = v

    competitors = []
    for company, area in V2_PRODUCT_AREA_MAP.items():
        signal = sig_index.get(company.lower())
        if not signal:
            # Synthesize a minimal signal from the verdict if available
            v_fallback = vrd_index.get(company.lower(), {})
            if v_fallback:
                print(f"  WARNING: No signal for {company} — synthesising from verdict", file=sys.stderr)
                impact = (v_fallback.get("impact_level") or "medium").lower()
                threat_map = {"critical": "critical", "high": "high", "medium": "medium", "low": "low"}
                signal = {
                    "company": company,
                    "threat_level": threat_map.get(impact, "medium"),
                    "hiring_intensity": impact,
                    "posting_count": 0,
                    "signal_summary": v_fallback.get("what_is_happening", ""),
                    "implications": [],
                    "recommended_actions": [v_fallback.get("recommended_action", "")],
                    "dominant_function": "Engineering",
                    "dominant_product_focus": "Platform",
                    "roadmap": {"direction": "", "confidence": "Low", "timeline": "Unknown", "watch_for": ""},
                    "last_updated": v_fallback.get("last_updated", ""),
                }
            else:
                print(f"  WARNING: No signal or verdict found for {company} — skipping", file=sys.stderr)
                continue

        verdict = vrd_index.get(company.lower(), {})

        threat = (signal.get("threat_level") or "low").upper()
        intensity = (signal.get("hiring_intensity") or "low").upper()
        posting_count = signal.get("posting_count", 0)
        last_updated = signal.get("last_updated", "")

        # Build routes
        routes = derive_routes(signal, verdict)

        # Build signals from implications
        sig_chips = build_signals(signal.get("implications", []))

        # Recommended actions
        recommended_actions = signal.get("recommended_actions", [])
        verdict_action = verdict.get("recommended_action", "Review competitive positioning.")

        # Per-team verdict text — each team sees distinct framing
        ci = verdict.get("competitive_impact", {}) if isinstance(verdict.get("competitive_impact"), dict) else {}
        overlap = ci.get("overlap_with_actian", "")
        at_risk = ci.get("at_risk_segments", "")
        what = verdict.get("what_is_happening", signal.get("signal_summary", ""))
        why = verdict.get("why_it_matters", what)
        primary = verdict.get("primary_interpretation", "")
        confidence = verdict.get("confidence", "")

        def _pmm_verdict():
            if overlap:
                return f"Competitive overlap: {overlap}. {why}".strip()
            return why or what

        def _sdrs_verdict():
            if at_risk:
                return f"At-risk segments: {at_risk}. {what}".strip()
            return what

        def _exec_verdict():
            base = why or what
            if confidence:
                return f"{base} (Confidence: {confidence.title()})"
            return base

        # Multi-area + theme exposure (from verdict)
        product_areas = verdict.get("product_areas") or [area]
        verdict_themes = verdict.get("themes") or []
        battlecard_url = battlecards.get(company, "")
        # Geographic footprint (top countries + active expansion in last 30d)
        cb = country_breakdown.get(company, {})
        top_countries = [{"country": c, "count": n} for c, n in cb.get("top", [])]
        recent_countries = [{"country": c, "count": n} for c, n in cb.get("recent", [])]

        # Strategic roadmap (published or inferred)
        roadmap = roadmaps_by_co.get(company)

        comp = {
            "id": re.sub(r"[^a-z0-9]", "", company.lower()),
            "name": company,
            "area": area,
            "areas": product_areas,
            "themes": verdict_themes,
            "battlecardUrl": battlecard_url,
            "topCountries": top_countries,
            "recentCountries": recent_countries,
            "roadmap": roadmap,
            "threat": threat,
            "intensity": intensity,
            "postingCount": posting_count,
            "dominantFunction": signal.get("dominant_function", "Engineering"),
            "dominantFocus": signal.get("dominant_product_focus", "Platform"),
            "signalSummary": signal.get("signal_summary", ""),
            # Note: `roadmap` is set above from data/roadmaps.json (rich schema with pillars).
            # Legacy flat-schema roadmap from hiring signal is not used — new schema supersedes.
            "verdict": {
                "All":        what,
                "Product":    primary or what,
                "PMM":        _pmm_verdict(),
                "Marketing":  why or what,
                "SDRs":       _sdrs_verdict(),
                "Executives": _exec_verdict(),
            },
            "action": derive_team_actions(company, threat, verdict, recommended_actions, verdict_action),
            "signals": sig_chips,
            "implications": signal.get("implications", [])[:5],
            "watchFor": signal.get("watch_for", [signal.get("roadmap", {}).get("watch_for", "")]),
            "routes": routes,
            "updated": relative_updated(last_updated),
            "score": compute_score(threat, posting_count),
            "functionBreakdown": {f: (per_company or {}).get(company, {}).get(f, 0) for f in TARGET_FUNCTIONS},
            "recentActivity": build_recent_activity(company, comp_signals or [], news or []),
            "sources": build_sources(company, comp_signals or [], news or []),
        }
        competitors.append(comp)

    # Sort by threat → score
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    competitors.sort(key=lambda c: (order.get(c["threat"], 9), -c["score"]))
    return competitors


# ── Transform competitive_signals → LAUNCHES + EVENTS ───────────────────────

def _is_latin_title(title):
    """Return True only when title contains no CJK / Hangul / Arabic script characters.
    Allows em-dash, curly quotes, and other Western punctuation above U+02FF."""
    for c in title:
        cp = ord(c)
        # Block East Asian & Arabic scripts specifically; allow everything else
        if (0x0600 <= cp <= 0x06FF   # Arabic
                or 0x0900 <= cp <= 0x097F  # Devanagari
                or 0x3040 <= cp <= 0x30FF  # Hiragana/Katakana
                or 0x4E00 <= cp <= 0x9FFF  # CJK Unified
                or 0xAC00 <= cp <= 0xD7AF  # Hangul syllables
                or 0xF900 <= cp <= 0xFAFF  # CJK compatibility
        ):
            return False
    return True


# Sanity rule — runtime defensive pass. Mirrors signal_scraper._title_year_in_past.
# Drops any signal whose title references only past years before the dashboard
# renders it, regardless of what the JSON says. Belt-and-suspenders.
_TITLE_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _title_year_in_past(title: str) -> bool:
    if not title:
        return False
    years = [int(y) for y in _TITLE_YEAR_RE.findall(title)]
    if not years:
        return False
    return max(years) < date.today().year


def build_launches_events(comp_signals):
    # Defensive sanity filter on input — even if competitive_signals.json has
    # stale items, never let them reach the dashboard.
    before = len(comp_signals or [])
    comp_signals = [s for s in (comp_signals or []) if not _title_year_in_past(s.get("title", ""))]
    if before != len(comp_signals):
        print(f"  [SANITY] dropped {before - len(comp_signals)} stale signal(s) by title-year scan")
    launches = []
    events = []
    launch_id = 1
    event_id = 1

    for item in comp_signals:
        company = item.get("company", "")
        area = V2_PRODUCT_AREA_MAP.get(company, item.get("product_area", "Data Intelligence"))
        relevance = (item.get("actian_relevance") or "low").upper()
        event_date = item.get("event_date")
        published_date = item.get("published_date", "")

        item_type = item.get("type", "blog_post")
        is_launch = item_type == "product_launch"

        if event_date and not is_launch:
            # Pure event (conference, webinar, meetup) — NOT a product launch
            raw_title = item.get("title", "")
            clean_title = re.sub(r'\s+at\s+\d{1,2}:\d{2}\s*(AM|PM).*$', '', raw_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'\s+https?://\S+', '', clean_title).strip()
            clean_title = re.sub(r'\s+Learn\s+More\s*$', '', clean_title, flags=re.IGNORECASE).strip()
            clean_title = re.sub(r'^(Virtual|In-Person|Webinar|Hackathon)\s+', '', clean_title).strip()
            # Skip events with non-Latin characters in the title (e.g. Korean, Japanese scraped pages)
            if not _is_latin_title(clean_title or raw_title):
                continue
            # Prefer signal's team_routing (intelligence-driven); fall back to legacy logic
            event_teams = item.get("team_routing") or derive_event_teams(relevance)
            events.append({
                "id": f"e{event_id}",
                "company": company,
                "area": area,
                "name": clean_title or raw_title,
                "date": fmt_date(event_date),
                "relevance": relevance,
                "why": item.get("summary", ""),
                "url": item.get("url", ""),
                "teams": event_teams,
                "themes": item.get("themes", []),
                "action": "Check dashboard for details.",
            })
            event_id += 1
        else:
            # Product launch — use event_date if present (launch event), else published_date
            date_str = event_date if event_date else published_date
            launches.append({
                "id": f"l{launch_id}",
                "company": company,
                "area": area,
                "type": item_type,
                "title": item.get("title", ""),
                "date": fmt_date(date_str),
                "summary": item.get("summary", ""),
                "relevance": relevance,
                "tags": item.get("tags", []),
                "url": item.get("url", ""),
                "teams": item.get("team_routing") or ["Product", "PMM", "Marketing"],
                "themes": item.get("themes", []),
            })
            launch_id += 1

    # Sort launches by published_date descending — no limit, show all
    launches_sorted = sorted(
        launches,
        key=lambda x: x.get("date", "") or "",
        reverse=True
    )

    # Sort events by date ascending (upcoming first) — no limit, show all
    events_sorted = sorted(
        events,
        key=lambda x: x.get("date", "") or "",
    )

    return launches_sorted, events_sorted


# ── JS generation ────────────────────────────────────────────────────────────

def to_js_value(val, indent=0):
    """Serialize a Python value to a JS literal string."""
    pad = "  " * indent
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, str):
        # Escape for JS string
        escaped = val.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "")
        return f'"{escaped}"'
    if isinstance(val, list):
        if not val:
            return "[]"
        items = [f"\n{pad}  {to_js_value(v, indent + 1)}" for v in val]
        return "[" + ",".join(items) + f"\n{pad}]"
    if isinstance(val, dict):
        if not val:
            return "{}"
        lines = []
        for k, v in val.items():
            key_js = k if re.match(r"^[A-Za-z_$][A-Za-z0-9_$]*$", k) else f'"{k}"'
            lines.append(f"\n{pad}  {key_js}: {to_js_value(v, indent + 1)}")
        return "{" + ",".join(lines) + f"\n{pad}}}"
    return f'"{val}"'


def generate_data_js(competitors, launches, events, function_trends=None, news=None, overall_countries=None):
    lines = []

    # claude fallback
    lines.append(
        "if (!window.claude) {\n"
        '  window.claude = { complete: async () => "CI assistant is unavailable in standalone mode." };\n'
        "}"
    )
    lines.append("")

    # THREAT tokens
    lines.append("window.THREAT = " + to_js_value(THREAT_TOKENS) + ";")
    lines.append("")

    # AREA tokens
    lines.append("window.AREA = " + to_js_value(AREA_TOKENS) + ";")
    lines.append("")

    # TEAMS
    lines.append("window.TEAMS = " + to_js_value(TEAMS) + ";")
    lines.append("")

    # OVERALL COUNTRY FOOTPRINT (top 10)
    lines.append("window.COUNTRIES = " + to_js_value(overall_countries or []) + ";")
    lines.append("")

    # COMPETITORS
    lines.append("window.COMPETITORS = " + to_js_value(competitors) + ";")
    lines.append("")

    # LAUNCHES
    lines.append("window.LAUNCHES = " + to_js_value(launches) + ";")
    lines.append("")

    # EVENTS
    lines.append("window.EVENTS = " + to_js_value(events) + ";")

    # NEWS
    lines.append("")
    lines.append("window.NEWS = " + to_js_value(news or []) + ";")

    # FUNCTION_TRENDS
    lines.append("")
    lines.append("window.FUNCTION_TRENDS = " + to_js_value(function_trends or {}) + ";")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("build_dashboard_v3.py starting...")

    # Load data
    print("  Loading data/signals.json...")
    signals = load_json(DATA_DIR / "signals.json")
    print(f"    {len(signals)} companies in signals.json")

    print("  Loading data/intelligence_verdicts.json...")
    verdicts = load_json(DATA_DIR / "intelligence_verdicts.json")
    print(f"    {len(verdicts)} verdicts loaded")

    print("  Loading data/competitive_signals.json...")
    comp_signals = load_json(DATA_DIR / "competitive_signals.json")
    print(f"    {len(comp_signals)} competitive signals loaded")

    print("  Loading data/news.json...")
    news = load_json(DATA_DIR / "news.json")
    print(f"    {len(news)} news items loaded")

    print("  Loading jobs_enriched_v2.csv for function breakdown...")
    allowed = list(V2_PRODUCT_AREA_MAP.keys())
    per_company = load_function_breakdown(DATA_DIR / "jobs_enriched_v2.csv", allowed)
    function_trends = build_function_trends(per_company, allowed)

    print("  Loading data/competitors.csv for battlecard URLs...")
    battlecards = load_battlecards(DATA_DIR / "competitors.csv")
    print(f"    {len(battlecards)} battlecard URLs loaded")

    print("  Computing per-company country breakdown...")
    country_breakdown = load_country_breakdown(DATA_DIR / "jobs_enriched_v2.csv", allowed)
    overall_countries = [{"country": c, "count": n} for c, n in country_breakdown.get("__overall__", {}).get("top", [])]
    print(f"    {len(overall_countries)} countries in overall footprint")

    # Roadmaps (published + inferred)
    roadmaps_path = DATA_DIR / "roadmaps.json"
    roadmaps = load_json(roadmaps_path) if roadmaps_path.exists() else []
    roadmaps_by_co = {r.get("company"): r for r in roadmaps}
    print(f"  Loaded {len(roadmaps_by_co)} roadmaps "
          f"({sum(1 for r in roadmaps if r.get('source')=='published')} published, "
          f"{sum(1 for r in roadmaps if r.get('source')=='inferred')} inferred)")

    # Build COMPETITORS
    print("  Building COMPETITORS array...")
    competitors = build_competitors(
        signals, verdicts, per_company,
        comp_signals=comp_signals, news=news, battlecards=battlecards,
        country_breakdown=country_breakdown,
        roadmaps_by_co=roadmaps_by_co,
    )
    print(f"    {len(competitors)} competitors built")
    for c in competitors:
        print(f"    - {c['name']:20s} {c['threat']:8s}  {c['postingCount']:3d} postings")

    # Build LAUNCHES + EVENTS
    print("  Building LAUNCHES + EVENTS...")
    launches, events = build_launches_events(comp_signals)
    print(f"    {len(launches)} launches, {len(events)} events")

    # Generate data JS
    print("  Generating JS data block...")
    data_js = generate_data_js(competitors, launches, events, function_trends, news, overall_countries)

    # Load template
    print(f"  Loading template: {TEMPLATE}")
    template_html = TEMPLATE.read_text(encoding="utf-8")

    if "%%DATA_JSON%%" not in template_html:
        print("ERROR: %%DATA_JSON%% placeholder not found in template!", file=sys.stderr)
        sys.exit(1)

    # Inject data
    output_html = template_html.replace("%%DATA_JSON%%", data_js)

    # Prepend pipeline provenance comment
    run_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    provenance_comment = (
        f"<!-- CI Pipeline · run: {run_at} · {len(competitors)} competitors · "
        f"{len(launches)} launches · {len(events)} events · public data -->\n"
    )
    output_html = provenance_comment + output_html

    # Write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(output_html, encoding="utf-8")

    # Write pipeline manifest
    manifest = {
        "run_at": run_at,
        "sources": {
            "signals.json":               {"records": len(signals),      "data_classification": "PUBLIC"},
            "competitive_signals.json":   {"records": len(comp_signals), "data_classification": "PUBLIC"},
            "news.json":                  {"records": len(news),          "data_classification": "PUBLIC"},
            "intelligence_verdicts.json": {"records": len(verdicts),     "data_classification": "PUBLIC"},
        },
        "output": {
            "competitors": len(competitors),
            "launches":    len(launches),
            "events":      len(events),
        },
    }
    manifest_path = DATA_DIR / "pipeline_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"  Manifest: {manifest_path}")

    size_kb = OUTPUT.stat().st_size / 1024
    print(f"\n  Done! Output: {OUTPUT}")
    print(f"  File size: {size_kb:.1f} KB")


if __name__ == "__main__":
    main()
