"""
team_routing.py — Single source of truth for team routing logic
────────────────────────────────────────────────────────────────
All three intelligence sources route to teams through this module:

  news_scraper.py     → per news item    → route_by_news_type()
  signal_scraper.py   → per comp signal  → route_by_signal_type()
  verdict_engine.py   → per company      → route_verdict() (union of all signals)

Teams: Product, PMM, Marketing, SDRs, Executives
Order is stable — always Product → PMM → Marketing → SDRs → Executives.
"""

from typing import Iterable

TEAM_ORDER = ["Product", "PMM", "Marketing", "SDRs", "Executives"]

# ── Function classification for hiring routing ───────────────────────────────
GTM_FUNCTIONS = {
    "Sales", "Marketing", "Go-to-Market", "Customer Success",
    "Revenue", "Business Development", "Solution Engineering", "Partners/Alliances",
}
ENG_FUNCTIONS = {
    "Engineering", "Product", "Product Management", "Research",
    "Infrastructure", "Data", "Data Science",
}


def _ordered(teams: Iterable[str]) -> list[str]:
    """Return teams in canonical order, deduplicated."""
    s = set(teams)
    return [t for t in TEAM_ORDER if t in s]


# ══════════════════════════════════════════════════════════════════════════
# PRIMITIVE ROUTERS — used per-item by scrapers
# ══════════════════════════════════════════════════════════════════════════

def route_by_news_type(news_type: str) -> list[str]:
    """Route a single newsroom item to teams based on news_type."""
    routing = {
        "funding":        ["Executives", "PMM"],
        "acquisition":    ["Executives", "PMM", "Product"],
        "leadership":     ["Executives", "PMM"],
        "partnership":    ["PMM", "SDRs", "Marketing"],
        "pricing":        ["SDRs", "Marketing", "PMM"],
        "product_launch": ["Product", "PMM", "Marketing"],
        "feature":        ["Product", "PMM"],
        "layoff":         ["Executives", "SDRs"],
        # New types
        "integration":    ["Product", "PMM"],          # third-party LLM/tool now on a vendor's platform
        "expansion":      ["SDRs", "Marketing", "PMM"], # geographic / infra footprint expansion
        "coverage":       ["Marketing", "PMM"],         # third-party media — analyst sentiment tracking
    }
    return _ordered(routing.get(news_type, ["PMM"]))


def route_by_signal_type(signal_type: str, actian_relevance: str = "medium") -> list[str]:
    """Route a single competitive signal (launch/event/partnership/etc.) to teams.

    Relevance escalates: high relevance always pulls Marketing + PMM.
    """
    base = {
        "product_launch":      ["Product", "PMM", "Marketing"],
        "open_source_release": ["Product", "PMM"],
        "partnership":         ["PMM", "SDRs"],
        "funding":             ["Executives", "PMM"],
        "event":               ["Marketing", "SDRs", "PMM"],
        "blog_post":           ["PMM"],
    }
    teams = set(base.get(signal_type, ["PMM"]))

    # Relevance escalation
    if actian_relevance == "high":
        teams.update(["Marketing", "PMM"])
        if signal_type in ("funding", "acquisition"):
            teams.add("Executives")

    return _ordered(teams)


# ══════════════════════════════════════════════════════════════════════════
# VERDICT ROUTER — union across all signal sources for a company
# ══════════════════════════════════════════════════════════════════════════

def route_verdict(
    *,
    news_types: Iterable[str] = (),
    comp_signal_types: Iterable[str] = (),
    hiring_function: str = "",
    posting_count: int = 0,
    impact_level: str = "feature",
    actian_relevance: str = "medium",
) -> list[str]:
    """
    Determine team_routing for a company-level verdict by combining:
      • All news items (union of route_by_news_type per type)
      • All competitive signals (union of route_by_signal_type per type)
      • Hiring surge direction (eng → Product, gtm → SDRs, when volume ≥ 20)
      • Impact level (platform/market → escalate to Executives)
      • Overall Actian relevance (high → pull PMM)
    """
    teams: set[str] = set()

    # Per-news-type routing
    for nt in news_types:
        teams.update(route_by_news_type(nt))

    # Per-signal-type routing
    for st in comp_signal_types:
        teams.update(route_by_signal_type(st, actian_relevance))

    # Hiring-driven routing — only kicks in at meaningful volume
    if posting_count >= 20:
        if hiring_function in ENG_FUNCTIONS:
            teams.add("Product")
        elif hiring_function in GTM_FUNCTIONS:
            teams.add("SDRs")
    elif posting_count >= 10:
        # Moderate hiring — always relevant to Product as baseline
        teams.add("Product")

    # Impact-level escalation
    if impact_level in ("platform", "market"):
        teams.add("Executives")

    # High Actian relevance always involves PMM
    if actian_relevance == "high":
        teams.add("PMM")

    # Safety net — every verdict routes somewhere
    if not teams:
        teams.add("Product")

    return _ordered(teams)


# ══════════════════════════════════════════════════════════════════════════
# TEAM RELEVANCE SCORING — 0–5 scale per team for dashboard heatmap
# ══════════════════════════════════════════════════════════════════════════

def compute_team_relevance(
    *,
    team_routing: list[str],
    impact_level: str = "feature",
    threat_level: str = "medium",
    posting_count: int = 0,
) -> dict:
    """
    Produce a {team: score 0-5} map. Teams in team_routing get a higher baseline;
    impact/threat modulate exec relevance; hiring volume bumps Product/SDRs.
    """
    threat_base = {"critical": 5, "high": 4, "medium": 3, "low": 2}.get(
        (threat_level or "").lower(), 2
    )

    def score(team: str) -> int:
        base = threat_base if team in team_routing else max(1, threat_base - 2)
        # Bumps
        if team == "Executives" and impact_level in ("platform", "market"):
            base = min(5, base + 1)
        if team == "Product" and posting_count >= 30:
            base = min(5, base + 1)
        if team == "SDRs" and posting_count >= 30:
            base = min(5, base + 1)
        return max(0, min(5, base))

    return {
        "product":    score("Product"),
        "pmm":        score("PMM"),
        "marketing":  score("Marketing"),
        "sdrs":       score("SDRs"),
        "executives": score("Executives"),
    }
