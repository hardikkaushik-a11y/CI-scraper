"""
themes.py — Semantic theme tagger
─────────────────────────────────
Classifies any free-text content (news titles, launch summaries, hiring blurbs)
into one or more of 15 stakeholder-defined intelligence themes.

Used by news_scraper, signal_scraper, and verdict_engine to add a `themes: [...]`
array to every item, enabling cross-vendor theme lenses in the dashboard.

Rule-based today (regex keyword match). When DEEPSEEK_API_KEY is present, the
LLM call already produces summaries that match these themes naturally, so
keyword tagging on those summaries stays accurate.
"""

import re
from typing import Iterable

# ── Stakeholder-defined themes (15) ───────────────────────────────────────
# Each maps to a regex of indicative keywords. Multi-word phrases use \s+ to
# tolerate whitespace variation. Word boundaries on short tokens to avoid
# false matches (e.g. "ml" inside "html").

THEME_KEYWORDS: dict[str, str] = {
    "Agentic BI":
        r"agentic\s+bi|ai\s+analyst|autonomous\s+analytics|agent.{0,20}business\s+intelligence",
    "Conversational Analytics":
        r"conversational\s+analytics|natural\s+language\s+(?:to\s+)?sql|nl2sql|text\s+to\s+sql|"
        r"chat\s+with\s+(?:your\s+)?data|ask\s+(?:your\s+)?data",
    "Embedded Analytics":
        r"embedded\s+analytics|white[-\s]?label\s+analytics|analytics\s+sdk|"
        r"embed.{0,20}dashboard",
    "Semantic / Metrics Layer":
        r"semantic\s+layer|metrics\s+layer|metric\s+store|headless\s+bi|"
        r"semantic\s+model",
    "AI-Ready Data":
        r"ai[-\s]?ready\s+data|ai\s+data\s+layer|data\s+for\s+ai|prepare.{0,20}for\s+ai|"
        r"feature\s+store",
    "Data Products / Contracts":
        r"data\s+product|data\s+contract|data\s+marketplace|productize.{0,20}data",
    "Data / AI Governance":
        r"data\s+governance|ai\s+governance|model\s+governance|policy\s+as\s+code|"
        r"compliance|hipaa|gdpr|sox",
    "Observability / Quality / Lineage":
        r"data\s+observability|data\s+quality|data\s+lineage|column\s+lineage|"
        r"\bsla\b|freshness|anomaly\s+detection",
    "Catalog / Metadata / Knowledge Graph":
        r"data\s+catalog|metadata\s+management|knowledge\s+graph|active\s+metadata|"
        r"context\s+layer",
    "Unstructured Data":
        r"unstructured\s+data|document\s+intelligence|pdf\s+ingestion|"
        r"document\s+ai|multimodal\s+data",
    "Vector / RAG / Retrieval":
        r"vector\s+(?:database|search|store|db)|embedding|retrieval[-\s]?augmented|"
        r"\brag\b|semantic\s+search|hybrid\s+search",
    "Lakehouse / Warehouse / HTAP / Postgres":
        r"lakehouse|warehouse\s+modernization|\bhtap\b|postgres|iceberg|delta\s+lake|"
        r"hudi|warehouse\s+native",
    "ETL / ELT / Integration":
        r"\betl\b|\belt\b|reverse\s+etl|data\s+integration|data\s+pipeline|"
        r"data\s+ingestion|cdc\b|change\s+data\s+capture",
    "MCP for Data":
        r"\bmcp\b|model\s+context\s+protocol|mcp\s+server",
    "Agent Observability":
        r"agent\s+observability|llm\s+observability|llm\s+monitoring|"
        r"prompt\s+(?:logging|tracking)|trace.{0,20}llm",
}

# Compile once
_COMPILED: dict[str, re.Pattern] = {
    theme: re.compile(pattern, re.IGNORECASE)
    for theme, pattern in THEME_KEYWORDS.items()
}

ALL_THEMES = tuple(THEME_KEYWORDS.keys())


def classify_themes(*text_parts: str) -> list[str]:
    """
    Return the list of themes whose keyword regex matches any of the supplied
    text parts. Empty / None parts are ignored. Order matches THEME_KEYWORDS.
    """
    blob = " ".join(p for p in text_parts if p).lower()
    if not blob.strip():
        return []
    return [theme for theme, pat in _COMPILED.items() if pat.search(blob)]


def aggregate_themes(items: Iterable[dict]) -> list[str]:
    """
    Given a sequence of items each potentially carrying a `themes: [...]` array,
    return the de-duplicated union in canonical theme order.
    """
    seen: set[str] = set()
    for item in items:
        for t in item.get("themes", []) or []:
            seen.add(t)
    return [t for t in ALL_THEMES if t in seen]


# ── Multi-area mapping: which Actian product areas does a theme touch? ────
# Used by verdict_engine to expand a verdict's `product_areas` array beyond
# the company's primary area when their signals span multiple areas.
#
# Mapping is INTENTIONALLY SPARSE. Only themes that unambiguously denote
# competition in a specific Actian product area get mapped. Generic themes
# (ETL, MCP, Lakehouse) are NOT mapped — almost every vendor touches them,
# so mapping them would pollute every area filter with every vendor.
#
# A vendor only appears in a non-primary area if a hard-signal theme tied to
# that area is present (e.g. Conversational Analytics → AI Analyst).

THEME_TO_PRODUCT_AREAS: dict[str, tuple[str, ...]] = {
    # AI Analyst — direct, unambiguous AI-on-analytics competition only
    "Agentic BI":                            ("AI Analyst",),
    "Conversational Analytics":              ("AI Analyst",),

    # Data Intelligence — catalog/governance pure plays
    "Catalog / Metadata / Knowledge Graph":  ("Data Intelligence",),
    "Data / AI Governance":                  ("Data Intelligence",),

    # Data Observability — quality/lineage/agent observability
    "Observability / Quality / Lineage":     ("Data Observability",),
    "Agent Observability":                   ("Data Observability",),

    # VectorAI — vector / unstructured retrieval
    "Vector / RAG / Retrieval":              ("VectorAI",),
    "Unstructured Data":                     ("VectorAI",),

    # The following are intentionally NOT mapped (too generic to assign cleanly):
    # - Embedded Analytics, Semantic / Metrics Layer, AI-Ready Data,
    # - Data Products / Contracts, Lakehouse / Warehouse / HTAP / Postgres,
    # - ETL / ELT / Integration, MCP for Data
    # These themes still surface via the theme filter, just don't pollute areas.
}


def derive_product_areas(primary: str, themes: list[str]) -> list[str]:
    """
    Compute the multi-area list for a verdict given its primary area and
    aggregated theme set. Primary always first. De-dupes while preserving order.
    Only direct, unambiguous theme→area mappings expand beyond primary.
    """
    out = [primary] if primary else []
    for theme in themes:
        for area in THEME_TO_PRODUCT_AREAS.get(theme, ()):
            if area and area not in out:
                out.append(area)
    return out
