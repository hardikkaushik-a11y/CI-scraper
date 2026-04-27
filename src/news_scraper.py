"""
news_scraper.py — Press Release & Newsroom Intelligence Scraper
────────────────────────────────────────────────────────────────
• Scrapes company newsroom/press release pages (11 companies)
• Strict classification: funding, leadership (exec appt/departure only),
  product_launch, feature, partnership, acquisition, pricing, layoff
• Classifies on TITLE ONLY — description is untrusted (may contain neighbor text)
• Deduplicates by URL at write time (news.json) AND via seen_news.json
• Rolling 90-day window — empty dates pass through (assume recent)
• Outputs: data/news.json
"""

import json
import os
import re
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from team_routing import route_by_news_type
from themes import classify_themes

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

MAX_NEWS_AGE_DAYS = 90
MAX_ITEMS_PER_COMPANY = 15

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = "deepseek-chat"  # V4-Flash

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "news.json"
SEEN_FILE = DATA_DIR / "seen_news.json"

V2_PRODUCT_AREA_MAP = {
    "Bigeye": "Data Observability",
    "Atlan": "Data Intelligence",
    "Qdrant": "VectorAI",
    "Collibra": "Data Intelligence",
    "Alation": "Data Intelligence",
    "Pinecone": "VectorAI",
    "Monte Carlo": "Data Observability",
    "Acceldata": "Data Observability",
    "Milvus": "VectorAI",
    "Snowflake": "AI Analyst",
    "Databricks": "AI Analyst",
}

NEWSROOM_URLS = {
    # Bigeye: newsroom for press releases + blog for product/integration posts
    "Bigeye": [
        "https://www.bigeye.com/newsroom",
        "https://www.bigeye.com/blog",
    ],
    # Atlan: newsroom + blog — blog is where MCP/integration/AI agent posts land
    "Atlan": [
        "https://atlan.com/newsroom/",
        "https://atlan.com/blog/",
    ],
    # Qdrant: blog is primary source (no separate newsroom)
    "Qdrant": "https://qdrant.tech/blog/",
    # Collibra: press releases + product blog for feature/integration announcements
    "Collibra": [
        "https://www.collibra.com/company/newsroom/press-releases",
        "https://www.collibra.com/blog/",
    ],
    # Alation: news/press + blog for product announcements and integrations
    "Alation": [
        "https://www.alation.com/news-and-press/",
        "https://www.alation.com/blog/",
    ],
    # Pinecone: newsroom + blog — blog has plugin/integration/SDK releases
    "Pinecone": [
        "https://www.pinecone.io/newsroom/",
        "https://www.pinecone.io/blog/",
    ],
    # Monte Carlo: announcements category + general blog for product posts
    "Monte Carlo": [
        "https://www.montecarlodata.com/category/announcements/",
        "https://www.montecarlodata.com/blog/",
    ],
    # Acceldata: newsroom only — their blog has persistent CMS date injection
    # (always returns today's date; items would fail within_window check)
    "Acceldata": "https://www.acceldata.io/newsroom",
    # Milvus/Zilliz: Zilliz news + Milvus blog for vector DB integration releases
    "Milvus": [
        "https://zilliz.com/news",
        "https://milvus.io/blog/",
    ],
    # Snowflake: press releases + coverage + blog for AI/agent integration posts
    "Snowflake": [
        "https://www.snowflake.com/en/news/press-releases/",
        "https://www.snowflake.com/en/news/news-coverage/",
        "https://www.snowflake.com/blog/",
    ],
    # Databricks: press releases + coverage + blog for open-source/agent releases
    "Databricks": [
        "https://www.databricks.com/company/newsroom/press-releases",
        "https://www.databricks.com/company/newsroom/media-coverage",
        "https://www.databricks.com/blog/",
    ],
}

today_str = date.today().isoformat()

# ══════════════════════════════════════════════════════════════════════════
# CLASSIFICATION — title-only, strict categories
# ══════════════════════════════════════════════════════════════════════════
# FIX #13: classify on TITLE ONLY. Description text is untrusted — it often
# contains neighboring article text from the parent node, which causes
# cascading misclassification (e.g. a product article gets typed as
# "leadership" because a neighboring leadership article's text bled in).

# --- KEEP patterns (applied to title only) --------------------------------

_FUNDING_RE = re.compile(
    r'\bSeries\s+[A-Z]\b'
    r'|\braised?\s+\$\d'
    r'|\braises?\s+\$\d'
    r'|\bfunding\s+round\b'
    r'|\bsecured?\s+\$\d'
    r'|\bclosed?\s+\$\d'
    r'|\bvaluation\s+of\s+\$'
    r'|\bIPO\b'
    r'|\bgoes?\s+public\b',
    re.I,
)

# FIX #6 + #11: Leadership = exec APPOINTMENT or DEPARTURE only.
# Must have an explicit appointment/departure verb AND an exec title.
# Never fires on "leaders", "leading", "elevates", "expands", "reports" etc.
_LEADERSHIP_RE = re.compile(
    # Appointment verbs + exec title
    r'\b(?:appoints?|names?|hires?|promotes?)\b.{0,60}\b(?:CEO|CTO|CFO|CRO|CPO|COO|CMO|'
    r'Chief\s+\w+\s+Officer|Chief\s+\w+|President|EVP|SVP|VP\s+of)\b'
    # "joins as CEO/CTO/..."
    r'|\bjoins?\s+as\s+(?:new\s+)?(?:CEO|CTO|CFO|CRO|CPO|COO|CMO|Chief|President|SVP|VP\b)'
    # Departure verbs + exec title
    r'|\b(?:steps?\s+down|departs?|resigns?)\s+as\s+(?:CEO|CTO|CFO|CRO|CPO|COO|CMO|Chief|President)\b'
    # Bare "new CEO/CTO" as subject
    r'|\bnew\s+(?:CEO|CTO|CFO|CRO|CPO|COO|CMO)\b',
    re.I,
)

_ACQUISITION_RE = re.compile(
    r'\bacquires?\b'
    r'|\bacquired?\s+by\b'
    r'|\bacquisition\s+of\b'
    r'|\bmerger\s+with\b'
    r'|\bmerges?\s+with\b'
    r'|\bto\s+acquire\b',
    re.I,
)

_PARTNERSHIP_RE = re.compile(
    r'\bpartnership\s+with\b'
    r'|\bpartners?\s+with\b'
    r'|\bstrategic\s+(?:partnership|alliance|collaboration)\b'
    r'|\bjoint\s+(?:venture|solution|offering)\b'
    r'|\bteams?\s+up\s+with\b'
    r'|\bOEM\s+(?:deal|agreement)\b',
    re.I,
)

_PRODUCT_LAUNCH_RE = re.compile(
    r'\blaunch(?:es|ed|ing)\b'
    r'|\bintroduc(?:es|ed|ing)\b'
    r'|\bunveils?\b'
    r'|\bgeneral(?:ly)?\s+available\b|\bgeneral\s+availability\b|\bnow\s+GA\b|\breaches?\s+GA\b'
    r'|\bpublic(?:ly)?\s+available\b'
    r'|\bpublic\s+preview\b|\bopen\s+preview\b'
    r'|\bnow\s+available\b'
    r'|\bdebuts?\b'
    r'|\bannounces?\s+(?:new\s+)?(?:product|platform|solution|service|SDK|API|engine|'
    r'framework|tool|model|update|release|version)\b'
    # Plugin / integration / connector releases with AI platforms
    r'|\bplugin\s+for\s+(?:Claude|GPT|ChatGPT|OpenAI|Anthropic|Cursor|Copilot)\b'
    r'|\b(?:node|connector|integration)\s+(?:in|for|with)\s+(?:n8n|Zapier|Make|LangChain|'
    r'LlamaIndex|Vertex|Bedrock|Claude|GPT|OpenAI)\b'
    r'|\bMCP\s+(?:server|connector|plugin|tool|integration)\b'
    r'|\bnative\s+integration\s+with\s+(?:Claude|GPT|OpenAI|Anthropic|n8n|Zapier)\b',
    re.I,
)

_FEATURE_RE = re.compile(
    r'\bannounces?\s+(?:new\s+)?(?:capability|capabilities|feature|features|support\s+for|integration)\b'
    r'|\badds?\s+support\s+for\b'
    r'|\brolls?\s+out\b'
    r'|\bnew\s+(?:capability|capabilities|feature|features|integration)\b',
    re.I,
)

_LAYOFF_RE = re.compile(
    r'\blay\s*offs?\b|\blaid\s+off\b|\bjob\s+cuts?\b|\bworkforce\s+reduction\b'
    r'|\brestructur(?:es|ed|ing)\b',
    re.I,
)

_PRICING_RE = re.compile(
    r'\bpricing\b'
    r'|\bprice\s+(?:change|cut|increase|reduction|update)\b'
    r'|\bnew\s+(?:pricing|price|tier|plan)\b'
    r'|\bfree\s+(?:tier|plan|version)\b'
    r'|\bopen[- ]source(?:s|d|ing)\b'
    r'|\bfreemium\b'
    r'|\benterprise\s+pricing\b',
    re.I,
)

# --- HARD DROP (applied to title only) ------------------------------------
# FIX #7 + #8 + #9: Add earnings/financial results, research reports,
# customer stories using their product.
_DROP_RE = re.compile(
    # Thought leadership / opinion / trend pieces
    r'\bAI\s+will\s+(?:change|transform|disrupt|revolutionize|reshape)\b'
    r'|\bfuture\s+of\s+\w+\b'
    r'|\bwhy\s+\w+\s+matters?\b'
    r'|\bthe\s+rise\s+of\b'
    r'|\bstate\s+of\s+\w+\s+(?:in\s+\d{4}|report)\b'
    r'|\bthought\s+leadership\b'
    r'|\bopinion\b'
    r'|\bperspective\b'
    r'|\bdeep\s+dive\b'
    # Awards / analyst recognition
    r'|\bnamed\s+a\s+(?:leader|visionary|challenger|niche\s+player)\b'
    r'|\bmagic\s+quadrant\b'
    r'|\bgartner\b'
    r'|\bforrester\s+wave\b'
    r'|\bIDC\s+marketscape\b'
    r'|\brecogni[sz]ed\s+(?:as|by|in)\b'
    r'|\bwins?\s+(?:award|recognition)\b'
    r'|\baward(?:-winning|s)\b'
    r'|\bnamed\s+(?:leader|best|top)\b'
    # FIX #8: Earnings / financial results / growth metrics
    r'|\breports?\s+(?:financial|quarterly|annual|fiscal|q[1-4])\b'
    r'|\bearnings\b'
    r'|\bfiscal\s+(?:year|quarter|q[1-4])\b'
    r'|\brevenue\s+(?:results?|report|growth|run.rate)\b'
    r'|\brun.rate\b'
    r'|\bfull.year\s+(?:results?|revenue)\b'
    r'|\bfinancial\s+results?\b'
    r'|\bYoY\b|\byear.over.year\b'
    r'|\bsurpasses?\s+\$\d'       # "Surpasses $4.8B" — financial milestone
    r'|\bgrows?\s+>\d+%\b'        # "Grows >55% YoY"
    r'|\bcorporate\s+momentum\b'
    # FIX #7: Research / survey reports
    r'|\bsurvey\s+(?:finds|reveals|shows|by|of)\b'
    r'|\bnew\s+survey\b'
    r'|\bresearch\s+(?:finds|reveals|shows|report|reveals)\b'
    r'|\bbenchmark\s+report\b'
    r'|\bindustry\s+report\b'
    r'|\b\d+%\s+of\s+(?:\w+\s+){1,4}(?:say|report|find|believe|agree)\b'
    r'|\breveals\s+(?:that\s+)?\d+%\b'
    # FIX #9: Customer stories — third party "uses/with" pattern
    r'|\bcustomer\s+stor(?:y|ies)\b'
    r'|\bcase\s+stud(?:y|ies)\b'
    r'|\bsuccess\s+stor(?:y|ies)\b'
    r'|\b(?:elevates?|transforms?|empowers?|modernizes?|accelerates?)\s+.{5,60}\s+with\s+[A-Z]\w+\b'
    r'|\bhow\s+\w[\w\s]{2,30}\s+(?:uses?|leverages?|adopted?)\b'
    # Educational / marketing content
    r'|\bhow\s+to\b|\btutorial\b|\bguide\s+to\b|\bbeginners?\s+guide\b'
    r'|\bwhat\s+(?:they|it|is|are)\b'
    r'|\bhow\s+(?:they|it)\s+work\b'
    r'|\btips?\s+(?:and|&)\s+tricks\b|\bbest\s+practices?\b'
    r'|\bwebinar\b|\bpodcast\b|\binterview\s+with\b'
    r'|\bep(?:isode)?\s+\d+\b'
    # Event recaps
    r'|\brecap\b|\bhighlights?\s+from\b'
    # Vague positioning / thought leadership
    r'|\bbecause\s+\w+\s+needs?\s+(?:context|access|data|insights?)\b'
    r'|\bwhy\s+\w[\w\s]{0,30}\s+(?:matters?|is\s+critical)\b'
    r'|\bcritical\s+(?:infrastructure|foundation)\b'
    r'|\bdata\s+products?\s+(?:are|is)\b'
    # Generic AI rebranding (Activate AI without substance)
    r'|\bactivate\s+AI\b'
    # "How to" tutorials / guides
    r'|\bhow\s+to\s+make\s+your?\b'
    r'|\bin\s+\d+\s+minutes?\b'  # "...in 30 minutes" = quick how-to
    # Explicit webinar/conference titles (with dates/times)
    r'|\bwebinar\b'
    r'|\b(?:event|conference|summit)\s+.{1,100}(?:\d{1,2}:\d{2}\s*(?:AM|PM|CET|ET)|Apr|May|June|July|August|September|October|November|December)\b',
    re.I,
)


_NEWS_CLASSIFY_SYSTEM = """\
You are a competitive intelligence analyst at Actian Corporation — a data integration \
and analytics platform. Classify a competitor news item for the Actian team.

Return ONLY valid JSON. No commentary. No markdown fences.

Required fields:
- "news_type": one of exactly: funding, acquisition, leadership, partnership, pricing, \
product_launch, feature, layoff — or null to drop the item
- "actian_relevance": one of exactly: high, medium, low
- "tags": array of 0–4 short tags (e.g. ["Series C", "$120M", "AI", "CEO"])
- "summary": 1 sharp sentence explaining what happened and why it matters to Actian

Classification rules:
- leadership: C-suite appointment or departure only (CEO/CTO/CFO/CRO/CPO/COO/CMO)
- product_launch: new product, major feature GA, new capability announced
- feature: incremental improvement to existing product
- funding: investment round, IPO, or strategic investment
- acquisition: company acquired or acquiring another
- partnership: integration, technology alliance, OEM/reseller deal
- pricing: pricing change, new tier, or monetization announcement
- layoff: workforce reduction
- Return null for news_type if the item is a blog post, thought leadership, or general news \
with no competitive signal for Actian
"""


def _call_deepseek_news(title: str, description: str, company: str) -> dict | None:
    """Call DeepSeek to classify a news item. Returns parsed dict or None."""
    if not DEEPSEEK_API_KEY:
        return None
    user_msg = (
        f"Company: {company}\n"
        f"Title: {title}\n"
        f"Description: {description[:400] if description else '(none)'}"
    )
    try:
        r = httpx.post(
            "https://api.deepseek.com/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEEPSEEK_MODEL,
                "max_tokens": 400,
                "messages": [
                    {"role": "system", "content": _NEWS_CLASSIFY_SYSTEM},
                    {"role": "user", "content": user_msg},
                ],
            },
            timeout=60,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"].strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
        parsed = json.loads(text)
        if parsed.get("news_type") is None:
            return None  # DeepSeek says drop
        valid_types = {"funding", "acquisition", "leadership", "partnership",
                       "pricing", "product_launch", "feature", "layoff"}
        if parsed.get("news_type") not in valid_types:
            return None
        if parsed.get("actian_relevance") not in ("high", "medium", "low"):
            parsed["actian_relevance"] = "high"
        parsed.setdefault("tags", [])
        parsed.setdefault("summary", title)
        return parsed
    except Exception as e:
        print(f"  [WARN] DeepSeek news classify failed: {e}")
        return None


def classify_item(company: str, title: str, description: str, url: str) -> dict | None:
    """
    Classify a news item. Tries DeepSeek first; falls back to title-only rule matching.
    Description is intentionally ignored in the rule path (untrusted neighbor text).
    Returns dict or None (drop).
    """
    # Hard drop on title — always run this regardless of LLM path
    if _DROP_RE.search(title):
        return None

    # Try DeepSeek first
    if DEEPSEEK_API_KEY:
        result = _call_deepseek_news(title, description, company)
        if result is not None:
            result["team_routing"] = route_by_news_type(result["news_type"])
            result["themes"] = classify_themes(title, description, result.get("summary", ""))
            return result
        # result == None means DeepSeek said drop the item
        # but we only trust that if the API call succeeded; a connection error
        # returns None too — fall through to rule-based to be safe

    # ── Rule-based fallback ──────────────────────────────────────────────
    t = title  # title-only for classification

    news_type = None
    if _FUNDING_RE.search(t):
        news_type = "funding"
    elif _ACQUISITION_RE.search(t):
        news_type = "acquisition"
    elif _LEADERSHIP_RE.search(t):
        news_type = "leadership"
    elif _PARTNERSHIP_RE.search(t):
        news_type = "partnership"
    elif _PRICING_RE.search(t):
        news_type = "pricing"
    elif _PRODUCT_LAUNCH_RE.search(t):
        news_type = "product_launch"
    elif _FEATURE_RE.search(t):
        news_type = "feature"
    elif _LAYOFF_RE.search(t):
        news_type = "layoff"
    else:
        return None

    relevance = "high"

    tags = []
    m = re.search(r'\$\s*(\d+(?:\.\d+)?)\s*([MB])', title)
    if m:
        tags.append(f"${m.group(1)}{m.group(2)}")
    m = re.search(r'Series\s+([A-Z])\b', title, re.I)
    if m:
        tags.append(f"Series {m.group(1).upper()}")
    for exec_title in ("CEO", "CTO", "CFO", "CRO", "CPO", "COO", "CMO"):
        if re.search(rf'\b{exec_title}\b', title):
            tags.append(exec_title)
            break
    if re.search(r'\b(?:AI|ML|agent|LLM|vector)\b', title, re.I):
        tags.append("AI")

    return {
        "news_type": news_type,
        "actian_relevance": relevance,
        "tags": tags,
        "team_routing": route_by_news_type(news_type),
        "themes": classify_themes(title, description),
    }


# ══════════════════════════════════════════════════════════════════════════
# DATE EXTRACTION
# ══════════════════════════════════════════════════════════════════════════

def extract_date_from_url(url: str) -> str:
    """Extract publication date embedded in URL paths (TechCrunch, BusinessWire, etc.)"""
    if not url:
        return ""
    # TechCrunch / Bloomberg style: /2021/04/15/
    m = re.search(r'/(20\d{2})/(\d{2})/(\d{2})/', url)
    if m:
        try:
            return date.fromisoformat(f"{m.group(1)}-{m.group(2)}-{m.group(3)}").isoformat()
        except Exception:
            pass
    # BusinessWire / PRNewswire style: /home/20220922005362/
    m = re.search(r'/(20\d{2})(\d{2})(\d{2})\d{1,6}/', url)
    if m:
        try:
            return date.fromisoformat(f"{m.group(1)}-{m.group(2)}-{m.group(3)}").isoformat()
        except Exception:
            pass
    return ""


def extract_date(html: str) -> str:
    if not html:
        return ""

    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        try:
            return date.fromisoformat(m.group(1).split("T")[0]).isoformat()
        except Exception:
            pass

    m = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html, re.I)
    if m:
        try:
            return date.fromisoformat(m.group(1).split("T")[0]).isoformat()
        except Exception:
            pass

    m = re.search(r'(\d{4}-\d{2}-\d{2})', html)
    if m:
        return m.group(1)

    months_long = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'
    m = re.search(rf'({months_long})\s+(\d{{1,2}}),\s+(\d{{4}})', html, re.I)
    if m:
        try:
            from datetime import datetime
            dt = datetime.strptime(f"{m.group(1)} {m.group(2).zfill(2)} {m.group(3)}", "%B %d %Y")
            return dt.date().isoformat()
        except Exception:
            pass

    months_short = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    m = re.search(rf'(\d{{1,2}})\s+({months_short})\s+(\d{{4}})', html, re.I)
    if m:
        try:
            from datetime import datetime
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %b %Y")
            return dt.date().isoformat()
        except Exception:
            pass

    m = re.search(rf'({months_short})\s+(\d{{1,2}}),?\s+(\d{{4}})', html, re.I)
    if m:
        try:
            from datetime import datetime
            dt = datetime.strptime(f"{m.group(1)} {m.group(2).zfill(2)} {m.group(3)}", "%b %d %Y")
            return dt.date().isoformat()
        except Exception:
            pass

    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', html)
    if m:
        try:
            return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
        except Exception:
            pass

    m = re.search(r'posted\s+(\d+)\s+days?\s+ago', html, re.I)
    if m:
        try:
            return (date.today() - timedelta(days=int(m.group(1)))).isoformat()
        except Exception:
            pass

    return ""


# ══════════════════════════════════════════════════════════════════════════
# ARTICLE-PAGE DATE FETCHER
# Fetches individual article pages to extract publication dates.
# Called when the listing page didn't expose a date for an article.
# Tries httpx first (fast); falls back to Playwright for JS-heavy pages.
# ══════════════════════════════════════════════════════════════════════════

# Companies whose article pages are JS-rendered and need Playwright
PLAYWRIGHT_ARTICLE_DOMAINS = {
    # JS-heavy article pages where httpx alone won't expose publish dates
    "databricks.com", "snowflake.com", "collibra.com",
    "pinecone.io", "atlan.com", "bigeye.com",
    "milvus.io", "zilliz.com", "alation.com",
}


def _html_to_date(html: str, url: str = "") -> str:
    """
    Try every date pattern on arbitrary HTML. Returns ISO date or ''.

    TRUST HIERARCHY:
      1. Structured (always trusted): JSON-LD, article:published_time meta, <time datetime>
      2. Semi-structured (trusted if NOT today): Drupal MM/DD/YYYY
      3. Text patterns (trusted if NOT today): "Month DD YYYY" in page body

    Reason: many CMS platforms inject today's date into freshness headers,
    copyright footers, or last-modified fields. Returning today's date from
    a text match is almost certainly a dynamic CMS artifact, not a real
    publish date. We only accept today from structured metadata.
    """
    if not html:
        return ""
    today_iso = date.today().isoformat()

    # ── TIER 1: structured metadata — always trusted ──────────────────────
    # JSON-LD datePublished / dateCreated
    for pat in (
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'"dateCreated"\s*:\s*"([^"]+)"',
        r'"date"\s*:\s*"(20\d{2}-\d{2}-\d{2}[^"]*)"',
    ):
        m = re.search(pat, html)
        if m:
            try:
                return date.fromisoformat(m.group(1).split("T")[0]).isoformat()
            except Exception:
                pass
    # <meta property="article:published_time" content="...">
    m = re.search(
        r'(?:article:published_time|og:published_time|datePublished)'
        r'["\s]+content=["\']([^"\']+)["\']',
        html, re.I,
    )
    if not m:
        m = re.search(
            r'content=["\']([^"\']+)["\'][^>]+'
            r'(?:article:published_time|og:published_time)',
            html, re.I,
        )
    if m:
        try:
            return date.fromisoformat(m.group(1).split("T")[0]).isoformat()
        except Exception:
            pass
    # <time datetime="...">
    m = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html, re.I)
    if m:
        try:
            return date.fromisoformat(m.group(1).split("T")[0]).isoformat()
        except Exception:
            pass

    # ── TIER 2: semi-structured — reject if result = today ────────────────
    # Drupal / CMS: "Tue, 03/10/2026 - 23:18"
    m = re.search(r'\b(\d{2})/(\d{2})/(20\d{2})\b', html[:5000])
    if m:
        try:
            d = date.fromisoformat(f"{m.group(3)}-{m.group(1)}-{m.group(2)}").isoformat()
            if d != today_iso:
                return d
        except Exception:
            pass

    # ── TIER 3: text patterns — reject if result = today ─────────────────
    # Text: "Month DD, YYYY" in first 6 KB
    m = re.search(
        rf'({_MONTHS})\s+(\d{{1,2}}),?\s+(20\d{{2}})',
        html[:6000], re.I,
    )
    if m:
        try:
            from datetime import datetime
            d = datetime.strptime(
                f"{m.group(1)[:3].title()} {m.group(2).zfill(2)} {m.group(3)}",
                "%b %d %Y",
            ).date().isoformat()
            if d != today_iso:
                return d
        except Exception:
            pass
    # Text: "DD Month YYYY" — reject if today
    m = re.search(
        rf'(\d{{1,2}})\s+({_MONTHS})\s+(20\d{{2}})',
        html[:6000], re.I,
    )
    if m:
        try:
            from datetime import datetime
            d = datetime.strptime(
                f"{m.group(1)} {m.group(2)[:3].title()} {m.group(3)}",
                "%d %b %Y",
            ).date().isoformat()
            if d != today_iso:
                return d
        except Exception:
            pass
    # URL-embedded date as fallback (always trusted — dates in URLs are set at publish time)
    if url:
        d = extract_date_from_url(url)
        if d:
            return d
    return ""


def fetch_article_date(url: str) -> str:
    """
    Fetch the article page and return its publication date.
    Tries httpx first; falls back to Playwright for JS-heavy domains.
    Returns ISO date string or '' if not found.
    """
    domain = urlparse(url).netloc.lstrip("www.")

    # --- httpx attempt (fast, works for most sites) ---
    html = ""
    try:
        r = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            follow_redirects=True,
            timeout=15,
        )
        if r.status_code == 200:
            html = r.text
    except Exception:
        pass

    d = _html_to_date(html, url)
    if d:
        return d

    # --- Playwright fallback for JS-rendered article pages ---
    if any(domain.endswith(pw) for pw in PLAYWRIGHT_ARTICLE_DOMAINS):
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
                html = page.content()
                browser.close()
            d = _html_to_date(html, url)
            if d:
                return d
        except Exception:
            pass

    return ""


# ══════════════════════════════════════════════════════════════════════════
# PLAYWRIGHT SUPPORT
# ══════════════════════════════════════════════════════════════════════════

PLAYWRIGHT_NEWSROOMS = {
    # These companies' newsroom/blog pages are JS-rendered and need Playwright.
    # Adding Milvus (zilliz.com/news is React) and Alation (Next.js blog).
    "Bigeye", "Atlan", "Pinecone", "Collibra", "Databricks", "Snowflake",
    "Milvus", "Alation",
}


def fetch_newsroom_playwright(url: str) -> str:
    from playwright.sync_api import sync_playwright

    for wait_event in ("domcontentloaded", "load", "commit"):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                )
                page.goto(url, timeout=45000, wait_until=wait_event)
                page.wait_for_timeout(2000)

                consent_selectors = [
                    "button#onetrust-accept-btn-handler",
                    "button.onetrust-accept-btn-handler",
                    "button[id*='accept'][id*='cookie']",
                    "button[class*='accept-all']",
                    "button[aria-label*='Accept']",
                    "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
                    "button.js-cookie-accept",
                ]
                for sel in consent_selectors:
                    try:
                        btn = page.query_selector(sel)
                        if btn and btn.is_visible():
                            btn.click()
                            page.wait_for_timeout(1500)
                            break
                    except Exception:
                        pass

                html = page.content()
                browser.close()
                if len(html) > 5000:
                    return html
        except Exception:
            pass
    return ""


# ══════════════════════════════════════════════════════════════════════════
# TITLE CLEANING
# ══════════════════════════════════════════════════════════════════════════

# FIX #4 + #5: Nav/CTA links — short generic titles from page navigation
_NAV_TITLE_RE = re.compile(
    r'^(?:get\s+started|sign\s+up|log\s+in|learn\s+more|read\s+more|view\s+all|'
    r'see\s+all|explore|download|contact\s+us|free\s+trial|request\s+demo|'
    r'book\s+a\s+demo|try\s+for\s+free|start\s+free|watch\s+now|'
    r'business\s+critical\s+plan|enterprise\s+plan|'
    # 1-2 word product/nav names that bleed through (Pinecone "Vector Database", "Dedicated Read Nodes")
    r'vector\s+database|dedicated\s+read\s+nodes?|sparse\s+dense|'
    r'(?:resource|architecture|learning|help|partner|knowledge|solution|'
    r'community|developer|support|documentation|training|certification)\s+'
    r'(?:center|hub|portal|base|zone|library|page))$',
    re.I,
)

_MONTHS = (
    r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
)

# FIX #12: Strip "Press Release", "News", "Article", "Featured" prefixes
_TITLE_PREFIX_RE = re.compile(
    r'^(?:Press\s+Release|News|Article|Featured|Blog|Announcement|Update)\s*[:\-–]?\s+',
    re.I,
)

# FIX #2: Strip trailing " Source Date" suffixes like " Apr 8, 2026 TechTarget"
# and author names like " Andre Zayarni March 12, 2026"
# Date/source-based suffix strip — always safe to apply
_TITLE_DATE_SUFFIX_RE = re.compile(
    # " Apr 8, 2026 TechTarget" / " March 12, 2026 Source"
    rf'\s+{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}}(?:\s+\w[\w\s]{{0,30}})?$'
    # " 2026-03-12 Source"
    r'|\s+\d{4}-\d{2}-\d{2}(?:\s+\w[\w\s]{0,30})?$'
    # Day-Month-Year trailing: "22 Sept 2022" or "22 September 2022"
    rf'|\s+\d{{1,2}}\s+{_MONTHS}\s+\d{{4}}(?:\s+\w[\w\s]{{0,30}})?$'
    # "Press Release 22 Sept 2022" / "Press Release April 2023" / bare "Press Release"
    rf'|\s+Press\s+Release(?:\s+\d{{1,2}}\s+{_MONTHS}\s+\d{{4}}|\s+{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}})?$'
    # " by Firstname Lastname" — exactly 2 title-case words after "by"
    r'|\s+by\s+[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}$'
    # "Name , Name , Name" comma-separated authors — e.g. "Gavin , Jeff , Brad"
    r'|\s+[A-Z][a-z]{2,}(?:\s*,\s*[A-Z][a-z]{2,})+\s*$'
    # Single initial at end " , J"
    r'|\s*,\s*[A-Z](?:\s*,\s*[A-Z])*\s*$'
    # "- Learn more" / "— Learn more"
    r'|\s*[-–—]\s*(?:Learn\s+more|Read\s+more|View\s+more|See\s+more|Find\s+out\s+more)\s*$',
    re.I,
)

# Bare author name — ONLY applied after a date has already been stripped
# (prevents stripping "Revenue Officer", "Generally Available", "Business User" etc.)
_TITLE_NAME_SUFFIX_RE = re.compile(
    r'\s+[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}$',
    re.I,
)

# Keep _TITLE_SUFFIX_RE as alias for backward compat (not used directly anymore)
_TITLE_SUFFIX_RE = _TITLE_DATE_SUFFIX_RE


def clean_title(raw: str, source_url: str = "") -> str:
    """Strip leading prefixes and trailing date/author/source from titles.

    source_url: the page URL this title was scraped from. When it contains
    '/blog/', blog listings often embed author names without a date, so we
    apply the bare-name strip unconditionally (still guarded by length).
    """
    t = raw.strip()
    # Strip leading emoji / symbol characters
    t = re.sub(r'^[\U0001F000-\U0001FFFF\U00002600-\U000027FF\U0000FE00-\U0000FEFF\s]+', '', t).strip()
    # Leading prefixes: "Press Release ...", "News ..."
    t = _TITLE_PREFIX_RE.sub('', t).strip()
    # Leading date patterns
    t = re.sub(r'^\d{1,2}/\d{1,2}/\d{4}\s*', '', t).strip()
    t = re.sub(rf'^[A-Za-z ,]+\s*[-–]\s*{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}}\s*', '', t).strip()
    t = re.sub(rf'^\w+\s+{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}}\s*', '', t).strip()
    t = re.sub(rf'^{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}}\s*', '', t).strip()
    t = re.sub(rf'^\d{{1,2}}\s+{_MONTHS}\s+\d{{4}}\s*', '', t).strip()
    # Trailing date/author/source — apply date-based strip first
    date_stripped = False
    for _ in range(3):
        cleaned = _TITLE_DATE_SUFFIX_RE.sub('', t).strip()
        if cleaned == t:
            break
        date_stripped = True
        t = cleaned
    # Bare author name strip:
    # • Always on blog listing pages (source URL contains "/blog/") — blog
    #   listings commonly append "Firstname Lastname" without a preceding date.
    # • Also fires after a date suffix was already stripped (existing behaviour).
    # Guard: resulting title must still be >= 40 chars to avoid over-stripping.
    is_blog_source = "/blog/" in source_url.lower() or source_url.lower().endswith("/blog")
    if date_stripped or is_blog_source:
        cleaned = _TITLE_NAME_SUFFIX_RE.sub('', t).strip()
        if len(cleaned) >= 40:
            t = cleaned
    # Truncate blurb-heavy titles — signals that tagline/body text was captured
    if len(t) > 100:
        # Split at ". " or "\n"
        first_sent = re.split(r'\.\s+|\n', t, maxsplit=1)
        if first_sent and 30 <= len(first_sent[0].strip()) < len(t):
            t = first_sent[0].strip().rstrip('.')
        # Split at closing paren + space + Capital word (tagline after abbreviation)
        # e.g. "...Data Management (ADM) Reimagining..." → cut at "(ADM)"
        if len(t) > 80:
            m = re.search(r'(\))\s+[A-Z][a-z]', t)
            if m and m.start(1) >= 30:
                t = t[:m.start(1) + 1].strip()
        # Hard truncate at word boundary
        if len(t) > 100:
            t = t[:100].rsplit(' ', 1)[0].rstrip(',:;–-')
    return t if len(t) >= 15 else raw.strip()


# ══════════════════════════════════════════════════════════════════════════
# NEWSROOM SCRAPER
# ══════════════════════════════════════════════════════════════════════════

def fetch_newsroom(company: str, url: str) -> list[dict]:
    html = ""

    if company in PLAYWRIGHT_NEWSROOMS:
        html = fetch_newsroom_playwright(url)
    else:
        try:
            r = httpx.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                follow_redirects=True,
                timeout=30,
            )
            r.raise_for_status()
            html = r.text
        except Exception as e:
            print(f"  [WARN] {company}: Fetch failed — {e}")
            return []

    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    articles = []
    seen_urls_local: set[str] = set()   # FIX #1/#14: dedup within fetch by URL
    seen_titles_local: set[str] = set() # also dedup by cleaned title

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        raw_title = a.get_text(separator=" ", strip=True)

        if href.startswith("/"):
            from urllib.parse import urljoin
            href = urljoin(url, href)
        elif not href.startswith("http"):
            continue

        # Skip very short nav links
        if len(raw_title) < 15:
            continue

        # Truncate + clean Collibra-style "Location - Date Title body..."
        if len(raw_title) > 200:
            clean = re.sub(r'^[A-Za-z ,]+\s*[-–]\s*\w+ \d+, \d{4}\s*', '', raw_title).strip()
            raw_title = clean[:200] if len(clean) >= 15 else raw_title[:200]

        # FIX #12 + #2: clean title before using it anywhere.
        # Pass source URL so blog listing author names can be stripped.
        title = clean_title(raw_title, source_url=url)

        # FIX #4: drop nav link titles
        if _NAV_TITLE_RE.match(title):
            continue

        # Dedup by URL and by cleaned title within this fetch
        if href in seen_urls_local:
            continue
        if title in seen_titles_local:
            continue

        # Only keep links that look like press releases or news articles
        news_domains = (
            "einpresswire", "prnewswire", "datanami", "techcrunch",
            "venturebeat", "crn", "forbes", "medium", "businesswire",
            "globenewswire", "prnews", "techradar", "zdnet", "infoq",
            "theregister", "wired", "bloomberg", "wsj", "reuters",
        )
        blog_patterns = ("blog", "press", "newsroom", "news", "release", "announcement")
        is_external_news = any(domain in href.lower() for domain in news_domains)
        is_blog_post = any(pattern in href.lower() for pattern in blog_patterns)

        if not (is_external_news or is_blog_post):
            continue

        # FIX #3: extract description from IMMEDIATE parent only.
        # Subtract the link's own text. Require >= 40 meaningful chars.
        # Never walk up the tree — that's what caused summaries to contain
        # neighboring articles' full text.
        ctx = ""
        date_html = str(a)
        immediate = a.parent
        if immediate is not None:
            date_html = immediate.decode_contents()
            full = immediate.get_text(separator=" ", strip=True)
            link_text = a.get_text(separator=" ", strip=True)
            remainder = full.replace(link_text, "", 1).strip()
            # Must be substantive text, not just a date or category label
            if (len(remainder) >= 40
                    and not re.fullmatch(r'[\d/\-,.\s\w]{1,30}', remainder)
                    # FIX #3: reject if remainder looks like a list of other headlines
                    # (contains multiple dates inline = neighbor bleed-through)
                    and len(re.findall(rf'{_MONTHS}\s+\d{{1,2}},?\s+\d{{4}}', remainder)) < 2):
                ctx = remainder[:300]

        pub_date = (extract_date(title)
                    or extract_date_from_url(href)
                    or extract_date(date_html)
                    or extract_date(ctx))

        # TODAY-GUARD on listing-page dates:
        # CMS listing pages sometimes inject today's date into the surrounding
        # HTML (e.g. Snowflake shows the most-recent article's date in a shared
        # parent element, which bleeds onto adjacent articles). If we got today's
        # date from the listing page, clear it — the article-page fetcher in main()
        # will fetch the real publish date. Genuinely-today articles will re-confirm
        # via Tier 1 structured metadata (JSON-LD / article:published_time) on their
        # own article page, which is always trusted.
        if pub_date == today_str:
            pub_date = ""

        seen_urls_local.add(href)
        seen_titles_local.add(title)
        articles.append({
            "title": title,
            "url": href,
            "published_date": pub_date,
            "description": ctx,
        })

        if len(articles) >= MAX_ITEMS_PER_COMPANY:
            break

    return articles


def within_window(published_date_str: str) -> bool:
    """
    Empty date = drop. fetch_article_date() already tried to get the date;
    if it's still empty the item is unverifiable and could be years old.
    """
    if not published_date_str:
        return False
    try:
        pub_date = date.fromisoformat(published_date_str)
        return (date.today() - pub_date).days <= MAX_NEWS_AGE_DAYS
    except Exception:
        return False


def clean_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip() if text else ""


def _clean_summary(desc: str, title: str) -> str:
    """Return teaser only if it adds real info beyond the title."""
    d = clean_text(desc)
    t = clean_text(title)
    if not d or len(d) < 40:
        return ""
    dl, tl = d.lower(), t.lower()
    if dl.startswith(tl) or tl.startswith(dl):
        return ""
    if tl and len(tl) > 15 and tl in dl and len(dl) < len(tl) * 1.4:
        return ""
    return d


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 70)
    print("NEWS SCRAPER — strict classification, title-only")
    print("=" * 70)

    seen_urls: set[str] = set()
    if SEEN_FILE.exists():
        try:
            seen_urls = set(json.loads(SEEN_FILE.read_text()))
        except Exception:
            pass

    existing_news: list[dict] = []
    if OUTPUT_FILE.exists():
        try:
            existing_news = json.loads(OUTPUT_FILE.read_text())
        except Exception:
            pass

    # FIX #1/#14: build existing URL index for global dedup
    existing_by_url: dict[str, dict] = {n["url"]: n for n in existing_news}

    new_news: list[dict] = []

    for company, url_entry in NEWSROOM_URLS.items():
        product_area = V2_PRODUCT_AREA_MAP.get(company)
        if not product_area:
            continue

        urls_to_scrape = url_entry if isinstance(url_entry, list) else [url_entry]
        articles: list[dict] = []
        for newsroom_url in urls_to_scrape:
            print(f"\n[{company}] {newsroom_url}")
            articles.extend(fetch_newsroom(company, newsroom_url))

        # Dedup across multiple URLs by URL
        seen_in_batch: set[str] = set()
        deduped = []
        for a in articles:
            if a["url"] not in seen_in_batch:
                seen_in_batch.add(a["url"])
                deduped.append(a)
        articles = deduped

        if not articles:
            print(f"  No articles found")
            continue

        print(f"  {len(articles)} articles extracted")
        added = 0

        for article in articles:
            url = article["url"]
            title = article["title"]
            desc = article["description"]

            # FIX #1/#14: skip if already in seen set OR already in news.json
            if url in seen_urls or url in existing_by_url:
                continue

            # Nav title check (redundant safety net)
            if _NAV_TITLE_RE.match(title.strip()):
                seen_urls.add(url)
                continue

            # If the listing page didn't give us a date, fetch the article page.
            # Do this BEFORE the window check so we can drop genuinely old content.
            if not article["published_date"]:
                fetched = fetch_article_date(url)
                if fetched:
                    article["published_date"] = fetched
                    print(f"    ↳ date fetched: {fetched}")

            if not within_window(article["published_date"]):
                seen_urls.add(url)
                continue

            classification = classify_item(company, title, desc, url)
            if not classification:
                seen_urls.add(url)
                continue

            news_item = {
                "company": company,
                "product_area": product_area,
                "news_type": classification["news_type"],
                "title": clean_text(title),
                "url": url,
                "published_date": article["published_date"],
                "source": "company_newsroom",
                "summary": classification.get("summary") or _clean_summary(desc, title),
                "actian_relevance": classification["actian_relevance"],
                "tags": classification["tags"],
                "team_routing": classification["team_routing"],
                "themes": classification.get("themes", []),
                "event_date": None,
                "scraped_at": today_str,
            }

            new_news.append(news_item)
            existing_by_url[url] = news_item  # prevent double-add in same run
            seen_urls.add(url)
            added += 1

            type_icon = {
                "funding": "💰", "leadership": "👤", "product_launch": "🚀",
                "feature": "✨", "partnership": "🤝", "acquisition": "🔀",
                "layoff": "📉", "pricing": "💲",
            }.get(classification["news_type"], "📰")
            print(f"  → {type_icon} {title[:65]}")

        print(f"  ✓ {added} new item(s) added")

    # FIX #1/#14: merge and hard-dedup by URL before writing
    all_news = list(existing_by_url.values())
    # Add new items not already in existing_by_url
    for n in new_news:
        if n["url"] not in {x["url"] for x in all_news}:
            all_news.append(n)

    # Drop items outside rolling window (only where date is known)
    cutoff = (date.today() - timedelta(days=MAX_NEWS_AGE_DAYS)).isoformat()
    all_news = [
        n for n in all_news
        if not n.get("published_date") or n["published_date"] >= cutoff
    ]

    # Sort newest first
    all_news.sort(key=lambda x: x.get("published_date") or "0000", reverse=True)

    OUTPUT_FILE.write_text(json.dumps(all_news, indent=2))
    print(f"\n✓ Saved {len(all_news)} news items (URL-deduped) to {OUTPUT_FILE}")

    SEEN_FILE.write_text(json.dumps(sorted(seen_urls), indent=2))
    print(f"✓ Tracked {len(seen_urls)} seen URLs")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
