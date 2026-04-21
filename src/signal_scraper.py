"""
signal_scraper.py — Competitive Signal Scraper for Direct Intelligence
───────────────────────────────────────────────────────────────────────
• Scrapes RSS feeds from 11 allowed v2 companies (launches, events, blog posts)
• Classifies each item using Claude Sonnet (not Haiku — quality matters here)
• Deduplicates by URL across runs (file-based: data/seen_signals.json)
• Rolling 90-day window — drops stale items automatically
• Filters low-quality Collibra RSS event pages (individual /events/ links)
• Skips low-relevance blog posts to reduce noise
• Outputs: data/competitive_signals.json
"""

import json
import os
import re
import time
from datetime import date
from pathlib import Path

import feedparser
import httpx

# ══════════════════════════════════════════════════════════════════════════
# CONFIG — exact same pattern as enrich.py
# ══════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SONNET_MODEL = "claude-sonnet-4-6"

MAX_SIGNAL_AGE_DAYS = 90
MAX_ITEMS_PER_FEED = 20   # Cap per company per run — avoid flooding

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "competitive_signals.json"
SEEN_FILE   = DATA_DIR / "seen_signals.json"

# ══════════════════════════════════════════════════════════════════════════
# ALLOWED COMPANIES — strict, deterministic, no fallback
# Must stay in sync with V2_PRODUCT_AREA_MAP in dashboard_v2.html
# ══════════════════════════════════════════════════════════════════════════

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

# ══════════════════════════════════════════════════════════════════════════
# FEED MAP — verified URLs only. RSS where it exists, HTML fallback otherwise.
# Tested 2026-04-09. Update when feeds change.
# ══════════════════════════════════════════════════════════════════════════

# RSS and HTML blog sources deliberately removed.
# Blogs pull resources/tutorials/guides/thought-leadership — not competitive intel.
# ONLY event pages are scraped for launches + events. That is the sole truth.
RSS_FEEDS: dict = {}
HTML_SOURCES: dict = {}

# ══════════════════════════════════════════════════════════════════════════
# EVENT URLS — direct event listing pages per company
# Qdrant excluded — events page non-functional per user note
# Milvus uses Zilliz (parent company) events page
# ══════════════════════════════════════════════════════════════════════════

EVENT_URLS = {
    "Collibra":   "https://www.collibra.com/events",
    "Databricks": "https://www.databricks.com/events",
    "Bigeye":     "https://www.bigeye.com/events",
    "Pinecone":   "https://www.pinecone.io/community/",
    "Atlan":      "https://atlan.com/events/",
    "Alation":    "https://www.alation.com/events/",
    "Milvus":     "https://zilliz.com/event",
    "Acceldata":  "https://www.acceldata.io/events",
    "Snowflake":  "https://www.snowflake.com/en/events/all-events/",
}

# Companies whose event pages require JavaScript execution (React/Next.js)
# These will use Playwright instead of httpx for event page scraping.
PLAYWRIGHT_EVENT_PAGES = {"Atlan", "Acceldata", "Alation", "Bigeye", "Milvus", "Databricks", "Snowflake"}

# ══════════════════════════════════════════════════════════════════════════
# HARD URL BLOCKLIST — drop any URL matching these path segments, regardless
# of how the scraper found it. Product pages, resource pages, docs, etc.
# are never competitive intelligence.
# ══════════════════════════════════════════════════════════════════════════

_BLOCKED_URL_PATHS_RE = re.compile(
    r'/product(?:s)?/'           # /product/ or /products/
    r'|/solution(?:s)?/'         # /solution/ or /solutions/
    r'|/platform(?:s)?/'         # /platform/
    r'|/resource(?:s)?/'         # /resources/
    r'|/documentation'           # /documentation
    r'|/docs/'                   # /docs/
    r'|/learn/'                  # /learn/
    r'|/guide(?:s)?/'            # /guide/ or /guides/
    r'|/tutorial(?:s)?/'         # /tutorial/
    r'|/training/'               # /training/
    r'|/certification/'          # /certification/
    r'|/glossary/'               # /glossary/
    r'|/case-stud'               # /case-study, /case-studies
    r'|/customer(?:s)?/'         # /customer/ or /customers/
    r'|/use-case(?:s)?/'         # /use-case/
    r'|/partner(?:s)?/'          # /partners/ (partner listing, not event)
    r'|/about(?:/company)?/?$'   # /about, /about/company (bare pages)
    r'|/careers?/'               # /career/ or /careers/
    r'|/job(?:s)?/',             # /jobs/
    re.I,
)


def is_blocked_url(url: str) -> bool:
    """Return True if this URL should never be scraped (product page, docs, etc.)."""
    return bool(_BLOCKED_URL_PATHS_RE.search(url))


# ══════════════════════════════════════════════════════════════════════════
# TEXT CLEANING — strip noise before classification
# ══════════════════════════════════════════════════════════════════════════

# Prefixes like "Product Apr 15 · " or "News May 2 ," at start of titles
_DATE_PREFIX_RE = re.compile(
    r'^(?:Product|News|Blog|Engineering|Company|Updates?|Resources?)\s+'
    r'[A-Z][a-z]{2,8}\.?\s+\d{1,2}[,·.\s]+',
    re.I,
)
# Inline dates like "Apr 15, 2026" or "April 15 2026"
_INLINE_DATE_RE = re.compile(
    r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s*\d{4}\b',
    re.I,
)
# "by First Last" author attribution
_AUTHOR_RE = re.compile(r'\bby\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b')


def clean_text(text: str) -> str:
    """Strip dates, author names, and category prefixes from free text."""
    if not text:
        return ""
    text = _DATE_PREFIX_RE.sub("", text)
    text = _INLINE_DATE_RE.sub("", text)
    text = _AUTHOR_RE.sub("", text)
    return re.sub(r'\s+', ' ', text).strip()


# ══════════════════════════════════════════════════════════════════════════
# EVENT GATING — hard rules for what constitutes a real competitive signal
# ══════════════════════════════════════════════════════════════════════════

# Strong product launch signals — explicit language that overrides event classification.
# These phrases mean the content IS about a product launch, not just a company event.
# Used in classify_item() and the event page loop to catch launches on event pages too.
_PRODUCT_LAUNCH_STRONG_RE = re.compile(
    r'\bproduct\s+launch\b'                           # "product launch" exact phrase
    r"|we'?re\s+launching"                             # "we're launching"
    r'|officially\s+(?:launch|releas)'                 # "officially launching/releasing"
    # verb + product noun (direct): "launches platform", "releases feature", "ships update"
    # Covers: launches/launched/launching, releases/released, introduces/introduced,
    #         ships/shipped/shipping, unveils/unveiled
    r'|(?:launch(?:es|ed|ing)|releas(?:es|ed|ing)|introduc(?:es|ed|ing)'
      r'|unveil(?:s|ed|ing)|ship(?:s|ped|ping))\s+'
      r'(?:new\s+)?(?:a\s+|our\s+|its\s+)?'
      r'(?:product|feature|platform|tool|integration|capability|solution|version|update'
      r'|catalog|database|db|service|model|agent|agents|workflow|engine|hub|studio|suite|connector|pipeline|assistant|copilot)\b'
    # verb + "new [multi-word]... noun": "introduces new data catalog platform"
    r'|(?:launch(?:es|ed|ing)|releas(?:es|ed|ing)|introduc(?:es|ed|ing)'
      r'|unveil(?:s|ed|ing)|ship(?:s|ped|ping))\s+(?:new\s+|a\s+|our\s+|its\s+).{2,60}'
      r'(?:product|feature|platform|tool|integration|capability|solution|version|update'
      r'|catalog|database|db|service|model|agent|agents|workflow|engine|hub|studio|suite|connector|pipeline|assistant|copilot)\b'
    # bare "launches/introduces" as verb in headline — not followed by event/conference nouns
    # "Atlan launches Context Engineering Studio" / "Acceldata introduces DataOps Hub"
    r'|\b(?:launches|introduces)\s+'
      r'(?!(?:at|in|with|from|to|on|for|its|the)\s+(?:\w+\s+){0,3}(?:conference|summit|meetup|webinar|event|tour)\b)'
    r'|(?:new\s+)?(?:product|feature|platform|capability)\s+(?:launch|releas|announc)\b'
    r'|\bnow\s+generally\s+available\b'
    r'|\bgeneral\s+availability\b'
    r'|\bga\s+(?:release|launch|today|announcement)\b'
    r"|what'?s\s+new\s+in\b"
    r"|today\s+we'?re\s+(?:launch|announc|introduc|releas)"
    r'|\bannouncing\s+(?:the\s+launch\s+of|general\s+availability|our\s+new)\b',
    re.I,
)

# Signals that indicate a real-world event (product, market movement)
_EVENT_GATE_RE = re.compile(
    r'\blaunch(?:ed|ing)?\b'
    r'|releas(?:ed|ing|e)\b'
    r'|\bga\b|general\s+availability'
    r'|announc(?:ed|ing|ement)\b'
    r'|new\s+feature'
    r'|now\s+available|available\s+now'
    r'|\bpricing\b|price\s+change'
    r'|partner(?:ship|ed|ing)\b'
    r'|integrat(?:ion|ed|ing)\s+with'
    r'|acqui(?:red|sition)\b'
    r'|funding\s+round|raised?\s+\$',
    re.I,
)

# Pure noise patterns — hard exclude regardless of type
_NOISE_RE = re.compile(
    r'\btop\s+\d+\b'
    r'|\bhow[\s-]to\b'
    r'|\btutorial\b'
    r'|\b(?:complete\s+)?guide\s+to\b'
    r'|\bbest\s+practices?\b'
    r'|\bthought\s+leadership\b'
    r'|\bfuture\s+of\b'
    r'|\btrends?\s+(?:in|for|20\d\d)\b'
    r'|\bpredictions?\s+for\b'
    r'|\bwhy\s+(?:you\s+should|your\s+team|companies|organizations)\b'
    r'|\bwhat\s+is\s+(?:a\s+)?[a-z]'
    r'|\bbeginners?\s+guide\b',
    re.I,
)

# ── Tier 1: Named educational series — ALWAYS skip, no judgment needed ────────
# These are recurring training/education programs, never competitive intel.
_SERIES_BLACKLIST_RE = re.compile(
    r'\bnorthstar\b'                        # Snowflake's Northstar education series
    r'|learn[-\s]code[-\s]connect'          # Snowflake's coding training series
    r'|\bbuilders?\s+series\b'              # Developer training (Snowflake, Databricks, etc.)
    r'|\bdata\s+for\s+breakfast\b'          # Snowflake's recurring breakfast event series
    r'|\bdata\s+radicals?\s+podcast\b'      # Alation's podcast
    r'|\bthe\s+observatory\b'               # Monte Carlo's newsletter/series
    r'|\boffice\s+hours\b'                  # "Office hours" recurring
    r'|\bq\s*&\s*a\s+(?:session|webinar)\b' # "Q&A session/webinar"
    r'|\bcertification(?:\s+exam)?\b'       # Certification exams
    r'|\bfundamentals\b'                    # "Snowflake Fundamentals" training
    r'|\bonboarding\b',                     # Customer onboarding events
    re.I,
)

# ── Tier 2: City-stop training — skip when city name is in a how-to/training title ─
# e.g. "Northstar London: Get Started with Snowflake" or "Learn-Code-Connect Nagpur"
# Cities appear in training roadshows but NOT in strategic/product announcements.
_CITY_RE = re.compile(
    r'\b(?:london|paris|tokyo|sydney|singapore|dubai|mumbai|bangalore|bengaluru|'
    r'nagpur|delhi|hyderabad|pune|chennai|amsterdam|berlin|munich|zurich|'
    r'toronto|chicago|austin|seattle|boston|denver|atlanta|dallas|miami)\b',
    re.I,
)

# ── Tier 3: Webinar / workshop hard-exclude ────────────────────────────────────
# A webinar is kept ONLY if _PRODUCT_LAUNCH_STRONG_RE also matches (it's a launch webinar).
# All purely informational webinars are dropped.
_WEBINAR_RE = re.compile(r'\bwebinar\b', re.I)

_WORKSHOP_RE = re.compile(r'\bworkshop\b', re.I)
_LOW_VALUE_WEBINAR_RE = re.compile(
    r'\b(?:training|tutorial|course)\b'
    r'|building\s+(?:a\s+)?(?:[\w-]+\s+){1,5}(?:for|with|using|in)\b'
    r'|getting\s+started\s+with'
    r'|how\s+to\s+(?:use|build|create|implement|deploy)'
    r'|deep\s+dive\s+into'
    r'|hands?[-\s]on\s+(?:lab|session)'
    r'|customer\s+stories?|case\s+stud'
    r'|best\s+practices?\s+(?:for|to|in)\b'
    r'|introduction\s+to\b'
    r'|101\b'                         # "Data Quality 101"
    r'|demo\s+(?:session|day|series)', # generic demo series (not a launch)
    re.I,
)

# ── Tier 4: Strategic value override — these always PASS even if other patterns match ─
# Roundtables/meetups with high-level strategic/market topics are competitive intel.
_STRATEGIC_OVERRIDE_RE = re.compile(
    r'\bai\s+(?:operating\s+system|strategy|roadmap|platform|governance|future)\b'
    r'|\benterprise\s+(?:ai|strategy|data\s+strategy|transformation|architecture)\b'
    r'|\bgo[-\s]to[-\s]market\b|\bgtm\b'
    r'|\bproduct\s+(?:roadmap|vision|direction|strategy)\b'
    r'|\bcompetitive\b|\bmarket\s+(?:shift|trend|direction)\b'
    r'|\bdigital\s+transformation\b'
    r'|\bplatform\s+(?:launch|announcement|strategy)\b',
    re.I,
)

# ══════════════════════════════════════════════════════════════════════════
# CLAUDE API — copied exactly from enrich.py, do not diverge
# ══════════════════════════════════════════════════════════════════════════

def _call_claude(model: str, system: str, user_msg: str, max_tokens: int = 512) -> str:
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
# CLASSIFICATION PROMPT
# ══════════════════════════════════════════════════════════════════════════

CLASSIFY_SYSTEM = """\
You are a competitive intelligence analyst at Actian Corporation — a data integration \
and analytics platform. Actian's core products compete in: data catalog/governance, \
data observability, vector/AI databases, and AI-powered analytics.

Your job: classify and summarize a competitor's blog post, announcement, or event \
to help Actian's product, sales, and marketing teams understand what is happening \
and why it matters.

Return ONLY valid JSON. No commentary. No markdown fences.

Required fields:
- "type": one of exactly: product_launch, event, partnership, funding, open_source_release, blog_post
- "summary": 1 sharp sentence — what happened, timing if relevant, why it matters to Actian
- "actian_relevance": one of exactly: low, medium, high
- "tags": array of 2–5 short relevant tags (e.g. ["AI agents", "enterprise", "GA", "MCP"])
- "source_type": one of exactly: blog, event_page, press_release, github
- "event_date": YYYY-MM-DD string if a specific event date is mentioned, otherwise null

Classification rules:
- type=product_launch: new product, feature GA, major update, new capability
- type=event: conference, summit, webinar, user group, demo day with a specific date
- type=partnership: integration, technology alliance, OEM/reseller deal
- type=funding: investment round, acquisition, strategic investment announcement
- type=open_source_release: GitHub release, open source project launch
- type=blog_post: thought leadership, engineering post, industry commentary (not a launch)
- actian_relevance=high: directly challenges Actian in catalog, observability, vector DB, or data integration
- actian_relevance=medium: adjacent area with indirect competitive signal
- actian_relevance=low: general company news unrelated to Actian's product areas

CRITICAL: The summary must be specific to this company and this content. \
Never write a summary that could apply to any company. \
Generic phrases like "company invests in AI" are not acceptable.\
"""


def classify_item(company: str, title: str, description: str) -> dict | None:
    """
    PHASE 2.5: Anthropic API restricted to verdict_engine only.
    Replace Claude Sonnet classification with rule-based keyword logic.
    """
    # Clean before classification — removes dates, author names, prefixes
    title = clean_text(title)
    description = clean_text(description)

    combined_text = (title + " " + description).lower()

    # ═══ DETERMINE TYPE ═══
    # IMPORTANT: Check strong product launch signals FIRST.
    # An event page can also announce a product launch — explicit launch language
    # must override generic "event" keywords (e.g. "Atlan Activate: GA of Context Studio").
    if _PRODUCT_LAUNCH_STRONG_RE.search(combined_text):
        signal_type = "product_launch"
    elif re.search(r'open.*source|github release|released.*version|version \d+\.\d+', combined_text):
        signal_type = "open_source_release"
    elif re.search(r'\bfunding\b|funding round|series [a-z]|investment|raise\s+\$|announced.*funding', combined_text):
        signal_type = "funding"
    elif re.search(r'partnership|partner|collaborate|integration with|works with|partner\s+with|teaming', combined_text):
        signal_type = "partnership"
    elif re.search(r'\bevent\b|conference|summit|meetup|webinar|forum|expo|announce.*event', combined_text):
        signal_type = "event"
    elif re.search(r'\blaunch\b|announce.*new|introduce|release|released|shipped|beta|available\s+now|what\'s new|new feature|now available|rollout', combined_text):
        # Weaker launch signals (no explicit "product launch" phrase) — still classify
        # as product_launch if not caught by any above bucket
        signal_type = "product_launch"
    else:
        signal_type = "blog_post"

    # ═══ DETERMINE RELEVANCE TO ACTIAN ═══
    high_keywords = r'catalog|observability|vector|embedding|metadata|lineage|governance|data quality|monitoring|quality metrics|analytics|data engineer|machine learning|agentic'
    medium_keywords = r'data|analytics|integration|platform|ai|ml|artificial|python|java|sql|database|semantic'

    if re.search(high_keywords, combined_text):
        relevance = "high"
    elif re.search(medium_keywords, combined_text):
        relevance = "medium"
    else:
        relevance = "low"

    # ═══ EXTRACT TAGS ═══
    tags = []
    if re.search(r'\benterprise\b|enterprise.grade|enterprise.scale', combined_text):
        tags.append("enterprise")
    if re.search(r'\bga\b|general availability|available now', combined_text):
        tags.append("GA")
    if re.search(r'beta|preview|early access', combined_text):
        tags.append("beta")
    if re.search(r'pricing|price increase|cost', combined_text):
        tags.append("pricing")
    if re.search(r'vector|embedding|semantic search', combined_text):
        tags.append("vector")
    if re.search(r'\bai\b|artificial intelligence|machine learning|\bml\b', combined_text):
        tags.append("AI")
    if re.search(r'api|sdk|library', combined_text):
        tags.append("API")

    # ═══ DETERMINE SOURCE TYPE ═══
    if re.search(r'github|repo|git', combined_text):
        source_type = "github"
    elif re.search(r'press release|press kit', combined_text):
        source_type = "press_release"
    elif signal_type == "event":
        source_type = "event_page"
    else:
        source_type = "blog"

    # ═══ BUILD SUMMARY ═══
    # Use description if available, else title
    if description and len(description) > 20:
        summary = description[:200].strip()
        if len(description) > 200:
            summary += "..."
    else:
        summary = title

    # ═══ PARSE EVENT DATE ═══
    # Look for date patterns in description (simple heuristic)
    event_date = None
    date_pattern = r'(\d{4})-(\d{2})-(\d{2})|([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})'
    date_match = re.search(date_pattern, description)
    if date_match:
        event_date = date_match.group(0)

    # ═══ RELEVANCE GUARANTEE ═══
    # Product launches are never "low" — if we classified it as a launch it has signal
    if signal_type == "product_launch" and relevance == "low":
        relevance = "medium"
    # Explicit GA / general availability → always high
    if re.search(r'\bga\b|general\s+availability', combined_text) and relevance != "high":
        relevance = "high"

    return {
        "type": signal_type,
        "summary": summary,
        "actian_relevance": relevance,
        "tags": tags,
        "source_type": source_type,
        "event_date": event_date,
    }

# ══════════════════════════════════════════════════════════════════════════
# DEDUPLICATION — file-based, same principle as seen_jobs.db
# ══════════════════════════════════════════════════════════════════════════

def load_seen_urls() -> set:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text()))
        except Exception:
            return set()
    return set()


def save_seen_urls(seen: set) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(seen), indent=2))

# ══════════════════════════════════════════════════════════════════════════
# RSS PARSING
# ══════════════════════════════════════════════════════════════════════════

def _parse_date(entry) -> str:
    """Extract published date from a feedparser entry, normalized to YYYY-MM-DD."""
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                return date(*val[:3]).isoformat()
            except Exception:
                pass
    return date.today().isoformat()


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def fetch_rss(company: str, url: str) -> list[dict]:
    """Fetch and parse an RSS feed. Returns list of raw item dicts.

    Uses httpx to fetch raw content first (handles redirects, encodings, and
    Content-Type issues that cause feedparser to fail when fetching directly).
    """
    try:
        r = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; feedparser/6.0; +https://github.com/kurtmckee/feedparser)"},
            follow_redirects=True,
            timeout=30,
        )
        r.raise_for_status()

        # Pass raw content to feedparser — avoids XML strictness issues
        feed = feedparser.parse(r.content)

        if not feed.entries:
            if feed.bozo:
                print(f"  [WARN] {company}: RSS parse error — {getattr(feed, 'bozo_exception', 'unknown')}")
            else:
                print(f"  [INFO] {company}: Feed returned 0 entries")
            return []

        items = []
        for entry in feed.entries[:MAX_ITEMS_PER_FEED]:
            items.append({
                "title":          entry.get("title", "").strip(),
                "url":            entry.get("link",  "").strip(),
                "published_date": _parse_date(entry),
                "description":    _strip_html(
                    entry.get("summary", "") or entry.get("description", "")
                )[:1000],
            })
        return items
    except httpx.HTTPStatusError as e:
        print(f"  [WARN] {company}: HTTP {e.response.status_code} from {url}")
        return []
    except Exception as e:
        print(f"  [WARN] {company}: Failed to fetch RSS from {url} — {e}")
        return []

# ══════════════════════════════════════════════════════════════════════════
# HTML FALLBACK — for companies without RSS feeds
# ══════════════════════════════════════════════════════════════════════════

# Patterns that indicate a nav/footer link, not a real blog post or event
_NAV_RE = re.compile(
    r"^(home|blog|resources|about|pricing|contact|careers|login|sign\s+in|"
    r"get\s+started|read\s+more|learn\s+more|view\s+all|see\s+all|"
    r"subscribe|newsletter|all\s+posts|back\s+to|"
    r"explore\s+\w+.*now|request\s+a?\s+demo|watch\s+(?:a\s+)?demo|"
    r"try\s+free|start\s+free|book\s+a?\s+demo|get\s+a?\s+demo|"
    r"documentation|support|community|partner\s+finder|resource\s+library|"
    r"training\s+overview|certifications?|live\s+demos?|hands.on\s+labs?"
    r"|fundamentals|analyst\s+reports?|trust\s+center|startup\s+program"
    r"|customer\s+stories|events?\s+&\s+webinars|the\s+observatory"
    r"|data\s+radicals\s+podcast|data\s+culture\s+maturity|"
    r"alation\s+help\s+center|alation\s+community|alation\s+pricing)$",
    re.I,
)

# Bigeye-specific noise: generic recurring product demo webinars have no intelligence value
_BIGEYE_NOISE_RE = re.compile(
    r"live\s+monthly\s+product\s+demo|product\s+demo\s+webinar|"
    r"register\s+for\s+our\s+live",
    re.I,
)


def fetch_html_blog(company: str, page_url: str, url_pattern: str) -> list[dict]:
    """Scrape a blog/newsroom listing page and return candidate items.

    Extracts <a> tags whose:
    - href contains url_pattern (stays on the company's blog)
    - anchor text is a plausible article title (20–200 chars, not a nav label)
    """
    try:
        r = httpx.get(
            page_url,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  [WARN] {company}: HTML fetch failed — {e}")
        return []

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r.text, "html.parser")

    seen_hrefs: set[str] = set()
    items: list[dict] = []

    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()
        title: str = a.get_text(separator=" ", strip=True)

        # Normalize relative URLs
        if href.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(page_url)
            href = f"{parsed.scheme}://{parsed.netloc}{href}"

        # Must contain the expected blog path pattern
        if url_pattern not in href:
            continue

        # Skip already-seen hrefs and obvious nav labels
        if href in seen_hrefs:
            continue
        if not (20 <= len(title) <= 200):
            continue
        if _NAV_RE.match(title):
            continue

        seen_hrefs.add(href)
        items.append({
            "title":          title[:200],
            "url":            href,
            "published_date": date.today().isoformat(),  # No date available from listing
            "description":    "",
        })

        if len(items) >= MAX_ITEMS_PER_FEED:
            break

    return items


# ══════════════════════════════════════════════════════════════════════════
# ROLLING WINDOW
# ══════════════════════════════════════════════════════════════════════════

def within_window(published_date_str: str) -> bool:
    """Return True if the date is within MAX_SIGNAL_AGE_DAYS of today."""
    try:
        published = date.fromisoformat(published_date_str)
        return (date.today() - published).days <= MAX_SIGNAL_AGE_DAYS
    except Exception:
        return True  # Keep items with unparseable dates (assume recent)

# ══════════════════════════════════════════════════════════════════════════
# EVENT PAGE SCRAPER
# ══════════════════════════════════════════════════════════════════════════

_MONTH_MAP = {
    "jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
    "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12",
}

_DATE_PATTERNS = [
    re.compile(r'\b(202\d)[-/](\d{1,2})[-/](\d{1,2})\b'),
    re.compile(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2}),?\s+(202\d)\b', re.I),
    re.compile(r'\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?,?\s+(202\d)\b', re.I),
]
# Year-less date patterns — e.g. "22 APR", "29 APRIL", "Apr 22"
# Year is inferred as current year, or next year if date is already past
_DATE_PATTERNS_NO_YEAR = [
    re.compile(r'\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b', re.I),
    re.compile(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?!\s*,?\s*\d{4})\b', re.I),
]

_EVENT_KW = re.compile(
    r'webinar|summit|conference|workshop|demo\s+day|session|meetup|virtual\s+event|'
    r'live\s+event|user\s+conference|world\s+tour|data\s+summit|community\s+event|'
    r'office\s+hours|register\s+now|join\s+us|save\s+the\s+date|upcoming\s+event',
    re.I,
)


def _parse_event_date(text: str) -> str | None:
    """Extract and normalize the first future-ish date from text.

    Handles both year-explicit formats ("Apr 22, 2026") and year-less formats
    like "22 APR" or "29 APRIL" used by Atlan and other event pages.
    Year is inferred as current year; rolls to next year if date is already past.
    """
    today = date.today()

    # ── Try patterns WITH explicit year first ──────────────────────────────
    for pat in _DATE_PATTERNS:
        for m in pat.finditer(text):
            raw = m.group(0)
            try:
                mi = re.match(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', raw)
                if mi:
                    d = date(int(mi.group(1)), int(mi.group(2)), int(mi.group(3)))
                else:
                    ma = re.match(r'(\w{3})[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})', raw, re.I)
                    mb = re.match(r'(\d{1,2})\s+(\w{3})[a-z]*\.?,?\s+(\d{4})', raw, re.I)
                    if ma:
                        mo = _MONTH_MAP.get(ma.group(1).lower()[:3])
                        d = date(int(ma.group(3)), int(mo), int(ma.group(2)))
                    elif mb:
                        mo = _MONTH_MAP.get(mb.group(2).lower()[:3])
                        d = date(int(mb.group(3)), int(mo), int(mb.group(1)))
                    else:
                        continue
                if (d - today).days >= -30:
                    return d.isoformat()
            except Exception:
                continue

    # ── Fallback: year-less patterns (e.g. "22 APR", "Apr 22") ────────────
    for pat in _DATE_PATTERNS_NO_YEAR:
        for m in pat.finditer(text):
            raw = m.group(0)
            try:
                ma = re.match(r'(\d{1,2})\s+(\w{3})', raw, re.I)
                mb = re.match(r'(\w{3})[a-z]*\.?\s+(\d{1,2})', raw, re.I)
                if ma:
                    day, mo_str = int(ma.group(1)), ma.group(2).lower()[:3]
                elif mb:
                    day, mo_str = int(mb.group(2)), mb.group(1).lower()[:3]
                else:
                    continue
                mo = _MONTH_MAP.get(mo_str)
                if not mo or not (1 <= day <= 31):
                    continue
                # Try current year first; roll to next year if the date is past
                for yr in [today.year, today.year + 1]:
                    try:
                        d = date(yr, int(mo), day)
                        if (d - today).days >= -30:
                            return d.isoformat()
                        break  # Date exists but is too far in the past — don't try next year
                    except ValueError:
                        continue
            except Exception:
                continue

    return None


def fetch_event_page(company: str, url: str) -> list[dict]:
    """
    Scrape a company event listing page.
    Strategy A: find <a> tags with event keywords + nearby dates.
    Strategy B: text-block scan for event names near dates (JS-rendered fallback).
    """
    from urllib.parse import urlparse
    from bs4 import BeautifulSoup

    try:
        r = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            follow_redirects=True,
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  [WARN] {company}: event page fetch failed — {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    base = urlparse(url)
    items: list[dict] = []
    seen: set[str] = set()

    # ── Strategy A: link-based extraction ────────────────────────────────
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        title = a.get_text(separator=" ", strip=True)

        # Normalize URL
        if href.startswith("/"):
            href = f"{base.scheme}://{base.netloc}{href}"
        elif not href.startswith("http"):
            continue

        if not (12 <= len(title) <= 160) or _NAV_RE.match(title) or title in seen:
            continue

        # Gather context (walk up 4 parent levels)
        ctx = ""
        node = a.parent
        for _ in range(4):
            if node:
                ctx = node.get_text(separator=" ", strip=True)[:600]
                node = node.parent
            if len(ctx) > 80:
                break

        if not _EVENT_KW.search(title + " " + ctx[:300]):
            continue

        # Check title first — it has the most specific date (e.g. "May 5, 2026")
        event_date = _parse_event_date(title) or _parse_event_date(ctx)
        seen.add(title)
        items.append({
            "title": title[:150],
            "url": href,
            "published_date": date.today().isoformat(),
            "description": ctx[:400].strip(),
            "event_date": event_date,
        })
        if len(items) >= 12:
            break

    # ── Strategy B: text scan (catches JS-rendered pages with SSR text) ──
    if not items:
        lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
        for i, line in enumerate(lines):
            if not (15 <= len(line) <= 150) or line in seen:
                continue
            ctx = " ".join(lines[max(0, i-3):i+4])
            if not _EVENT_KW.search(ctx):
                continue
            event_date = _parse_event_date(ctx)
            if not event_date:
                continue
            seen.add(line)
            items.append({
                "title": line[:150],
                "url": url,   # Falls back to listing page URL
                "published_date": date.today().isoformat(),
                "description": ctx[:400],
                "event_date": event_date,
            })
            if len(items) >= 8:
                break

    return items


# ══════════════════════════════════════════════════════════════════════════
# PLAYWRIGHT EVENT PAGE SCRAPER — for JS-rendered event pages
# Used for: Atlan, Acceldata, Alation, Bigeye (React/Next.js pages)
# Falls back to httpx-based fetch_event_page() on Playwright failure.
# ══════════════════════════════════════════════════════════════════════════

def _slug_to_title(slug: str) -> str:
    """Convert a URL slug to a readable title.

    '/event/google-cloud-next/' → 'Google Cloud Next'
    '/events/evanta-cdao---new-york' → 'Evanta CDAO - New York'
    """
    # Strip path prefix and trailing slash
    slug = re.sub(r'^/events?/', '', slug.strip('/'))
    # Replace triple dash (separator) with ` - `
    slug = re.sub(r'-{2,}', ' - ', slug)
    # Replace remaining dashes with spaces
    slug = slug.replace('-', ' ')
    # Title-case, preserve known acronyms
    words = []
    for w in slug.split():
        words.append(w.upper() if w.lower() in ('cdao', 'cio', 'ai', 'emea', 'apac') else w.capitalize())
    return ' '.join(words)


def _heading_from_parents(a_tag) -> str | None:
    """Walk up parent DOM tree to find the nearest heading that looks like an event title."""
    node = a_tag.parent
    for _ in range(6):
        if node is None:
            break
        for heading_tag in node.find_all(['h1', 'h2', 'h3', 'h4'], limit=5):
            text = heading_tag.get_text(separator=' ', strip=True)
            if 20 <= len(text) <= 160 and not _NAV_RE.match(text):
                return text
        node = node.parent
    return None


# Generic CTA link texts — when a link has this text, use the parent heading instead
_CTA_LINK_RE = re.compile(
    r'^(view\s+details?|details?\s*[→»]?|book\s+a?\s+demo|see\s+product\s+tour|'
    r'register\s+now|learn\s+more|save\s+your\s+spot|join\s+now|rsvp|'
    r'sign\s+up|get\s+tickets?|watch\s+replay|watch\s+on\s+demand)\s*[→»]?$',
    re.I,
)

# Bigeye product-page nav items that bleed onto events page sidebar
_BIGEYE_SIDEBAR_RE = re.compile(
    r'^(anomaly\s+detection|monitoring\s+as\s+code|integrations?|'
    r'data\s+engineer|data\s+governance|data\s+architect|data\s+analyst|'
    r'data\s+executive|developer\s+tools?|all\s+systems\s+operational|'
    r'terms\s+of\s+use|privacy\s+policy|status\s+page)$',
    re.I,
)


def fetch_event_page_playwright(company: str, url: str) -> list[dict]:
    """Playwright-based event page scraper for JS-rendered sites.

    Strategy A+: For event-URL links (Atlan /event/slug, Acceldata /events/slug),
                 extract real title from parent heading or slug, date from context.
    Strategy A:  Standard link scan — skip CTA text, use parent heading instead.
    Strategy B:  Text-block scan fallback for pages without clean link structure.

    Falls back to httpx-based fetch_event_page() on Playwright failure.
    """
    from bs4 import BeautifulSoup
    from urllib.parse import urlparse

    html = ""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = ctx.new_page()
            # Use load (not networkidle) — Atlan's page never reaches networkidle
            page.goto(url, wait_until="load", timeout=45000)
            page.wait_for_timeout(4000)  # Extra settle time for React hydration
            html = page.content()
            browser.close()
    except Exception as e:
        print(f"  [WARN] {company}: Playwright failed ({e}) — falling back to httpx")
        return fetch_event_page(company, url)

    if not html:
        return fetch_event_page(company, url)

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    base = urlparse(url)
    listing_path = base.path.rstrip("/")
    items: list[dict] = []
    seen_titles: set[str] = set()
    seen_hrefs: set[str] = set()

    # ── Strategy Zilliz: structured upcoming-event card extraction ───────
    # zilliz.com/event renders a clean "Upcoming Events" section where each
    # card is a <li><a href="external"> containing two headings:
    #   heading[0] = event type ("In-Person Event", "Meet Up", "Webinar")
    #   heading[1] = actual event name
    # plus a text node "MMM DD, YYYY HH:MM Pacific" for the date.
    # We target only the Upcoming Events section — ignore Past Events.
    if company == "Milvus":
        upcoming_heading = None
        for h in soup.find_all(["h2", "h3", "h4"]):
            if re.search(r'upcoming\s+events?', h.get_text(), re.I):
                upcoming_heading = h
                break

        if upcoming_heading:
            ul = upcoming_heading.find_next("ul")
            if ul:
                for li in ul.find_all("li", recursive=False):
                    a = li.find("a", href=True)
                    if not a:
                        continue
                    href = a["href"].strip()
                    if not href.startswith("http"):
                        continue
                    if href in seen_hrefs:
                        continue

                    # First heading = type ("In-Person Event"), second = event name
                    headings = a.find_all(["h1","h2","h3","h4","h5"])
                    if len(headings) >= 2:
                        event_name = headings[1].get_text(strip=True)
                    elif headings:
                        event_name = headings[0].get_text(strip=True)
                    else:
                        continue

                    if not event_name or len(event_name) < 5 or event_name in seen_titles:
                        continue
                    # Skip generic type-only names
                    if re.match(r'^(in-person event|virtual event|webinar|meet up|hybrid event|training)$', event_name, re.I):
                        continue

                    ctx = li.get_text(separator=" ", strip=True)[:500]
                    event_date = _parse_event_date(ctx)

                    seen_titles.add(event_name)
                    seen_hrefs.add(href)
                    items.append({
                        "title":          event_name[:150],
                        "url":            href,
                        "published_date": date.today().isoformat(),
                        "description":    ctx[:400].strip(),
                        "event_date":     event_date,
                    })

        print(f"  [Milvus/Zilliz] Strategy Zilliz: {len(items)} upcoming events found")
        return items  # Skip generic A+/A/B for Milvus

    # ── Strategy Snowflake: article card extraction ──────────────────────
    # snowflake.com/en/events/all-events renders a clean grid of article cards.
    # Each card is: <link><article>...<heading>Title</heading>...<generic>Month</generic>...</article></link>
    # The parent <link> (actually <a>) has the href to the event page.
    # We extract title from the <heading> and date from context text.
    if company == "Snowflake":
        # Snowflake uses article tags inside a links — find all articles on the page
        for article in soup.find_all("article"):
            # The parent link gives us the URL
            parent_link = article.find_parent("a", href=True)
            if not parent_link:
                continue
            href = parent_link.get("href", "").strip()
            if not href:
                continue
            # Convert relative URLs to absolute
            if not href.startswith("http"):
                if href.startswith("/"):
                    href = f"{base.scheme}://{base.netloc}{href}"
                else:
                    continue
            if href in seen_hrefs:
                continue

            # Extract title from the first heading in the article
            heading = article.find(["h2", "h3", "h4", "h5", "h6"])
            if not heading:
                continue
            title = heading.get_text(strip=True)
            if not title or len(title) < 8 or title in seen_titles:
                continue

            # Extract date from the article context (month abbreviation + day)
            ctx = article.get_text(separator=" ", strip=True)[:600]
            event_date = _parse_event_date(ctx)

            seen_titles.add(title)
            seen_hrefs.add(href)
            items.append({
                "title":          title[:150],
                "url":            href,
                "published_date": date.today().isoformat(),
                "description":    ctx[:400].strip(),
                "event_date":     event_date,
            })

        print(f"  [Snowflake] Strategy Snowflake: {len(items)} events found")
        if items:
            return items  # Skip generic A+/A/B for Snowflake if we found events

    # ── Strategy A+: event-slug link scan ───────────────────────────────
    # For Atlan (/event/slug) and Acceldata (/events/slug) — the event cards
    # link to individual event pages but the link TEXT is a generic CTA.
    # We extract the real title from the parent heading or derive from the slug.
    event_slug_re = re.compile(r'^/events?/[^/]+/?$', re.I)

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("/"):
            continue
        if not event_slug_re.match(href):
            continue
        if href.rstrip("/") == listing_path:
            continue
        full_href = f"{base.scheme}://{base.netloc}{href}"
        if full_href in seen_hrefs:
            continue

        # Walk up parents for context (date, description)
        ctx = ""
        node = a.parent
        for _ in range(6):
            if node:
                ctx = node.get_text(separator=" ", strip=True)[:800]
                node = node.parent
            if len(ctx) > 60:
                break

        event_date = _parse_event_date(ctx)

        # Title: prefer parent heading over slug
        title = _heading_from_parents(a) or _slug_to_title(href)
        if not title or len(title) < 8 or title in seen_titles:
            continue
        if company == "Bigeye" and (_BIGEYE_NOISE_RE.search(title) or _BIGEYE_SIDEBAR_RE.match(title)):
            continue

        # Skip events marked "Concluded" — these are past events on Atlan's page
        if re.search(r'\bConcluded\b', ctx, re.I):
            continue

        seen_titles.add(title)
        seen_hrefs.add(full_href)
        items.append({
            "title":          title[:150],
            "url":            full_href,
            "published_date": date.today().isoformat(),
            "description":    ctx[:400].strip(),
            "event_date":     event_date,
        })
        if len(items) >= 20:
            break

    # ── Strategy A: standard link-based extraction ───────────────────────
    # Always runs after A+ (not guarded by `not items`) so that events at
    # non-slug URLs (e.g. Atlan /activate/, /live-demo-series-*) are also
    # captured. seen_hrefs prevents duplicates with A+.
    if True:  # always run — Strategy A complements A+, not replaces it
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            link_text = a.get_text(separator=" ", strip=True)

            if href.startswith("/"):
                href = f"{base.scheme}://{base.netloc}{href}"
            elif not href.startswith("http"):
                continue
            if href in seen_hrefs:
                continue

            # If link text is a generic CTA, extract real title from parent heading.
            # CTA-linked cards are trusted events on an events page — skip _EVENT_KW.
            is_cta_link = bool(_CTA_LINK_RE.match(link_text))
            if is_cta_link:
                title = _heading_from_parents(a)
                if not title:
                    continue
            else:
                title = link_text
                if not (15 <= len(title) <= 200):
                    continue
                if _NAV_RE.match(title):
                    continue

            if title in seen_titles:
                continue
            if company == "Bigeye" and (_BIGEYE_NOISE_RE.search(title) or _BIGEYE_SIDEBAR_RE.match(title)):
                continue

            # Walk up parents for context
            ctx = ""
            node = a.parent
            for _ in range(5):
                if node:
                    ctx = node.get_text(separator=" ", strip=True)[:800]
                    node = node.parent
                if len(ctx) > 100:
                    break

            combined = title + " " + ctx[:400]
            # CTA-linked cards are on the events page by definition — trust them.
            # For descriptive-text links, require an event keyword in context.
            if not is_cta_link and not _EVENT_KW.search(combined):
                continue

            # Skip concluded/past events
            if re.search(r'\bConcluded\b', ctx, re.I):
                continue

            event_date = _parse_event_date(title) or _parse_event_date(ctx)
            seen_titles.add(title)
            seen_hrefs.add(href)
            items.append({
                "title":          title[:150],
                "url":            href,
                "published_date": date.today().isoformat(),
                "description":    ctx[:400].strip(),
                "event_date":     event_date,
            })
            if len(items) >= 15:
                break

    # ── Strategy B: text-block scan fallback ────────────────────────────
    if not items:
        lines = [l.strip() for l in soup.get_text(separator="\n").split("\n") if l.strip()]
        for i, line in enumerate(lines):
            if not (15 <= len(line) <= 200) or line in seen_titles:
                continue
            if company == "Bigeye" and (_BIGEYE_NOISE_RE.search(line) or _BIGEYE_SIDEBAR_RE.match(line)):
                continue
            ctx = " ".join(lines[max(0, i - 3):i + 4])
            if not _EVENT_KW.search(ctx):
                continue
            event_date = _parse_event_date(ctx)
            seen_titles.add(line)
            items.append({
                "title":          line[:150],
                "url":            url,
                "published_date": date.today().isoformat(),
                "description":    ctx[:400],
                "event_date":     event_date,
            })
            if len(items) >= 12:
                break

    return items


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    # PHASE 2.5: Anthropic API restricted to verdict_engine only
    # Using rule-based classification (no API key needed)
    seen_urls = load_seen_urls()

    # Load existing signals, enforce rolling window
    existing: list[dict] = []
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text())
            before = len(existing)
            existing = [s for s in existing if within_window(s.get("published_date", ""))]
            dropped = before - len(existing)
            if dropped:
                print(f"[INFO] Dropped {dropped} signals outside 90-day window")
        except Exception:
            existing = []

    # Index existing signals by URL for fast dedup
    existing_urls = {s["url"] for s in existing}
    seen_urls.update(existing_urls)

    new_signals: list[dict] = []
    today_str = date.today().isoformat()

    # Build unified source list: RSS first, then HTML fallback
    all_sources: list[tuple[str, str, str]] = []  # (company, source_type, url_or_page)
    for company, feed_url in RSS_FEEDS.items():
        all_sources.append((company, "rss", feed_url))
    for company, (page_url, pattern) in HTML_SOURCES.items():
        all_sources.append((company, "html", f"{page_url}||{pattern}"))

    for company, source_type, source_info in all_sources:
        product_area = V2_PRODUCT_AREA_MAP.get(company)
        if not product_area:
            continue

        if source_type == "rss":
            print(f"\n[{company}] RSS: {source_info}")
            items = fetch_rss(company, source_info)
        else:
            page_url, pattern = source_info.split("||", 1)
            print(f"\n[{company}] HTML: {page_url}")
            items = fetch_html_blog(company, page_url, pattern)

        if not items:
            print(f"  No items returned")
            continue

        print(f"  {len(items)} items fetched")
        processed = 0

        for item in items:
            url   = item["url"]
            title = item["title"]
            pub   = item["published_date"]

            # Skip already seen
            if url in seen_urls:
                continue

            # Hard URL block — product pages, docs, resources, careers, etc.
            if is_blocked_url(url):
                seen_urls.add(url)
                continue

            # Skip items outside rolling window — mark seen so we skip next run too
            if not within_window(pub):
                seen_urls.add(url)
                continue

            # Skip items with no title or URL
            if not title or not url:
                seen_urls.add(url)
                continue

            # Classify via rule-based logic (PHASE 2.5: Anthropic disabled)
            print(f"  → {title[:75]}")
            classification = classify_item(company, title, item["description"])

            if not classification:
                print(f"    [WARN] Classification failed — skipping")
                seen_urls.add(url)
                continue

            # Hard exclude: noise content patterns regardless of type
            gate_text = title + " " + item["description"][:300] + " " + url
            if _NOISE_RE.search(gate_text):
                seen_urls.add(url)
                continue

            # ── Event quality gate — 4-tier filter ─────────────────────────────
            if classification["type"] != "product_launch":
                # Tier 1: Named educational series — always skip
                if _SERIES_BLACKLIST_RE.search(gate_text):
                    seen_urls.add(url)
                    continue
                # Tier 2: City-stop training — skip if city + training keywords present
                if _CITY_RE.search(gate_text) and _LOW_VALUE_WEBINAR_RE.search(gate_text):
                    seen_urls.add(url)
                    continue
                # Tier 3: Generic noise — skip workshops/training UNLESS strategic override
                if (_WORKSHOP_RE.search(gate_text) or _LOW_VALUE_WEBINAR_RE.search(gate_text)):
                    if not _STRATEGIC_OVERRIDE_RE.search(gate_text):
                        seen_urls.add(url)
                        continue
                # Tier 4: Webinar hard gate — drop ALL webinars unless they are
                # explicitly a product launch (launch webinar is fine, info webinar is not)
                if _WEBINAR_RE.search(gate_text):
                    if not _PRODUCT_LAUNCH_STRONG_RE.search(gate_text):
                        seen_urls.add(url)
                        continue

            # Event gating: blog posts must contain an explicit event keyword to pass
            # All other types (product_launch, partnership, etc.) already required
            # event-like keywords during type classification, so they pass through
            if classification["type"] == "blog_post":
                if not _EVENT_GATE_RE.search(gate_text):
                    seen_urls.add(url)
                    continue

            signal = {
                "company":          company,
                "product_area":     product_area,
                "type":             classification["type"],
                "title":            clean_text(title),
                "url":              url,
                "published_date":   pub,
                "summary":          clean_text(classification.get("summary", "")),
                "actian_relevance": classification["actian_relevance"],
                "tags":             classification.get("tags", []),
                "source_type":      classification.get("source_type", "blog"),
                "event_date":       classification.get("event_date"),
                "scraped_at":       today_str,
            }

            new_signals.append(signal)
            seen_urls.add(url)
            processed += 1
            time.sleep(0.5)  # Rate limit between Sonnet calls

        print(f"  ✓ {processed} new signal(s) added")

    # ── Event page scraping ──────────────────────────────────────────────
    print("\n── Event page scraping ─────────────────────────────────────────")
    for company, event_url in EVENT_URLS.items():
        product_area = V2_PRODUCT_AREA_MAP.get(company)
        if not product_area:
            continue

        print(f"\n[{company}] {'[Playwright]' if company in PLAYWRIGHT_EVENT_PAGES else '[httpx]'} {event_url}")
        if company in PLAYWRIGHT_EVENT_PAGES:
            event_items = fetch_event_page_playwright(company, event_url)
        else:
            event_items = fetch_event_page(company, event_url)

        if not event_items:
            print(f"  No events found")
            continue

        print(f"  {len(event_items)} events found on page")
        added = 0

        for item in event_items:
            item_url = item["url"]
            title = item["title"]

            if item_url in seen_urls:
                continue
            if not title:
                continue

            # Hard URL block — product pages, docs, resources, etc. (all companies)
            if is_blocked_url(item_url):
                seen_urls.add(item_url)
                continue

            # Classify — but check for explicit product launch language first.
            # An event page item can be type=product_launch if the title/description
            # explicitly says "product launch", "GA", "generally available", etc.
            # Otherwise default to type=event (it came from an event listing page).
            classification = classify_item(company, title, item["description"])
            combined_check = (title + " " + item.get("description", "")).lower()
            if _PRODUCT_LAUNCH_STRONG_RE.search(combined_check):
                classification["type"] = "product_launch"
            else:
                classification["type"] = "event"
            classification["source_type"] = "event_page"
            if item.get("event_date"):
                classification["event_date"] = item["event_date"]

            # ── Event quality gate — 4-tier filter (event page items) ────────────
            if classification["type"] != "product_launch":
                combined = title + " " + item.get("description", "") + " " + item_url
                # Tier 1: Named educational series — always skip
                if _SERIES_BLACKLIST_RE.search(combined):
                    seen_urls.add(item_url)
                    continue
                # Tier 2: City-stop training — skip if city + training keywords
                if _CITY_RE.search(combined) and _LOW_VALUE_WEBINAR_RE.search(combined):
                    seen_urls.add(item_url)
                    continue
                # Tier 3: Generic noise — skip UNLESS strategic override passes
                if _WORKSHOP_RE.search(combined) or _LOW_VALUE_WEBINAR_RE.search(combined):
                    if not _STRATEGIC_OVERRIDE_RE.search(combined):
                        seen_urls.add(item_url)
                        continue
                # Tier 4: Webinars — drop unless explicitly a product launch webinar
                if _WEBINAR_RE.search(combined):
                    if not _PRODUCT_LAUNCH_STRONG_RE.search(combined):
                        seen_urls.add(item_url)
                        continue

            signal = {
                "company":          company,
                "product_area":     product_area,
                "type":             classification["type"],
                "title":            clean_text(title),
                "url":              item_url,
                "published_date":   item["published_date"],
                "summary":          clean_text(classification.get("summary", title)),
                "actian_relevance": classification.get("actian_relevance", "medium"),
                "tags":             classification.get("tags", []),
                "source_type":      "event_page",
                "event_date":       item.get("event_date"),
                "scraped_at":       today_str,
            }

            new_signals.append(signal)
            seen_urls.add(item_url)
            added += 1
            type_label = "🚀 LAUNCH" if classification["type"] == "product_launch" else "📅 event"
            print(f"  ✓ [{type_label}] {title[:55]} | {item.get('event_date', 'no date')}")

        if added == 0:
            print(f"  (all already seen)")

    # Merge new with existing, re-enforce rolling window, sort newest first
    all_signals = existing + new_signals
    all_signals = [s for s in all_signals if within_window(s.get("published_date", ""))]
    all_signals.sort(key=lambda s: s.get("published_date", ""), reverse=True)

    OUTPUT_FILE.write_text(json.dumps(all_signals, indent=2))
    save_seen_urls(seen_urls)

    print(f"\n{'─'*60}")
    print(f"✓ competitive_signals.json: {len(all_signals)} total ({len(new_signals)} new)")
    print(f"✓ seen_signals.json:        {len(seen_urls)} URLs tracked")


if __name__ == "__main__":
    main()
