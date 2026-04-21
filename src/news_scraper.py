"""
news_scraper.py — Press Release & Newsroom Intelligence Scraper
────────────────────────────────────────────────────────────────
• Scrapes company newsroom/press release pages (6 companies, Phase 1)
• Classifies items using rule-based patterns (funding, leadership, product, pricing, expansion, award, partnership)
• Deduplicates by URL (file-based: data/seen_news.json)
• Rolling 60-day window — drops stale news automatically
• Hard filters noise: customer stories, tutorials, blog posts, thought leadership
• Phase 2 (later): Add Haiku summaries if needed; Phase 3: integrate with verdicts
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

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

MAX_NEWS_AGE_DAYS = 60  # Rolling 60-day window
MAX_ITEMS_PER_COMPANY = 30  # Cap per company to avoid flooding

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT_FILE = DATA_DIR / "news.json"
SEEN_FILE = DATA_DIR / "seen_news.json"

# 6 companies Phase 1
V2_PRODUCT_AREA_MAP = {
    "Bigeye": "Data Observability",
    "Atlan": "Data Intelligence",
    "Qdrant": "VectorAI",
    "Collibra": "Data Intelligence",
    "Alation": "Data Intelligence",
    "Pinecone": "VectorAI",
    # Phase 2 additions
    "Monte Carlo": "Data Observability",
    "Acceldata": "Data Observability",
    "Milvus": "VectorAI",
    "Snowflake": "AI Analyst",
    "Databricks": "AI Analyst",
}

# Newsroom URLs — user-provided, verified
# Values can be a single URL string or a list of URLs for companies with multiple sections
NEWSROOM_URLS = {
    "Bigeye": "https://www.bigeye.com/newsroom",
    "Atlan": "https://atlan.com/newsroom/",
    "Qdrant": "https://qdrant.tech/blog/",
    "Collibra": "https://www.collibra.com/company/newsroom/press-releases",
    "Alation": "https://www.alation.com/news-and-press/",
    "Pinecone": "https://www.pinecone.io/newsroom/",
    # Phase 2 additions
    "Monte Carlo": "https://www.montecarlodata.com/category/announcements/",
    "Acceldata": "https://www.acceldata.io/newsroom",
    "Milvus": "https://zilliz.com/news",
    "Snowflake": [
        "https://www.snowflake.com/en/news/press-releases/",
        "https://www.snowflake.com/en/news/news-coverage/",
    ],
    "Databricks": [
        "https://www.databricks.com/company/newsroom/press-releases",
        "https://www.databricks.com/company/newsroom/media-coverage",
    ],
}

today_str = date.today().isoformat()

# ══════════════════════════════════════════════════════════════════════════
# STRICT CLASSIFICATION — only 6 signal categories kept
# ══════════════════════════════════════════════════════════════════════════
# KEEP ONLY: product_launch, feature, funding, leadership, partnership, acquisition
# (layoff category defined too but rarely appears in company newsrooms)
#
# DROP EVERYTHING ELSE: awards/Gartner/Forrester, surveys, generic expansion,
# pricing tweaks, thought-leadership, "AI will transform X", opinion pieces,
# webinars, tutorials, customer stories, interviews, podcasts.

# --- KEEP patterns --------------------------------------------------------
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
_LEADERSHIP_RE = re.compile(
    r'\bappoints?\b'
    r'|\bnames?\s+(?:new\s+)?(?:CEO|CTO|CFO|CRO|CPO|COO|CMO|Chief|President|SVP|VP)'
    r'|\bjoins?\s+as\s+(?:new\s+)?(?:CEO|CTO|CFO|CRO|CPO|COO|CMO|Chief|President|SVP|VP)'
    r'|\bhires?\s+(?:new\s+)?(?:CEO|CTO|CFO|CRO|CPO|COO|CMO|Chief|President|SVP|VP)'
    r'|\bnew\s+(?:CEO|CTO|CFO|CRO|CPO|COO|CMO|Chief)\b'
    r'|\bsteps?\s+down\s+as\b'
    r'|\bdeparts?\s+as\b',
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
# Product launches = an actual shipped product/platform. GA. Public preview.
# Intentionally broad on "launches/announces/introduces/unveils" — the _DROP_RE
# handles generic content before we get here, so anything reaching this point
# that says "launches X" is likely a real product announcement.
_PRODUCT_LAUNCH_RE = re.compile(
    r'\blaunch(?:es|ed|ing)\b'
    r'|\bintroduc(?:es|ed|ing)\b'
    r'|\bunveils?\b'
    r'|\bgeneral(?:ly)?\s+available\b|\bgeneral\s+availability\b|\bnow\s+GA\b|\breaches?\s+GA\b'
    r'|\bpublic(?:ly)?\s+available\b'
    r'|\bpublic\s+preview\b|\bopen\s+preview\b'
    r'|\bnow\s+available\b'
    r'|\bdebuts?\b'
    r'|\bannounces?\s+(?:new\s+)?(?:product|platform|solution|service|SDK|API|engine|framework|tool|model|update|release|version)\b',
    re.I,
)
# Groundbreaking features — major new capability announcements
_FEATURE_RE = re.compile(
    r'\bannounces?\s+(?:new\s+)?(?:capability|capabilities|feature|features|support\s+for|integration)\b'
    r'|\badds?\s+support\s+for\b'
    r'|\brolls?\s+out\b'
    r'|\bnew\s+(?:capability|capabilities|feature|features|integration)\b',
    re.I,
)
_LAYOFF_RE = re.compile(
    r'\blay\s*offs?\b|\blaid\s+off\b|\bjob\s+cuts?\b|\bworkforce\s+reduction\b|\brestructur(?:es|ed|ing)\b',
    re.I,
)
# Pricing changes — new tiers, price cuts, free plans, enterprise pricing
_PRICING_RE = re.compile(
    r'\bpricing\b'
    r'|\bprice\s+(?:change|cut|increase|reduction|update)\b'
    r'|\bnew\s+(?:pricing|price|tier|plan)\b'
    r'|\bfree\s+(?:tier|plan|version)\b'
    r'|\bopen[- ]source(?:s|d|ing)?\b'    # OSS releases = pricing signal
    r'|\bfreemium\b'
    r'|\benterprise\s+pricing\b',
    re.I,
)

# --- HARD DROP — always skip, even if a KEEP pattern matches -------------
_DROP_RE = re.compile(
    # Thought leadership / opinion / generic AI trend pieces
    r'\bAI\s+will\s+(?:change|transform|disrupt|revolutionize|reshape)\b'
    r'|\bfuture\s+of\s+\w+\b'
    r'|\bwhy\s+\w+\s+matters?\b'
    r'|\bthe\s+rise\s+of\b'
    r'|\bstate\s+of\s+\w+\s+(?:in\s+\d{4}|report)\b'
    r'|\bthought\s+leadership\b'
    r'|\bopinion\b'
    r'|\bperspective\b'
    r'|\bdeep\s+dive\b'
    # Awards / analyst recognition (not what user wants)
    r'|\bnamed\s+a\s+leader\b'
    r'|\bmagic\s+quadrant\b'
    r'|\bgartner\s+(?:names|recognizes)\b'
    r'|\bforrester\s+wave\b'
    r'|\bIDC\s+marketscape\b'
    r'|\brecogni[sz]ed\s+(?:as|by|in)\b'
    r'|\bwins?\s+(?:award|recognition)\b'
    r'|\baward(?:-winning|s)\b'
    # Customer stories / case studies / testimonials
    r'|\bcustomer\s+stor(?:y|ies)\b'
    r'|\bcase\s+stud(?:y|ies)\b'
    r'|\bsuccess\s+stor(?:y|ies)\b'
    r'|\bhow\s+\w+\s+(?:uses|used|leverages|leveraged|adopted)\b'
    # Educational / marketing content
    r'|\bhow\s+to\b|\btutorial\b|\bguide\s+to\b|\bbeginners?\s+guide\b'
    r'|\btips?\s+(?:and|&)\s+tricks\b|\bbest\s+practices?\b'
    r'|\bwebinar\b|\bpodcast\b|\binterview\s+with\b'
    r'|\bep(?:isode)?\s+\d+\b'
    # Surveys / reports / generic research
    r'|\bsurvey\s+(?:finds|reveals|shows|by|of)\b|\bnew\s+survey\b'
    r'|\bresearch\s+(?:finds|reveals|shows|report)\b'
    r'|\bbenchmark\s+report\b|\bindustry\s+report\b'
    # Event recaps (already covered by events page)
    r'|\brecap\b|\bhighlights?\s+from\b|\bat\s+\w+\s+\d{4}\b',
    re.I,
)


def classify_item(company: str, title: str, description: str, url: str) -> dict | None:
    """
    Strict classification. Returns dict or None (drop).

    KEEP categories: product_launch, feature, funding, leadership,
    partnership, acquisition, layoff.
    """
    combined = f"{title} {description}"

    # Hard drop first — overrides everything
    if _DROP_RE.search(combined):
        return None

    news_type = None
    if _FUNDING_RE.search(combined):
        news_type = "funding"
    elif _ACQUISITION_RE.search(combined):
        news_type = "acquisition"
    elif _LEADERSHIP_RE.search(combined):
        news_type = "leadership"
    elif _PARTNERSHIP_RE.search(combined):
        news_type = "partnership"
    elif _PRICING_RE.search(combined):
        news_type = "pricing"
    elif _PRODUCT_LAUNCH_RE.search(combined):
        news_type = "product_launch"
    elif _FEATURE_RE.search(combined):
        news_type = "feature"
    elif _LAYOFF_RE.search(combined):
        news_type = "layoff"
    else:
        return None  # Not a signal we care about

    # All kept categories are high-relevance by construction
    relevance = "high"

    # Tags
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
        "team_routing": _route_by_type(news_type),
    }


def _route_by_type(news_type: str) -> list[str]:
    routing = {
        "funding": ["Executives", "PMM"],
        "acquisition": ["Executives", "PMM", "Product"],
        "leadership": ["Executives", "PMM"],
        "partnership": ["PMM", "SDRs", "Marketing"],
        "pricing": ["SDRs", "Marketing", "PMM"],
        "product_launch": ["Product", "PMM", "Marketing"],
        "feature": ["Product", "PMM"],
        "layoff": ["Executives", "SDRs"],
    }
    return routing.get(news_type, ["PMM"])


# ══════════════════════════════════════════════════════════════════════════
# DATE EXTRACTION (reference: scraper.py extract_date)
# ══════════════════════════════════════════════════════════════════════════


def extract_date(html: str) -> str:
    """
    Extract publication date from HTML using multiple strategies.
    Returns ISO date string (YYYY-MM-DD) or empty string if not found.
    Optimized for newsroom pages with unstructured dates.
    """
    if not html:
        return ""

    # Pattern 1: JSON-LD "datePosted" field (most reliable)
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        try:
            return date.fromisoformat(m.group(1).split("T")[0]).isoformat()
        except Exception:
            pass

    # Pattern 2: HTML5 <time datetime="..."> tag
    m = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html, re.I)
    if m:
        try:
            return date.fromisoformat(m.group(1).split("T")[0]).isoformat()
        except Exception:
            pass

    # Pattern 3: ISO date format (YYYY-MM-DD) — Pinecone format
    m = re.search(r'(\d{4}-\d{2}-\d{2})', html)
    if m:
        return m.group(1)

    # Pattern 4a: Month DD, YYYY format — Alation, Qdrant, Collibra
    # Examples: "April 21, 2026", "December 10, 2025", "June 4, 2025"
    months_long = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'
    m = re.search(rf'({months_long})\s+(\d{{1,2}}),\s+(\d{{4}})', html, re.I)
    if m:
        try:
            from datetime import datetime
            dt = datetime.strptime(f"{m.group(1)} {m.group(2).zfill(2)} {m.group(3)}", "%B %d %Y")
            return dt.date().isoformat()
        except Exception:
            pass

    # Pattern 4b: DD Mon YYYY format — Atlan
    # Examples: "06 Jan 2026", "09 Jan 2025", "15 Mar 2025"
    months_short = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    m = re.search(rf'(\d{{1,2}})\s+({months_short})\s+(\d{{4}})', html, re.I)
    if m:
        try:
            from datetime import datetime
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %b %Y")
            return dt.date().isoformat()
        except Exception:
            pass

    # Pattern 4c: Mon DD, YYYY format — Collibra
    # Examples: "Mar 30, 2026", "Jan 28, 2026", "Dec 9, 2025"
    m = re.search(rf'({months_short})\s+(\d{{1,2}}),?\s+(\d{{4}})', html, re.I)
    if m:
        try:
            from datetime import datetime
            dt = datetime.strptime(f"{m.group(1)} {m.group(2).zfill(2)} {m.group(3)}", "%b %d %Y")
            return dt.date().isoformat()
        except Exception:
            pass

    # Pattern 5: M/D/YYYY format — Bigeye format
    # Examples: "4/7/2026", "12/10/2025", "3/23/2022"
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', html)
    if m:
        try:
            month = m.group(1).zfill(2)
            day = m.group(2).zfill(2)
            year = m.group(3)
            return f"{year}-{month}-{day}"
        except Exception:
            pass

    # Pattern 6: Relative dates ("posted 5 days ago")
    m = re.search(r'posted\s+(\d+)\s+days?\s+ago', html, re.I)
    if m:
        try:
            return (date.today() - timedelta(days=int(m.group(1)))).isoformat()
        except Exception:
            pass

    return ""


# ══════════════════════════════════════════════════════════════════════════
# PLAYWRIGHT SUPPORT — JS-rendered newsroom pages
# ══════════════════════════════════════════════════════════════════════════

# Newsrooms that require Playwright (JS-rendered dates)
PLAYWRIGHT_NEWSROOMS = {
    "Bigeye",      # React-rendered, dates in JS data
    "Atlan",       # JS-rendered content
    "Pinecone",    # Dynamic content
    "Collibra",    # Dynamic content
    "Databricks",  # JS-rendered newsroom
    "Snowflake",   # JS-rendered press releases
}


def fetch_newsroom_playwright(url: str) -> str:
    """Fetch newsroom page using Playwright for JS rendering.
    Tries domcontentloaded first (fast), falls back to load, then commit.
    Handles cookie consent banners automatically.
    Never lets timeouts be a roadblock.
    """
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

                # Dismiss cookie consent banners (OneTrust, Osano, etc.)
                consent_selectors = [
                    "button#onetrust-accept-btn-handler",   # OneTrust Accept All
                    "button.onetrust-accept-btn-handler",
                    "button[id*='accept'][id*='cookie']",
                    "button[class*='accept-all']",
                    "button[aria-label*='Accept']",
                    "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",  # Cookiebot
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
            pass  # Try next wait_event
    return ""


# ══════════════════════════════════════════════════════════════════════════
# NEWSROOM SCRAPER
# ══════════════════════════════════════════════════════════════════════════


def fetch_newsroom(company: str, url: str) -> list[dict]:
    """
    Scrape newsroom/press release page.

    Returns list of articles: {title, url, published_date, description}
    Published dates extracted from page HTML (JSON-LD, time tags, relative dates).
    Falls back to empty string if not found (will be filtered by within_window).
    Uses Playwright for JS-rendered sites (Bigeye, Atlan, Pinecone, Collibra).
    """
    html = ""

    # Use Playwright for JS-rendered newsrooms
    if company in PLAYWRIGHT_NEWSROOMS:
        html = fetch_newsroom_playwright(url)
    else:
        # Use httpx for static HTML sites (Qdrant, Alation)
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
    seen_titles = set()

    # Strategy A: Look for article links (most newsrooms have <a> tags with titles)
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        title = a.get_text(separator=" ", strip=True)

        # Normalize relative URLs
        if href.startswith("/"):
            from urllib.parse import urljoin

            href = urljoin(url, href)
        elif not href.startswith("http"):
            continue

        # Skip obvious nav links (allow up to 600 chars for sites like Collibra
        # that embed date + body into the link text)
        if len(title) < 15:
            continue
        # Truncate over-long titles to the first meaningful sentence
        if len(title) > 200:
            # Collibra pattern: "Location - Mon DD, YYYY Title body..." — strip location/date prefix
            clean = re.sub(r'^[A-Za-z ,]+\s*[-–]\s*\w+ \d+, \d{4}\s*', '', title).strip()
            title = clean[:200] if len(clean) >= 15 else title[:200]

        if title in seen_titles:
            continue

        # Filter: Only keep links that are external press releases or blog posts
        # (Skip internal nav/feature/product links)
        news_domains = ("einpresswire", "prnewswire", "datanami", "techcrunch", "venturebeat", "crn", "forbes", "medium")
        blog_patterns = ("blog", "press", "newsroom", "news", "release")
        is_external_news = any(domain in href.lower() for domain in news_domains)
        is_blog_post = any(pattern in href.lower() for pattern in blog_patterns)

        if not (is_external_news or is_blog_post):
            continue

        # Extract description from the IMMEDIATE parent only (one hop), and only
        # if it contains additional text beyond the link title itself.
        # Previously we walked up 4 levels which pulled in ALL neighboring
        # articles' text as the "description". That bug is fixed here.
        ctx = ""
        date_html = str(a)
        immediate = a.parent
        if immediate is not None:
            date_html = immediate.decode_contents()
            full = immediate.get_text(separator=" ", strip=True)
            # Remove the link's own text so we keep only surrounding sibling text
            link_text = a.get_text(separator=" ", strip=True)
            remainder = full.replace(link_text, "", 1).strip()
            # Only use it if it looks like a real teaser (>= 40 chars, not just a date/category)
            if len(remainder) >= 40 and not re.fullmatch(r'[\d/\-,.\s\w]{1,30}', remainder):
                ctx = remainder[:400]

        # Extract date — try title first (Pinecone, Collibra embed dates in title text),
        # then fall back to parent HTML (Alation, Qdrant use separate date elements)
        pub_date = extract_date(title) or extract_date(date_html) or extract_date(ctx)

        # Strip leading date/location/type noise from titles
        months = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)'
        clean_title = title
        # "4/7/2026 Title..."
        clean_title = re.sub(r'^\d{1,2}/\d{1,2}/\d{4}\s*', '', clean_title).strip()
        # "New York - Mar 30, 2026 Title..." (Collibra)
        clean_title = re.sub(rf'^[A-Za-z ,]+\s*[-–]\s*{months}\s+\d{{1,2}},?\s+\d{{4}}\s*', '', clean_title).strip()
        # "Product Apr 15, 2026 Title..." / "Featured Jan 28, 2026 Title..." (Pinecone)
        clean_title = re.sub(rf'^\w+\s+{months}\s+\d{{1,2}},?\s+\d{{4}}\s*', '', clean_title).strip()
        # "Apr 15, 2026 Title..." (bare month-day-year prefix)
        clean_title = re.sub(rf'^{months}\s+\d{{1,2}},?\s+\d{{4}}\s*', '', clean_title).strip()
        # "06 Jan 2026 Title..." (Atlan DD Mon YYYY)
        clean_title = re.sub(rf'^\d{{1,2}}\s+{months}\s+\d{{4}}\s*', '', clean_title).strip()
        if len(clean_title) >= 15:
            title = clean_title

        seen_titles.add(title)
        articles.append(
            {
                "title": title,
                "url": href,
                "published_date": pub_date,  # Real extracted date or empty string
                "description": ctx,
            }
        )

        if len(articles) >= MAX_ITEMS_PER_COMPANY:
            break

    return articles


def within_window(published_date_str: str) -> bool:
    """Check if date is within rolling window."""
    # Empty date = extraction failed, skip this item
    if not published_date_str:
        return False

    try:
        pub_date = date.fromisoformat(published_date_str)
        return (date.today() - pub_date).days <= MAX_NEWS_AGE_DAYS
    except Exception:
        return False  # Malformed dates get dropped


def clean_text(text: str) -> str:
    """Strip extra whitespace."""
    return re.sub(r'\s+', ' ', text).strip() if text else ""


def _clean_summary(desc: str, title: str) -> str:
    """Return a teaser only if it adds info beyond the title.
    Returns empty string if the description is just the title or a substring of it,
    or too short to be meaningful.
    """
    d = clean_text(desc)
    t = clean_text(title)
    if not d or len(d) < 40:
        return ""
    # If description begins with the title or is nearly equal, drop it
    dl, tl = d.lower(), t.lower()
    if dl.startswith(tl) or tl.startswith(dl):
        return ""
    # If 80%+ of description characters also appear in title → it's a duplicate
    if tl and len(tl) > 15 and tl in dl and len(dl) < len(tl) * 1.4:
        return ""
    return d


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════


def main():
    print("\n" + "=" * 70)
    print("NEWS SCRAPER — Phase 1 (Rule-based, zero API cost)")
    print("=" * 70)

    # Load seen URLs
    seen_urls = set()
    if SEEN_FILE.exists():
        try:
            with open(SEEN_FILE) as f:
                seen_urls = set(json.load(f))
        except Exception:
            pass

    # Load existing news
    existing_news = []
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE) as f:
                existing_news = json.load(f)
        except Exception:
            pass

    new_news = []

    # Scrape each company's newsroom (supports single URL or list of URLs)
    for company, url_entry in NEWSROOM_URLS.items():
        product_area = V2_PRODUCT_AREA_MAP.get(company)
        if not product_area:
            continue

        urls_to_scrape = url_entry if isinstance(url_entry, list) else [url_entry]
        articles = []
        for newsroom_url in urls_to_scrape:
            print(f"\n[{company}] {newsroom_url}")
            articles.extend(fetch_newsroom(company, newsroom_url))

        # Deduplicate across multiple URLs by URL
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

            if url in seen_urls:
                continue

            if not within_window(article["published_date"]):
                seen_urls.add(url)
                continue

            # Classify
            classification = classify_item(company, title, desc, url)
            if not classification:
                seen_urls.add(url)
                continue

            # Create news item
            news_item = {
                "company": company,
                "product_area": product_area,
                "news_type": classification["news_type"],
                "title": clean_text(title),
                "url": url,
                "published_date": article["published_date"],
                "source": "company_newsroom",
                "summary": _clean_summary(desc, title),  # Blank if summary just echoes title
                "actian_relevance": classification["actian_relevance"],
                "tags": classification["tags"],
                "team_routing": classification["team_routing"],
                "event_date": None,
                "scraped_at": today_str,
            }

            new_news.append(news_item)
            seen_urls.add(url)
            added += 1

            type_icon = {
                "funding": "💰",
                "leadership": "👤",
                "product_launch": "🚀",
                "feature": "✨",
                "partnership": "🤝",
                "acquisition": "🔀",
                "layoff": "📉",
            }.get(classification["news_type"], "📰")

            print(f"  → {type_icon} {title[:60]}")

        print(f"  ✓ {added} new item(s) added")

    # Merge with existing, drop old
    all_news = existing_news + new_news
    cutoff = (date.today() - timedelta(days=MAX_NEWS_AGE_DAYS)).isoformat()
    all_news = [n for n in all_news if n["published_date"] >= cutoff]

    # Sort by date (newest first)
    all_news.sort(key=lambda x: x["published_date"], reverse=True)

    # Save
    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_news, f, indent=2)
    print(f"\n✓ Saved {len(all_news)} news items to {OUTPUT_FILE}")

    # Save seen URLs
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen_urls), f)

    print(f"✓ Tracked {len(seen_urls)} seen URLs")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
