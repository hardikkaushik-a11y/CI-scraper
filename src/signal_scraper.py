"""
signal_scraper.py — Competitive Signal Scraper for Direct Intelligence
───────────────────────────────────────────────────────────────────────
• Scrapes RSS feeds from 11 allowed v2 companies (launches, events, blog posts)
• Classifies each item using Claude Sonnet (not Haiku — quality matters here)
• Deduplicates by URL across runs (file-based: data/seen_signals.json)
• Rolling 90-day window — drops stale items automatically
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

# Companies with working RSS feeds
RSS_FEEDS = {
    "Collibra":    "https://www.collibra.com/feed/",
    "Monte Carlo": "https://www.montecarlodata.com/blog/feed/",
    "Qdrant":      "https://qdrant.tech/blog/index.xml",
    "Snowflake":   "https://feeds.feedburner.com/SnowflakeBlog",
    "Databricks":  "https://www.databricks.com/feed/",
}

# Companies without RSS — use HTML blog/newsroom scraping as fallback
# Each entry: (page_url, url_must_contain_pattern)
HTML_SOURCES = {
    "Atlan":       ("https://atlan.com/newsroom/", "atlan.com"),
    "Alation":     ("https://www.alation.com/blog/", "alation.com/blog/"),
    "Bigeye":      ("https://www.bigeye.com/blog/", "bigeye.com/blog/"),
    "Acceldata":   ("https://www.acceldata.io/blog/", "acceldata.io/blog/"),
    "Pinecone":    ("https://www.pinecone.io/blog/", "/blog/"),
    "Milvus":      ("https://milvus.io/blog/", "/blog/"),
}

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
    """Call Claude Sonnet to classify a single signal item. Returns dict or None."""
    prompt = f"""Company: {company}
Title: {title}
Content: {description[:800]}

Classify and return JSON."""

    raw = _call_claude(SONNET_MODEL, CLASSIFY_SYSTEM, prompt, max_tokens=512)
    if not raw:
        return None

    # Strip accidental markdown code fences
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip())

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  [WARN] JSON parse failed for: {title[:60]}")
        return None

    # Validate and sanitize enum fields — never let invalid values through
    valid_types     = {"product_launch", "event", "partnership", "funding", "open_source_release", "blog_post"}
    valid_relevance = {"low", "medium", "high"}
    valid_source    = {"blog", "event_page", "press_release", "github"}

    if result.get("type") not in valid_types:
        result["type"] = "blog_post"
    if result.get("actian_relevance") not in valid_relevance:
        result["actian_relevance"] = "low"
    if result.get("source_type") not in valid_source:
        result["source_type"] = "blog"
    if not isinstance(result.get("tags"), list):
        result["tags"] = []

    return result

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

# Patterns that indicate a nav/footer link, not a real blog post
_NAV_RE = re.compile(
    r"^(home|blog|resources|about|pricing|contact|careers|login|sign\s+in|"
    r"get\s+started|read\s+more|learn\s+more|view\s+all|see\s+all|"
    r"subscribe|newsletter|all\s+posts|back\s+to)$",
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
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    if not ANTHROPIC_API_KEY:
        print("[WARN] No ANTHROPIC_API_KEY — signal classification will be skipped")

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

            # Skip items outside rolling window — mark seen so we skip next run too
            if not within_window(pub):
                seen_urls.add(url)
                continue

            # Skip items with no title or URL
            if not title or not url:
                seen_urls.add(url)
                continue

            # Classify via Claude Sonnet
            print(f"  → {title[:75]}")
            if not ANTHROPIC_API_KEY:
                seen_urls.add(url)
                continue

            classification = classify_item(company, title, item["description"])

            if not classification:
                print(f"    [WARN] Classification failed — skipping")
                seen_urls.add(url)
                continue

            # Drop low-relevance blog posts to keep signals meaningful
            if classification["type"] == "blog_post" and classification["actian_relevance"] == "low":
                seen_urls.add(url)
                time.sleep(0.3)
                continue

            signal = {
                "company":          company,
                "product_area":     product_area,
                "type":             classification["type"],
                "title":            title,
                "url":              url,
                "published_date":   pub,
                "summary":          classification.get("summary", ""),
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
