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
}

# Newsroom URLs — user-provided, verified
NEWSROOM_URLS = {
    "Bigeye": "https://www.bigeye.com/newsroom",
    "Atlan": "https://atlan.com/newsroom/",
    "Qdrant": "https://qdrant.tech/blog/",
    "Collibra": "https://www.collibra.com/company/newsroom/press-releases",
    "Alation": "https://www.alation.com/news-and-press/",
    "Pinecone": "https://www.pinecone.io/newsroom/",
}

today_str = date.today().isoformat()

# ══════════════════════════════════════════════════════════════════════════
# PATTERN MATCHING — Rule-based classification (zero API cost, Phase 1)
# ══════════════════════════════════════════════════════════════════════════

# Tier 1: Hard signal patterns — deterministic news types
_FUNDING_RE = re.compile(
    r'\bSeries\s+[A-Z]|\braised\s+\$|\bfunding\s+round|\binvestment\s+round'
    r'|\bsecured\s+\$|\bclosed\s+\$',
    re.I,
)
_LEADERSHIP_RE = re.compile(
    r'\bappoints?|\bnames?|\bjoins?\s+as|\bhires?\s+(?:as\s+)?(?:new\s+)?(?:CEO|CTO|SVP|VP|Chief|President)'
    r'|\bCEO|\bCTO|\bChief\s+(?:Executive|Technology|Financial|Revenue|Product|Operating)'
    r'|\beleads?\s+as',
    re.I,
)
_PRODUCT_RE = re.compile(
    r'\blaunch(?:es|ed|ing)?(?:\s+(?:new\s+)?(?:product|platform|feature|tool))?\b'
    r'|\bintroduc(?:es|ed|ing)(?:\s+(?:new\s+)?(?:product|platform|feature|tool))?\b'
    r'|\bannounce(?:s|d)?(?:\s+(?:new\s+)?(?:product|platform|feature|tool|release))?\b'
    r'|\bGA\b|\bgeneral\s+availability'
    r'|\bnow\s+available\b',
    re.I,
)
_PRICING_RE = re.compile(
    r'\bpricing\b|\bprice\s+(?:change|cut|increase|update)'
    r'|\bnew\s+pricing\s+(?:model|tier|option)'
    r'|\bfree\s+tier|\bfree\s+plan',
    re.I,
)
_EXPANSION_RE = re.compile(
    r'\bexpand(?:s|ed|ing)?\b'
    r'|\bnew\s+(?:office|region|market|country|location)'
    r'|\blaunch(?:es|ed|ing)?\s+(?:in|to)\b'
    r'|\benter(?:s|ed|ing)?\s+(?:the\s+)?(?:market|region)',
    re.I,
)
_AWARD_RE = re.compile(
    r'\bGartner\b|\bMagic\s+Quadrant'
    r'|\bIDC\b|\bForrester'
    r'\baward(?:s|ed)?\b|\brecognition\b|\bnamed\s+(?:leader|best)'
    r'|\bwins?\s+(?:award|recognition)',
    re.I,
)
_PARTNERSHIP_RE = re.compile(
    r'\bpartnership\b|\bpartnered\b'
    r'|\bintegrat(?:es?|ed|ing)\s+with'
    r'|\bacquir(?:es|ed)?\b|\bmerger\b'
    r'|\bstrategic\s+alliance\b',
    re.I,
)

# Tier 2: Hard exclude — noise patterns (always skip, even if Tier 1 matches)
_NOISE_RE = re.compile(
    r'\bcustomer\s+story'
    r'|\bcase\s+study'
    r'|\bhow\s+to\b|\bhow\s+[-]?to'
    r'|\btips?\s+(?:and|&)\s+tricks'
    r'|\btutorial\b'
    r'|\bthought\s+leadership'
    r'|\bopinion\b'
    r'|\bwebinar\b'
    r'|\bguide\s+to'
    r'|\bbeginners?\s+guide'
    r'|\bbest\s+practices?'
    r'|\btrends?\s+(?:in|for|report)'
    r'|\bpodcast\s+episode',
    re.I,
)

# Tier 3: Soft exclude — blog-post-only patterns (exclude unless has strong signal)
_BLOG_ONLY_RE = re.compile(
    r'\bblog\s+post|^blog:|insights?|perspective|deep\s+dive|interview',
    re.I,
)


def classify_item(company: str, title: str, description: str, url: str) -> dict | None:
    """
    Rule-based classification of news item.

    Returns dict with: news_type, actian_relevance, tags, team_routing
    Returns None if should be skipped.

    Phase 2 (later): Can add Haiku here to improve summaries.
    """
    combined = f"{title} {description}".lower()
    url_lower = url.lower()

    # Hard exclude: noise patterns
    if _NOISE_RE.search(combined):
        return None

    # Detect news type (priority order)
    news_type = None
    relevance = "medium"

    if _FUNDING_RE.search(combined):
        news_type = "funding"
        relevance = "high"  # Funding is always strategic
    elif _LEADERSHIP_RE.search(combined):
        news_type = "leadership"
        relevance = "high"  # Leadership changes are strategic
    elif _PRODUCT_RE.search(combined):
        news_type = "product"
        relevance = "high"  # Product launches are strategic
    elif _AWARD_RE.search(combined):
        news_type = "award"
        relevance = "high"  # Awards = market validation
    elif _PARTNERSHIP_RE.search(combined):
        news_type = "partnership"
        relevance = "high"  # Strategic partnerships matter
    elif _EXPANSION_RE.search(combined):
        news_type = "expansion"
        relevance = "high"  # Expansion = growth signal
    elif _PRICING_RE.search(combined):
        news_type = "pricing"
        relevance = "medium"  # Pricing changes can be significant
    elif _BLOG_ONLY_RE.search(combined):
        # Blog posts with no strong signal = skip
        return None
    else:
        # Doesn't match any category
        return None

    if not news_type:
        return None

    # Extract tags
    tags = []
    if "Series" in title or "raised" in title:
        if "Series A" in title:
            tags.append("Series A")
        elif "Series B" in title:
            tags.append("Series B")
        elif "Series C" in title:
            tags.append("Series C")
        elif "Series D" in title:
            tags.append("Series D")
        if "$" in title:
            # Extract dollar amount (rough)
            dollar_match = re.search(r'\$\s*(\d+\.?\d*)\s*[MB](?:illion)?', title)
            if dollar_match:
                tags.append(f"${dollar_match.group(1)}M")

    if "CEO" in title or "CTO" in title or "VP" in title:
        tags.append("leadership_exec")

    if "AI" in title or "agent" in title.lower():
        tags.append("AI")

    if "APAC" in title or "Europe" in title or "Asia" in title:
        tags.append("international_expansion")

    # Determine team routing
    team_routing = _route_by_type(news_type)

    return {
        "news_type": news_type,
        "actian_relevance": relevance,
        "tags": tags,
        "team_routing": team_routing,
    }


def _route_by_type(news_type: str) -> list[str]:
    """Determine which teams should see this news."""
    routing = {
        "funding": ["Executives", "PMM"],
        "leadership": ["Executives", "PMM"],
        "product": ["Product", "PMM", "Marketing"],
        "pricing": ["Sales", "PMM", "SDRs"],
        "expansion": ["Sales", "SDRs", "PMM"],
        "award": ["Marketing", "PMM", "Executives"],
        "partnership": ["PMM", "SDRs"],
    }
    return routing.get(news_type, ["PMM"])


# ══════════════════════════════════════════════════════════════════════════
# NEWSROOM SCRAPER
# ══════════════════════════════════════════════════════════════════════════


def fetch_newsroom(company: str, url: str) -> list[dict]:
    """
    Scrape newsroom/press release page.

    Returns list of articles: {title, url, published_date, description}
    """
    try:
        r = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"  [WARN] {company}: Fetch failed — {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
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

        # Skip obvious nav links
        if not (15 <= len(title) <= 200):
            continue
        if title in seen_titles:
            continue

        # Extract context (description)
        parent = a.parent
        ctx = ""
        for _ in range(4):
            if parent:
                ctx = parent.get_text(separator=" ", strip=True)[:400]
                parent = parent.parent
            if len(ctx) > 50:
                break

        seen_titles.add(title)
        articles.append(
            {
                "title": title,
                "url": href,
                "published_date": date.today().isoformat(),  # No date on listing, use today
                "description": ctx,
            }
        )

        if len(articles) >= MAX_ITEMS_PER_COMPANY:
            break

    return articles


def within_window(published_date_str: str) -> bool:
    """Check if date is within rolling window."""
    try:
        pub_date = date.fromisoformat(published_date_str)
        return (date.today() - pub_date).days <= MAX_NEWS_AGE_DAYS
    except Exception:
        return True  # Keep items with bad dates


def clean_text(text: str) -> str:
    """Strip extra whitespace."""
    return re.sub(r'\s+', ' ', text).strip() if text else ""


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

    # Scrape each company's newsroom
    for company, newsroom_url in NEWSROOM_URLS.items():
        product_area = V2_PRODUCT_AREA_MAP.get(company)
        if not product_area:
            continue

        print(f"\n[{company}] {newsroom_url}")
        articles = fetch_newsroom(company, newsroom_url)

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
                "summary": clean_text(title),  # Phase 2: Haiku will improve this
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
                "product": "🚀",
                "pricing": "💲",
                "expansion": "🌍",
                "award": "🎖️",
                "partnership": "🤝",
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
