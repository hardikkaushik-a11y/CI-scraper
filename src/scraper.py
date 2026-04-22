"""
scraper.py — Competitive Hiring Intelligence Scraper v5
────────────────────────────────────────────────────────
• Reads competitor list from competitors.csv — zero hardcoded companies
• Async httpx for static pages, shared Playwright browser for JS-heavy sites
• Direct API extraction for Lever, Greenhouse, and Ashby ATS
• Single extraction path — no dual-path duplication
• SQLite persistent dedup across daily runs (seen_jobs.db)
• Per-domain rate limiting, max 5 concurrent requests
• Enhanced title cleaning with location extraction
• FORBIDDEN_TEXT_RE + GARBAGE_TITLE_RE filtering for non-job content
• 365-day rolling window — drops jobs with posting dates older than 1 year
• Location sanitization — strips work-type and scraping artifacts from location
• Outputs jobs_raw.csv
"""

import asyncio
import csv
import hashlib
import json
import os
import re
import sqlite3
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

# ── Optional Playwright (only imported when needed) ──────────────────────
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

MAX_JOB_AGE_DAYS = 365   # Drop jobs older than this
MAX_JOBS_PER_COMPANY = 200  # Hard cap per company to prevent large-org flood

# JS-heavy sites that require Playwright rendering
PLAYWRIGHT_DOMAINS = {
    "snowflake", "databricks", "oracle", "salesforce", "ibm",
    "sap", "collibra", "cloudera", "alteryx", "mongodb",
    "amazon", "pentaho", "onetrust", "atlassian",
    "microsoft", "google", "alation",
}

# URL domains that need Playwright (for more flexible matching)
PLAYWRIGHT_URL_DOMAINS = {
    "onetrust.com", "atlassian.com", "microsoft.com", "google.com",
    "myworkdayjobs.com",       # All Workday career sites
    "careers.exasol.com",      # Recruitee-powered, needs JS render
    "www.firebolt.io",         # Custom JS career portal
    "www.denodo.com",          # Custom JS career portal
    "www.domo.com",            # Custom Webflow career portal
    "boomi.com",               # React-powered career portal
    "data.world",              # Greenhouse embed, needs JS render
    "careers.teradata.com",    # gr8people Next.js portal, needs JS render
    "careerhub.qlik.com",      # Eightfold AI — fully JS-rendered
}

# ATS patterns that indicate a real job detail URL
ATS_PATTERNS = [
    "lever.co", "greenhouse.io", "myworkdayjobs.com", "ashbyhq.com",
    "bamboohr.com", "smartrecruiters.com", "jobvite.com", "gr8people.com",
    "welcometothejungle.com", "/jobs/", "/job/",
    "/careers/job", "/open-roles/", "/openings/",
]

# Non-English character pattern — filter out CJK/non-Latin job titles
NON_ENGLISH_RE = re.compile(r'[\u3000-\u9fff\uac00-\ud7af\u0400-\u04ff]')

ROLE_RE = re.compile(
    r'\b(engineer|developer|manager|director|architect|scientist|analyst|'
    r'product|sre|intern|specialist|consultant|lead|head|vp|chief|officer|'
    r'executive|recruiter|designer|researcher|strategist|coordinator|accountant|'
    r'counsel|attorney|writer|advocate)\b', re.I
)

FORBIDDEN_URL_RE = re.compile(
    r'/(about|privacy|press|blog|partners|pricing|docs|support|events|'
    r'resources|login|news|legal|contact|team|culture|values|benefits)(/|$)',
    re.I
)

# Non-job content text patterns
FORBIDDEN_TEXT_RE = re.compile(
    r'\b(demo|webinar|podcast|blog|whitepaper|case study|ebook|'
    r'watch now|sign up|subscribe|pricing|learn more|request|'
    r'data ?sheet|product tour|contact us|see how|documentation)\b', re.I
)

# Garbage titles — text that is clearly not a real job title
GARBAGE_TITLE_RE = re.compile(
    r'^(apply|jobs?|careers?|search|view|open positions|all jobs|'
    r'explore|click|submit|back|next|previous|loading|search by|'
    r'filter|sort|show|hide|close|menu|home|login|sign in|'
    r'apply here|apply now|see all|view all|learn more|'
    r'explore our roles|join our team|work with us|'
    r'search results|no results|page \d|'
    r'internal developer portal|developer portal|product overview|'
    r'documentation|docs|blog|pricing|platform|solutions|resources)$', re.I
)

# URLs that indicate a product/marketing page, not a job listing
GARBAGE_URL_RE = re.compile(
    r'/(product|products|solutions|platform|features|pricing|blog|docs|documentation|resources|about|news)/',
    re.I
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ══════════════════════════════════════════════════════════════════════════
# LOAD COMPETITORS FROM CSV
# ══════════════════════════════════════════════════════════════════════════

def load_competitors(path: str = "competitors.csv") -> list[dict]:
    """Load competitor list from CSV. Returns list of dicts with Company, Career_URL, Company_Group."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"competitors.csv not found at {path}")
    companies = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            company = row.get("Company", "").strip()
            url = row.get("Career_URL", "").strip()
            group = row.get("Company_Group", "Other").strip()
            if company and url:
                companies.append({
                    "Company": company,
                    "Career_URL": url,
                    "Company_Group": group,
                })
    print(f"[CONFIG] Loaded {len(companies)} competitors from {path}")
    return companies


def needs_playwright(company: str, url: str) -> bool:
    """Check if a company/URL requires Playwright rendering."""
    if company.lower() in PLAYWRIGHT_DOMAINS:
        return True
    domain = urlparse(url).netloc.lower()
    return any(d in domain for d in PLAYWRIGHT_URL_DOMAINS)

# ══════════════════════════════════════════════════════════════════════════
# SQLITE DEDUP DATABASE
# ══════════════════════════════════════════════════════════════════════════

def init_db(db_path: str = "data/seen_jobs.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_jobs (
            url_hash   TEXT PRIMARY KEY,
            job_url    TEXT,
            company    TEXT,
            title      TEXT,
            first_seen TEXT,
            last_seen  TEXT
        )
    """)
    conn.commit()
    return conn


def url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().lower().encode()).hexdigest()[:16]


def upsert_job(conn: sqlite3.Connection, job: dict) -> tuple[bool, str]:
    """Insert or update job. Returns (is_new, first_seen_date)."""
    h = url_hash(job["Job Link"])
    today = date.today().isoformat()
    row = conn.execute("SELECT first_seen FROM seen_jobs WHERE url_hash=?", (h,)).fetchone()
    if row:
        conn.execute(
            "UPDATE seen_jobs SET last_seen=?, title=? WHERE url_hash=?",
            (today, job["Job Title"], h),
        )
        conn.commit()
        return False, row[0]
    else:
        conn.execute(
            "INSERT INTO seen_jobs VALUES (?,?,?,?,?,?)",
            (h, job["Job Link"], job["Company"], job["Job Title"], today, today),
        )
        conn.commit()
        return True, today


def prune_old_jobs(conn: sqlite3.Connection, max_age_days: int = MAX_JOB_AGE_DAYS):
    """Remove jobs from dedup DB that haven't been seen in max_age_days."""
    cutoff = (date.today() - timedelta(days=max_age_days)).isoformat()
    deleted = conn.execute("DELETE FROM seen_jobs WHERE last_seen < ?", (cutoff,)).rowcount
    conn.commit()
    if deleted:
        print(f"[DB] Pruned {deleted} jobs older than {max_age_days} days from dedup DB")

# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def normalize_url(base: str, href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if urlparse(href).netloc:
        return href
    try:
        return urljoin(base, href)
    except Exception:
        return href


def is_too_old(posting_date: str) -> bool:
    """Return True if the posting date is older than MAX_JOB_AGE_DAYS."""
    if not posting_date:
        return False  # Unknown date — keep it
    try:
        d = datetime.fromisoformat(posting_date).date()
        return (date.today() - d).days > MAX_JOB_AGE_DAYS
    except Exception:
        return False


def extract_location_from_title(raw: str) -> str:
    """
    Extract location info from title garbage before cleaning.
    Patterns:
    - Workday: "Title IND-CHENNAI Hybrid" → "Chennai, India"
    - MongoDB: "Title location Dublin suitcase Full-time chevron-right" → "Dublin"
    """
    if not raw:
        return ""

    # Workday format: IND-CHENNAI, USA-CA-REDWOOD CITY, USA-REMOTE, etc.
    m = re.search(r'\s([A-Z]{2,4})[-–](?:[A-Z]{2}[-–])?([A-Z][A-Za-z\s]+?)(?:\s+[•·]|\s+Remote|\s+Hybrid|\s+On.?Site)', raw)
    if m:
        code, city = m.group(1), m.group(2).strip()
        country_map = {
            "IND": "India", "USA": "USA", "GBR": "UK", "CAN": "Canada",
            "AUS": "Australia", "DEU": "Germany", "FRA": "France", "SGP": "Singapore",
            "JPN": "Japan", "NLD": "Netherlands", "IRL": "Ireland", "ESP": "Spain",
        }
        country = country_map.get(code, code)
        if city.upper() == "REMOTE":
            return f"Remote, {country}" if country != code else "Remote"
        return f"{city.title()}, {country}"

    # MongoDB format: "location Dublin suitcase Full-time"
    m = re.search(r'location\s+(.+?)\s+suitcase', raw, re.I)
    if m:
        loc = m.group(1).strip()
        loc = loc.replace(";", ",")
        return loc

    return ""


def clean_location(raw: str) -> str:
    """
    Sanitize location field — remove work-type info, scraping artifacts,
    and UI garbage that leaked into location.
    """
    if not raw:
        return ""
    loc = raw.strip()

    # Remove obvious garbage
    garbage_patterns = [
        r'^search by location$',
        r'^search$',
        r'^all locations?$',
        r'^filter$',
        r'^select$',
        r'^n/?a$',
        r'^-$',
        r'^none$',
        r'^location$',
        r'^anywhere$',
        r'^multiple locations?$',
    ]
    for pat in garbage_patterns:
        if re.match(pat, loc, re.I):
            return ""

    # Strip work-type suffixes: "New York, NY - Remote" → "New York, NY"
    loc = re.sub(r'\s*[-–|•,]\s*(remote|hybrid|on.?site|full.?time|part.?time|contract|temporary)\s*$', '', loc, flags=re.I).strip()

    # Strip leading work-type: "Remote - New York" → "New York"
    loc = re.sub(r'^(remote|hybrid|on.?site|full.?time|part.?time)\s*[-–|•,]\s*', '', loc, flags=re.I).strip()

    # Strip parenthetical work-type: "NYC (Remote)" → "NYC"
    loc = re.sub(r'\s*\([^)]*(?:remote|hybrid|on.?site|full.?time|part.?time)[^)]*\)\s*$', '', loc, flags=re.I).strip()

    # If after cleaning it's just a work type, return empty
    if re.match(r'^(remote|hybrid|on.?site|full.?time|part.?time|contract|temporary)$', loc, re.I):
        return ""

    return loc[:200]


def clean_title(raw: str) -> str:
    """
    Clean title by stripping location codes, work types, and junk.
    """
    if not raw:
        return ""

    t = re.sub(r'\s+', ' ', raw).strip()

    # Reject garbage titles early
    if GARBAGE_TITLE_RE.match(t):
        return ""

    # Workday format: strip location code onwards
    t = re.sub(
        r'\s+[A-Z]{2,4}[-–][A-Z][-A-Z\s,]+[•·]\s*(Remote|Hybrid|On.?Site).*$',
        '', t, flags=re.I
    ).strip()

    # MongoDB format: strip "location CITY... suitcase... chevron-right"
    t = re.sub(r'\s+location\s+.+$', '', t, flags=re.I).strip() if re.search(r'\s+location\s+.+\s+suitcase\s+', t, re.I) else t

    # Generic bullet/dash + worktype patterns
    t = re.sub(r'\s+[•\-|]\s+(Remote|Hybrid|On.?Site|Full.?time|Part.?time|Contract|Temporary).*$', '', t, flags=re.I).strip()

    # Parenthetical patterns
    t = re.sub(r'\s*\([^)]*(?:remote|hybrid|on.?site|full.?time|part.?time|contract|temporary)[^)]*\)\s*$', '', t, flags=re.I).strip()

    # Remove leading punctuation/numbers
    t = re.sub(r'^[\-•\*\d\.]+\s*', '', t)

    # Strip trailing location-like suffixes: "Engineer - New York, NY"
    # Only if there's a clear separator and it looks like city/state
    t = re.sub(r'\s+[-–|]\s+[A-Z][a-z]+(?:,\s*[A-Z]{2})?\s*$', '', t).strip()

    t = t.strip(" -:,.|")

    # Final garbage check after cleaning
    if GARBAGE_TITLE_RE.match(t):
        return ""

    return t[:240]


def is_job_url(href: str, text: str) -> bool:
    if not href:
        return False
    if FORBIDDEN_URL_RE.search(href):
        return False
    if GARBAGE_URL_RE.search(href):
        return False
    if FORBIDDEN_TEXT_RE.search(text):
        return False
    # Reject garbage anchor text
    cleaned = text.strip()
    if GARBAGE_TITLE_RE.match(cleaned):
        return False
    if len(cleaned) < 3:
        return False
    h = href.lower()
    t = (text or "").lower()
    if any(p in h for p in ATS_PATTERNS):
        return True
    if ROLE_RE.search(t) and len(t.split()) >= 2:
        return True
    return False


def _strip_html(html_text: str) -> str:
    """Strip HTML tags and collapse whitespace for description text."""
    text = re.sub(r'<[^>]+>', ' ', html_text or '')
    return re.sub(r'\s+', ' ', text).strip()


def extract_date(html: str) -> str:
    if not html:
        return ""
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        try:
            return datetime.fromisoformat(m.group(1).split("T")[0]).date().isoformat()
        except Exception:
            pass
    m = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html, re.I)
    if m:
        try:
            return datetime.fromisoformat(m.group(1).split("T")[0]).date().isoformat()
        except Exception:
            pass
    m = re.search(r'posted\s+(\d+)\s+days?\s+ago', html, re.I)
    if m:
        try:
            return (date.today() - timedelta(days=int(m.group(1)))).isoformat()
        except Exception:
            pass
    m = re.search(r'(\d{4}-\d{2}-\d{2})', html)
    if m:
        return m.group(1)
    return ""


def dedup_and_cap(jobs: list[dict], company: str, cap: int = MAX_JOBS_PER_COMPANY) -> list[dict]:
    """
    Deduplicate by title (max 3 per identical title — keeps regional variants,
    removes spam like 9 identical 'AI Engineer - FDE' rows) then cap total.
    Also drops non-English titles.
    """
    title_counts: dict[str, int] = {}
    filtered = []
    for job in jobs:
        title = job.get("Job Title", "")
        # Drop non-English titles (CJK, Cyrillic, etc.)
        if NON_ENGLISH_RE.search(title):
            continue
        count = title_counts.get(title, 0)
        if count >= 3:
            continue
        title_counts[title] = count + 1
        filtered.append(job)

    if len(filtered) > cap:
        print(f"  [{company}] Capped {len(filtered)} → {cap} jobs after title dedup")
        filtered = filtered[:cap]
    return filtered


def extract_location(soup) -> str:
    selectors = [
        ".location", ".job-location", ".posting-location",
        "[data-test='job-location']", ".job_meta_location",
        ".location--name", "[class*='location']",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            raw = el.get_text(" ", strip=True)
            cleaned = clean_location(raw)
            if cleaned:
                return cleaned
    # JSON-LD fallback
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for obj in items:
                jl = obj.get("jobLocation") or obj.get("jobLocations")
                if jl:
                    entry = jl[0] if isinstance(jl, list) else jl
                    if isinstance(entry, dict):
                        addr = entry.get("address") or entry
                        if isinstance(addr, dict):
                            parts = [
                                addr.get(k, "")
                                for k in ("addressLocality", "addressRegion", "addressCountry")
                                if addr.get(k)
                            ]
                            if parts:
                                return clean_location(", ".join(parts))
        except Exception:
            pass
    return ""

# ══════════════════════════════════════════════════════════════════════════
# RATE LIMITER (per-domain)
# ══════════════════════════════════════════════════════════════════════════

class DomainRateLimiter:
    """Enforces per-domain rate limiting with configurable delay."""

    def __init__(self, delay: float = 1.0):
        self._delay = delay
        self._last_request: dict[str, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    async def wait(self, url: str):
        domain = urlparse(url).netloc
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request[domain]
            if elapsed < self._delay:
                await asyncio.sleep(self._delay - elapsed)
            self._last_request[domain] = time.monotonic()

# ══════════════════════════════════════════════════════════════════════════
# ASYNC FETCHERS
# ══════════════════════════════════════════════════════════════════════════

rate_limiter = DomainRateLimiter(delay=1.0)


async def fetch_html(client: httpx.AsyncClient, url: str, retries: int = 2) -> str:
    for attempt in range(retries + 1):
        try:
            await rate_limiter.wait(url)
            r = await client.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code == 429:
                wait = min(10, 2 ** (attempt + 1))
                print(f"  [429] Rate limited on {urlparse(url).netloc}, waiting {wait}s")
                await asyncio.sleep(wait)
                continue
        except Exception as e:
            if attempt == retries:
                print(f"  [WARN] fetch failed {url}: {e}")
            await asyncio.sleep(1.5 * (attempt + 1))
    return ""


async def fetch_with_playwright(url: str, browser=None) -> str:
    """Fetch a JS-heavy page using Playwright. Uses shared browser if provided."""
    if not PLAYWRIGHT_AVAILABLE:
        print(f"  [WARN] Playwright not available, skipping {url}")
        return ""
    try:
        if browser:
            page = await browser.new_page()
            try:
                await page.goto(url, timeout=40000, wait_until="networkidle")
                await page.wait_for_timeout(2000)
                html = await page.content()
                return html
            finally:
                await page.close()
        else:
            # Fallback: launch own browser (shouldn't happen in normal flow)
            async with async_playwright() as p:
                b = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                page = await b.new_page()
                await page.goto(url, timeout=40000, wait_until="networkidle")
                await page.wait_for_timeout(2000)
                html = await page.content()
                await b.close()
                return html
    except Exception as e:
        print(f"  [PLAYWRIGHT-WARN] {url}: {e}")
        return ""

# ══════════════════════════════════════════════════════════════════════════
# ATS API EXTRACTORS
# ══════════════════════════════════════════════════════════════════════════

async def extract_lever_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via Lever API (jobs.lever.co)."""
    try:
        slug = url.rstrip('/').split('/')[-1].split('?')[0]
        api_url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        await rate_limiter.wait(api_url)
        r = await client.get(api_url, headers=HEADERS, timeout=20, follow_redirects=True)
        if r.status_code != 200:
            return []

        data = r.json()
        if isinstance(data, list):
            postings = data
        elif isinstance(data, dict):
            postings = data.get("postings", data.get("jobs", []))
        else:
            postings = []
        jobs = []

        for item in postings:
            title = clean_title(item.get("text", ""))
            if not title or not ROLE_RE.search(title):
                continue

            location = ""
            categories = item.get("categories", {})
            if isinstance(categories, dict):
                location = clean_location(categories.get("location", ""))

            job_url = item.get("hostedUrl", "")
            created_at = item.get("createdAt", "")
            posting_date = ""
            if created_at:
                try:
                    posting_date = datetime.fromtimestamp(created_at / 1000).date().isoformat()
                except Exception:
                    pass

            # Skip jobs older than MAX_JOB_AGE_DAYS
            if is_too_old(posting_date):
                continue

            desc = item.get("descriptionPlain", "") or _strip_html(item.get("description", ""))
            desc = re.sub(r'\s+', ' ', desc).strip()[:600]

            jobs.append({
                "Company": company,
                "Job Title": title,
                "Job Link": job_url,
                "Location": location,
                "Posting Date": posting_date,
                "Seniority": "Mid",  # enrich.py re-detects with better logic
                "Description": desc,
            })

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Lever API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Lever API WARN] {company}: {e}")
        return []


async def extract_greenhouse_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via Greenhouse API (boards.greenhouse.io or job-boards.greenhouse.io)."""
    try:
        board = url.rstrip('/').split('/')[-1].split('?')[0]
        api_url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
        await rate_limiter.wait(api_url)
        r = await client.get(api_url, headers=HEADERS, timeout=20, follow_redirects=True)
        if r.status_code != 200:
            return []

        data = r.json()
        jobs_list = data.get("jobs", [])
        jobs = []

        for item in jobs_list:
            title = clean_title(item.get("title", ""))
            if not title or not ROLE_RE.search(title):
                continue

            location = ""
            loc_obj = item.get("location", {})
            if isinstance(loc_obj, dict):
                location = clean_location(loc_obj.get("name", ""))

            job_url = item.get("absolute_url", "")
            updated_at = item.get("updated_at", "")
            posting_date = ""
            if updated_at:
                try:
                    posting_date = datetime.fromisoformat(updated_at.split("T")[0]).date().isoformat()
                except Exception:
                    pass

            if is_too_old(posting_date):
                continue

            # Use department name as a classification hint (already in bulk response)
            depts = item.get("departments", [])
            dept_hint = depts[0].get("name", "") if depts else ""

            jobs.append({
                "Company": company,
                "Job Title": title,
                "Job Link": job_url,
                "Location": location,
                "Posting Date": posting_date,
                "Seniority": "Mid",
                "Description": f"Dept: {dept_hint}" if dept_hint else "",
            })

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Greenhouse API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Greenhouse API WARN] {company}: {e}")
        return []


async def extract_ashby_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via Ashby API (jobs.ashbyhq.com)."""
    try:
        slug = url.rstrip('/').split('/')[-1].split('?')[0]
        api_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        await rate_limiter.wait(api_url)
        r = await client.get(api_url, headers=HEADERS, timeout=20, follow_redirects=True)
        if r.status_code != 200:
            return []

        data = r.json()
        jobs_list = data.get("jobs", [])
        jobs = []

        for item in jobs_list:
            title = clean_title(item.get("title", ""))
            if not title or not ROLE_RE.search(title):
                continue

            location = clean_location(item.get("location", ""))
            job_url = item.get("jobUrl", "")
            published_at = item.get("publishedAt", "")
            posting_date = ""
            if published_at:
                try:
                    posting_date = datetime.fromisoformat(published_at.split("T")[0]).date().isoformat()
                except Exception:
                    pass

            if is_too_old(posting_date):
                continue

            # Ashby bulk API doesn't include description; use department as hint
            dept_hint = item.get("department", item.get("team", ""))

            jobs.append({
                "Company": company,
                "Job Title": title,
                "Job Link": job_url,
                "Location": location,
                "Posting Date": posting_date,
                "Seniority": "Mid",
                "Description": f"Dept: {dept_hint}" if dept_hint else "",
            })

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Ashby API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Ashby API WARN] {company}: {e}")
        return []


async def extract_bamboohr_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via BambooHR public JSON API (subdomain.bamboohr.com)."""
    try:
        # Extract subdomain from URL: https://solidatus.bamboohr.com/careers → solidatus
        from urllib.parse import urlparse as _up
        host = _up(url).netloc  # e.g. solidatus.bamboohr.com
        subdomain = host.split(".")[0]
        api_url = f"https://{subdomain}.bamboohr.com/careers/list"
        await rate_limiter.wait(api_url)
        r = await client.get(api_url, headers=HEADERS, timeout=20, follow_redirects=False)
        if r.status_code != 200:
            return []

        data = r.json()
        jobs_list = data.get("result", [])
        jobs = []

        for item in jobs_list:
            title = clean_title(item.get("jobOpeningName", ""))
            if not title or not ROLE_RE.search(title):
                continue

            loc_parts = []
            loc = item.get("location", {})
            if isinstance(loc, dict):
                city = loc.get("city", "")
                state = loc.get("state", "")
                if city:
                    loc_parts.append(city)
                if state and state != city:
                    loc_parts.append(state)
            location = clean_location(", ".join(loc_parts))

            job_id = item.get("id", "")
            job_url = f"https://{subdomain}.bamboohr.com/careers/{job_id}" if job_id else ""

            jobs.append({
                "Company": company,
                "Job Title": title,
                "Job Link": job_url,
                "Location": location,
                "Posting Date": "",
                "Seniority": "Mid",
            })

        jobs = dedup_and_cap(jobs, company)
        print(f"  [BambooHR API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [BambooHR API WARN] {company}: {e}")
        return []


async def extract_workable_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via Workable public API (apply.workable.com)."""
    try:
        # Extract slug from URL: https://apply.workable.com/soda-data-nv/ → soda-data-nv
        slug = url.rstrip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
        await rate_limiter.wait(api_url)
        r = await client.post(
            api_url,
            json={"query": "", "location": [], "department": [], "worktype": []},
            headers={**HEADERS, "Content-Type": "application/json"},
            timeout=20,
        )
        if r.status_code != 200:
            return []

        data = r.json()
        jobs_list = data.get("results", [])
        jobs = []

        for item in jobs_list:
            title = clean_title(item.get("title", ""))
            if not title or not ROLE_RE.search(title):
                continue

            # Build location from location object
            loc_obj = item.get("location", {})
            loc_parts = []
            if isinstance(loc_obj, dict):
                city = loc_obj.get("city", "")
                country = loc_obj.get("country", "")
                if city:
                    loc_parts.append(city)
                if country and country != city:
                    loc_parts.append(country)
            location = clean_location(", ".join(loc_parts))

            # Workable shortcode used to build job URL
            shortcode = item.get("shortcode", "")
            job_url = f"https://apply.workable.com/{slug}/j/{shortcode}/" if shortcode else ""

            published = item.get("published", "")
            posting_date = ""
            if published:
                try:
                    posting_date = datetime.fromisoformat(published.split("T")[0]).date().isoformat()
                except Exception:
                    pass

            if is_too_old(posting_date):
                continue

            jobs.append({
                "Company": company,
                "Job Title": title,
                "Job Link": job_url,
                "Location": location,
                "Posting Date": posting_date,
                "Seniority": "Mid",
            })

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Workable API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Workable API WARN] {company}: {e}")
        return []

async def extract_gem_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via Gem job board GraphQL API (jobs.gem.com/{board})."""
    try:
        board_id = url.rstrip("/").split("/")[-1].split("?")[0]
        gql_url = "https://jobs.gem.com/api/public/graphql/batch"
        payload = [{
            "operationName": "JobBoardList",
            "variables": {"boardId": board_id},
            "query": (
                "query JobBoardList($boardId: String!) {"
                "  oatsExternalJobPostings(boardId: $boardId) {"
                "    jobPostings {"
                "      id extId title"
                "      locations { name city isoCountry isRemote }"
                "      job { department { name } locationType employmentType }"
                "    }"
                "  }"
                "}"
            ),
        }]
        await rate_limiter.wait(gql_url)
        r = await client.post(
            gql_url,
            json=payload,
            headers={**HEADERS, "Content-Type": "application/json",
                     "Origin": "https://jobs.gem.com",
                     "Referer": f"https://jobs.gem.com/{board_id}"},
            timeout=20,
        )
        if r.status_code != 200:
            return []

        resp = r.json()
        postings = resp[0]["data"]["oatsExternalJobPostings"]["jobPostings"]
        jobs = []

        for item in postings:
            title = clean_title(item.get("title", ""))
            if not title or not ROLE_RE.search(title):
                continue

            loc_parts = []
            for loc in item.get("locations", []):
                city = loc.get("city", "") or loc.get("name", "")
                country = loc.get("isoCountry", "")
                if loc.get("isRemote"):
                    loc_parts.append("Remote")
                    break
                if city:
                    loc_parts.append(city)
                if country and country not in loc_parts:
                    loc_parts.append(country)
                break  # use first location only
            location = clean_location(", ".join(loc_parts))

            ext_id = item.get("extId", item.get("id", ""))
            job_url = f"https://jobs.gem.com/{board_id}/jobs/{ext_id}" if ext_id else ""

            jobs.append({
                "Company": company,
                "Job Title": title,
                "Job Link": job_url,
                "Location": location,
                "Posting Date": "",
                "Seniority": "Mid",
            })

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Gem API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Gem API WARN] {company}: {e}")
        return []


async def extract_join_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via join.com (Next.js SPA, data in __NEXT_DATA__)."""
    try:
        slug = url.rstrip("/").split("/")[-1].split("?")[0]
        jobs = []
        page = 1

        while True:
            page_url = f"https://join.com/companies/{slug}?page={page}"
            await rate_limiter.wait(page_url)
            r = await client.get(page_url, headers=HEADERS, timeout=20, follow_redirects=True)
            if r.status_code != 200:
                break

            # Jobs are embedded in __NEXT_DATA__ JSON
            match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if not match:
                break

            data = json.loads(match.group(1))
            jobs_state = data["props"]["pageProps"]["initialState"]["jobs"]
            items = jobs_state.get("items", [])
            pagination = jobs_state.get("pagination", {})

            if not items:
                break

            company_id = data["props"]["pageProps"]["initialState"]["company"].get("id", "")

            for item in items:
                title = clean_title(item.get("title", ""))
                if not title or not ROLE_RE.search(title):
                    continue

                city_obj = item.get("city") or {}
                country_obj = item.get("country") or {}
                loc_parts = []
                city_name = city_obj.get("cityName", "") if isinstance(city_obj, dict) else ""
                country_name = country_obj.get("countryName", "") if isinstance(country_obj, dict) else ""
                if city_name:
                    loc_parts.append(city_name)
                if country_name and country_name != city_name:
                    loc_parts.append(country_name)
                location = clean_location(", ".join(loc_parts))

                created_at = item.get("createdAt", "")
                posting_date = ""
                if created_at:
                    try:
                        posting_date = datetime.fromisoformat(created_at.split("T")[0]).date().isoformat()
                    except Exception:
                        pass

                if is_too_old(posting_date):
                    continue

                job_id = item.get("id", "")
                id_param = item.get("idParam", job_id)
                job_url = f"https://join.com/companies/{slug}/jobs/{id_param}" if id_param else ""

                jobs.append({
                    "Company": company,
                    "Job Title": title,
                    "Job Link": job_url,
                    "Location": location,
                    "Posting Date": posting_date,
                    "Seniority": "Mid",
                })

            total_pages = pagination.get("pageCount", 1)
            if page >= total_pages:
                break
            page += 1

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Join.com] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Join.com WARN] {company}: {e}")
        return []


async def extract_smartrecruiters_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via SmartRecruiters public API (jobs.smartrecruiters.com/{slug})."""
    try:
        # Extract company slug from URL: jobs.smartrecruiters.com/Collibra → Collibra
        slug = url.rstrip("/").split("/")[-1].split("?")[0]
        jobs = []
        offset = 0
        limit = 100

        while True:
            api_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?status=PUBLIC&offset={offset}&limit={limit}"
            await rate_limiter.wait(api_url)
            r = await client.get(api_url, headers=HEADERS, timeout=20, follow_redirects=True)
            if r.status_code != 200:
                break

            data = r.json()
            postings = data.get("content", [])
            if not postings:
                break

            for item in postings:
                title = clean_title(item.get("name", ""))
                if not title or not ROLE_RE.search(title):
                    continue

                loc_obj = item.get("location", {})
                loc_parts = []
                city = loc_obj.get("city", "")
                country = loc_obj.get("country", "")
                if city:
                    loc_parts.append(city)
                if country and country != city:
                    loc_parts.append(country)
                location = clean_location(", ".join(loc_parts))

                released = item.get("releasedDate", "")
                posting_date = ""
                if released:
                    try:
                        posting_date = datetime.fromisoformat(released.split("T")[0]).date().isoformat()
                    except Exception:
                        pass

                if is_too_old(posting_date):
                    continue

                job_url = item.get("ref", f"https://jobs.smartrecruiters.com/{slug}/{item.get('id', '')}")

                jobs.append({
                    "Company": company,
                    "Job Title": title,
                    "Job Link": job_url,
                    "Location": location,
                    "Posting Date": posting_date,
                    "Seniority": "Mid",
                })

            total_found = data.get("totalFound", 0)
            offset += limit
            if offset >= total_found or offset >= MAX_JOBS_PER_COMPANY:
                break

        jobs = dedup_and_cap(jobs, company)
        print(f"  [SmartRecruiters API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [SmartRecruiters API WARN] {company}: {e}")
        return []


async def extract_datadog_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs from Datadog's Typesense-powered career site.

    Datadog embeds a Typesense search index with a public read-only key.
    We page through all results using the documented search API.
    """
    TYPESENSE_HOST = "gk6e3zbyuntvc5dap"
    TYPESENSE_KEY = "1Hwq7hntXp211hKvRS3CSI2QSU7w2gFm"
    COLLECTION = "careers_alias"
    PER_PAGE = 100

    try:
        jobs = []
        page = 1
        total_found = None

        while True:
            api_url = (
                f"https://{TYPESENSE_HOST}.a1.typesense.net"
                f"/collections/{COLLECTION}/documents/search"
                f"?q=*&query_by=title&per_page={PER_PAGE}&page={page}&filter_by=language:en"
            )
            await rate_limiter.wait(api_url)
            r = await client.get(
                api_url,
                headers={**HEADERS, "x-typesense-api-key": TYPESENSE_KEY},
                timeout=20,
            )
            if r.status_code != 200:
                print(f"  [Datadog Typesense] HTTP {r.status_code}")
                break

            data = r.json()
            if total_found is None:
                total_found = data.get("found", 0)

            hits = data.get("hits", [])
            if not hits:
                break

            for hit in hits:
                doc = hit.get("document", {})
                title = clean_title(doc.get("title", ""))
                if not title or not ROLE_RE.search(title):
                    continue

                location = clean_location(doc.get("location_string", ""))
                job_url = doc.get("absolute_url", "")

                # last_mod is ISO datetime e.g. "2026-03-31T17:08:56-04:00"
                posting_date = ""
                last_mod = doc.get("last_mod", "")
                if last_mod:
                    try:
                        posting_date = datetime.fromisoformat(last_mod).date().isoformat()
                    except Exception:
                        posting_date = last_mod[:10]

                if is_too_old(posting_date):
                    continue

                jobs.append({
                    "Company": company,
                    "Job Title": title,
                    "Job Link": job_url,
                    "Location": location,
                    "Posting Date": posting_date,
                    "Seniority": "Mid",
                })

            page += 1
            if len(jobs) >= MAX_JOBS_PER_COMPANY or (page - 1) * PER_PAGE >= (total_found or 0):
                break

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Datadog Typesense] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Datadog Typesense WARN] {company}: {e}")
        return []


async def extract_workday_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs via Workday CXS public API (myworkdayjobs.com).

    Converts a standard Workday URL like:
      https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers
    into the CXS API call:
      POST https://alteryx.wd108.myworkdayjobs.com/wday/cxs/alteryx/AlteryxCareers/jobs
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.netloc  # e.g. alteryx.wd108.myworkdayjobs.com
        company_slug = hostname.split(".")[0]  # e.g. alteryx
        board = parsed.path.strip("/").split("/")[0]  # e.g. AlteryxCareers
        if not board:
            return []

        api_base = f"https://{hostname}/wday/cxs/{company_slug}/{board}/jobs"
        jobs = []
        offset = 0
        limit = 20

        while True:
            payload = {
                "appliedFacets": {},
                "limit": limit,
                "offset": offset,
                "searchText": "",
            }
            await rate_limiter.wait(api_base)
            r = await client.post(
                api_base,
                json=payload,
                headers={**HEADERS, "Content-Type": "application/json"},
                timeout=20,
            )
            if r.status_code != 200:
                print(f"  [Workday API] {company}: HTTP {r.status_code}")
                break

            data = r.json()
            postings = data.get("jobPostings", [])
            if not postings:
                break

            for item in postings:
                title = clean_title(item.get("title", ""))
                if not title or not ROLE_RE.search(title):
                    continue

                # Workday returns locationsText like "3 Locations" or a city name
                location = clean_location(item.get("locationsText", "") or item.get("primaryLocation", ""))

                # Build full job URL
                ext_url = item.get("externalPath", "")
                job_url = f"https://{hostname}{ext_url}" if ext_url else ""

                # Workday dates in postedOn like "Posted 30+ Days Ago" — no exact date
                posting_date = ""
                posted_on = item.get("postedOn", "")
                if "Today" in posted_on or "1 Day" in posted_on:
                    posting_date = date.today().isoformat()

                jobs.append({
                    "Company": company,
                    "Job Title": title,
                    "Job Link": job_url,
                    "Location": location,
                    "Posting Date": posting_date,
                    "Seniority": "Mid",
                })

            total = data.get("total", 0)
            offset += limit
            if offset >= total or offset >= MAX_JOBS_PER_COMPANY:
                break

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Workday API] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Workday API WARN] {company}: {e}")
        return []


async def extract_phenom_jobs(
    client: httpx.AsyncClient, company: str, url: str
) -> list[dict]:
    """Extract jobs from Phenom People career sites (e.g. careers.salesforce.com).

    Phenom renders server-side paginated HTML. Paginates through ?page=N until
    no more job links are found. Works for any Phenom-powered site.
    """
    try:
        base_url = url.rstrip("/").split("?")[0].split("#")[0]
        jobs = []
        page = 1
        base_domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

        while len(jobs) < MAX_JOBS_PER_COMPANY:
            page_url = f"{base_url}?page={page}"
            await rate_limiter.wait(page_url)
            r = await client.get(page_url, headers=HEADERS, timeout=20, follow_redirects=True)
            if r.status_code != 200:
                break

            soup = BeautifulSoup(r.text, "lxml")

            # Salesforce: links like /en/jobs/jr{id}/{slug}/
            job_links = [
                (a.get_text(strip=True), a["href"])
                for a in soup.find_all("a", href=True)
                if re.match(r"/\w+/jobs?/\w+\d+/", a["href"])
            ]

            if not job_links:
                break  # No more jobs — stop pagination

            for raw_title, href in job_links:
                title = clean_title(raw_title)
                if not title or not ROLE_RE.search(title):
                    continue

                # Find location from card container
                a_tag = soup.find("a", href=href)
                location = ""
                if a_tag:
                    card = a_tag.find_parent(["li", "div", "article"])
                    if card:
                        loc_el = card.find(class_=re.compile(r"location|loc-", re.I))
                        if loc_el:
                            location = clean_location(loc_el.get_text(strip=True))

                job_url = href if href.startswith("http") else f"{base_domain}{href}"

                # Fetch detail page to extract posting date
                posting_date = ""
                try:
                    await rate_limiter.wait(job_url)
                    detail_r = await client.get(job_url, headers=HEADERS, timeout=20, follow_redirects=True)
                    if detail_r.status_code == 200:
                        posting_date = extract_date(detail_r.text)
                except Exception:
                    pass

                # Skip jobs older than MAX_JOB_AGE_DAYS
                if is_too_old(posting_date):
                    continue

                jobs.append({
                    "Company": company,
                    "Job Title": title,
                    "Job Link": job_url,
                    "Location": location,
                    "Posting Date": posting_date,
                    "Seniority": "Mid",
                })

            page += 1

        jobs = dedup_and_cap(jobs, company)
        print(f"  [Phenom Pages] {len(jobs)} jobs extracted for {company}")
        return jobs
    except Exception as e:
        print(f"  [Phenom Pages WARN] {company}: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════
# CORE EXTRACTION — SINGLE PATH
# ══════════════════════════════════════════════════════════════════════════

async def extract_jobs_from_page(
    client: httpx.AsyncClient,
    company: str,
    main_url: str,
    html: str,
) -> list[dict]:
    """Extract job listings from a career page. Single extraction path."""
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    seen_hrefs: set[str] = set()
    candidates: list[dict] = []

    # ── Collect from structured job-card containers first ────────────────
    for el in soup.select(
        "[data-job], .job, .job-listing, .job-card, .opening, "
        ".position, .posting, .role, .job-row, "
        "[class*='job-card'], [class*='JobCard'], [class*='posting']"
    ):
        a = el.find("a", href=True)
        if not a:
            continue
        href = normalize_url(main_url, a.get("href", ""))
        if not href or href in seen_hrefs or href.rstrip("/") == main_url.rstrip("/"):
            continue
        text = a.get_text(" ", strip=True) or el.get_text(" ", strip=True)
        if is_job_url(href, text):
            seen_hrefs.add(href)
            candidates.append({"href": href, "text": text, "card": el})

    # ── Fallback: scan all anchors ──────────────────────────────────────
    for a in soup.find_all("a", href=True):
        href = normalize_url(main_url, a.get("href", ""))
        if not href or href in seen_hrefs or href.rstrip("/") == main_url.rstrip("/"):
            continue
        text = a.get_text(" ", strip=True)
        if is_job_url(href, text):
            seen_hrefs.add(href)
            card = a.find_parent(
                class_=re.compile(r'job|card|listing|posting|opening|role|position', re.I)
            )
            candidates.append({"href": href, "text": text, "card": card})

    if not candidates:
        print(f"  [WARN] No candidates found for {company}")
        return []

    print(f"  [{company}] {len(candidates)} candidates found")

    # ── Fetch detail pages concurrently (max 5) ─────────────────────────
    sem = asyncio.Semaphore(5)

    async def process_candidate(cand: dict) -> dict | None:
        async with sem:
            href = cand["href"]
            anchor_text = cand["text"]
            card = cand["card"]

            # Extract location from anchor text before cleaning
            loc_from_text = clean_location(extract_location_from_title(anchor_text))

            # Try location from card context
            loc_from_card = ""
            if card:
                for sel in [".location", ".job-location", "[class*='location']"]:
                    try:
                        loc_el = card.select_one(sel)
                        if loc_el:
                            loc_from_card = clean_location(loc_el.get_text(" ", strip=True))
                            if loc_from_card:
                                break
                    except Exception:
                        pass

            # Fetch detail page
            detail_html = await fetch_html(client, href)
            if not detail_html:
                title = clean_title(anchor_text)
                if not title or not ROLE_RE.search(title) or FORBIDDEN_TEXT_RE.search(title):
                    return None
                return {
                    "Company": company,
                    "Job Title": title,
                    "Job Link": href,
                    "Location": loc_from_text or loc_from_card,
                    "Posting Date": "",
                    "Seniority": "Mid",
                }

            detail_soup = BeautifulSoup(detail_html, "lxml")

            # Workday-specific title extraction
            title = ""
            if 'myworkdayjobs.com' in href:
                for sel in ['[data-automation-id="jobPostingTitle"]', '.css-1q2dra3', 'h2[data-automation-id]']:
                    title_el = detail_soup.select_one(sel)
                    if title_el:
                        title = clean_title(title_el.get_text(" ", strip=True))
                        if title:
                            break

            # Title: prefer <h1> from detail page, reject generic headers
            if not title:
                h1 = detail_soup.find("h1")
                if h1:
                    t = clean_title(h1.get_text(" ", strip=True))
                    if t and not re.search(r'career|jobs at|work at|join us|open positions', t, re.I):
                        title = t

            if not title:
                title = clean_title(anchor_text)

            # Check title validity and forbidden content
            if not title or not ROLE_RE.search(title) or FORBIDDEN_TEXT_RE.search(title):
                return None

            location = extract_location(detail_soup) or loc_from_text or loc_from_card
            posting_date = extract_date(detail_html)

            # Skip jobs older than MAX_JOB_AGE_DAYS
            if is_too_old(posting_date):
                return None

            return {
                "Company": company,
                "Job Title": title,
                "Job Link": href,
                "Location": location,
                "Posting Date": posting_date,
                "Seniority": "Mid",
            }

    # Cap at 300 candidates per company to avoid runaway scrapes
    tasks = [process_candidate(c) for c in candidates[:300]]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r]

# ══════════════════════════════════════════════════════════════════════════
# MAIN SCRAPE LOOP
# ══════════════════════════════════════════════════════════════════════════

async def scrape_all(competitors_path: str = "data/competitors.csv") -> list[dict]:
    competitors = load_competitors(competitors_path)
    conn = init_db()
    prune_old_jobs(conn)
    today = date.today().isoformat()
    all_jobs: list[dict] = []

    # Shared Playwright browser for all JS-heavy sites
    pw_context = None
    pw_browser = None
    need_pw = any(needs_playwright(c["Company"], c["Career_URL"]) for c in competitors
                  if "lever.co/" not in c["Career_URL"]
                  and "greenhouse.io/" not in c["Career_URL"]
                  and "ashbyhq.com/" not in c["Career_URL"]
                  and "bamboohr.com" not in c["Career_URL"]
                  and "workable.com" not in c["Career_URL"]
                  and "join.com/companies/" not in c["Career_URL"])

    if need_pw and PLAYWRIGHT_AVAILABLE:
        print("[PLAYWRIGHT] Launching shared browser...")
        pw_context = await async_playwright().start()
        pw_browser = await pw_context.chromium.launch(headless=True, args=["--no-sandbox"])

    try:
        limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
        async with httpx.AsyncClient(limits=limits, timeout=25) as client:
            for comp in competitors:
                company = comp["Company"]
                url = comp["Career_URL"]
                print(f"\n[SCRAPING] {company} → {url}")

                jobs: list[dict] = []

                # Dispatch to API handlers for known ATS systems
                if "lever.co/" in url:
                    jobs = await extract_lever_jobs(client, company, url)
                elif "greenhouse.io/" in url:
                    jobs = await extract_greenhouse_jobs(client, company, url)
                elif "ashbyhq.com/" in url:
                    jobs = await extract_ashby_jobs(client, company, url)
                elif "bamboohr.com" in url:
                    jobs = await extract_bamboohr_jobs(client, company, url)
                elif "workable.com" in url:
                    jobs = await extract_workable_jobs(client, company, url)
                elif "smartrecruiters.com" in url:
                    jobs = await extract_smartrecruiters_jobs(client, company, url)
                elif "join.com/companies/" in url:
                    jobs = await extract_join_jobs(client, company, url)
                elif "jobs.gem.com/" in url:
                    jobs = await extract_gem_jobs(client, company, url)
                elif "datadoghq.com" in url:
                    jobs = await extract_datadog_jobs(client, company, url)
                elif "myworkdayjobs.com" in url:
                    jobs = await extract_workday_jobs(client, company, url)
                elif "careers.salesforce.com" in url:
                    jobs = await extract_phenom_jobs(client, company, url)
                else:
                    # HTML scraping fallback
                    if needs_playwright(company, url):
                        html = await fetch_with_playwright(url, browser=pw_browser)
                    else:
                        html = await fetch_html(client, url)

                    if not html:
                        print(f"  [SKIP] No HTML for {company}")
                        continue

                    jobs = await extract_jobs_from_page(client, company, url, html)

                print(f"  [{company}] {len(jobs)} valid jobs extracted")

                for job in jobs:
                    is_new, first_seen = upsert_job(conn, job)
                    job["First_Seen"] = first_seen
                    job["Last_Seen"] = today
                    all_jobs.append(job)
    finally:
        if pw_browser:
            await pw_browser.close()
        if pw_context:
            await pw_context.stop()

    conn.close()
    return all_jobs


def write_csv(jobs: list[dict], path: str = "data/jobs_raw.csv") -> int:
    fieldnames = [
        "Company", "Job Title", "Job Link", "Location",
        "Posting Date", "Seniority", "First_Seen", "Last_Seen", "Description",
    ]

    # Deduplicate by Job Link — prefer row with more data
    dedup: dict[str, dict] = {}
    for job in jobs:
        link = job.get("Job Link", "")
        if link not in dedup or (not dedup[link].get("Posting Date") and job.get("Posting Date")):
            dedup[link] = job

    # Filter out jobs older than MAX_JOB_AGE_DAYS
    rows = [r for r in dedup.values() if not is_too_old(r.get("Posting Date", ""))]
    rows = sorted(rows, key=lambda x: (x.get("Company", ""), x.get("Job Title", "")))

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"\n{'='*60}")
    print(f"[OK] Wrote {len(rows)} jobs → {path}")
    print(f"{'='*60}")
    return len(rows)


if __name__ == "__main__":
    jobs = asyncio.run(scrape_all())
    write_csv(jobs)
