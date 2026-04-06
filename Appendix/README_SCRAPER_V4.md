# Scraper v4 - Quick Reference

**File**: `/sessions/confident-wonderful-bohr/mnt/outputs/scraper.py`  
**Lines**: 743  
**Status**: Production-ready, syntax validated  
**Python**: 3.10+

## What Changed From v3

1. **API Support**: Direct JSON extraction for Lever and Greenhouse ATS platforms
2. **Better Title Cleaning**: 5 additional patterns for messy location codes
3. **Location Extraction**: Recovers location from title before cleaning (fallback)
4. **Content Filtering**: FORBIDDEN_TEXT_RE blocks demo, webinar, blog links
5. **Workday Support**: Specific CSS selectors for Workday job posting titles
6. **Playwright Domains**: Added ashbyhq, onetrust, atlassian, microsoft, google

## Key Functions

### Clean Title - Line 218
```python
def clean_title(raw: str) -> str:
```
Strips Workday codes, MongoDB suitcase/location/chevron, bullets, locations.

### Extract Location from Title - Line 183
```python
def extract_location_from_title(raw: str) -> str:
```
NEW: Extracts "Chennai, India" from "Title IND-CHENNAI • Hybrid"

### Lever API - Line 424
```python
async def extract_lever_jobs(client, company, url) -> list[dict]:
```
NEW: Direct JSON from `https://api.lever.co/v0/postings/{slug}?mode=json`

### Greenhouse API - Line 475
```python
async def extract_greenhouse_jobs(client, company, url) -> list[dict]:
```
NEW: Direct JSON from `https://boards-api.greenhouse.io/v1/boards/{board}/jobs`

## Usage

Run the same way as before:
```bash
python scraper.py
```

Or import:
```python
from scraper import scrape_all, write_csv
jobs = asyncio.run(scrape_all("competitors.csv"))
write_csv(jobs, "jobs_raw.csv")
```

## Output Format (Unchanged)

CSV with columns:
- Company
- Job Title
- Job Link
- Location
- Posting Date (YYYY-MM-DD)
- Seniority (Director+, Principal/Staff, Senior, Manager, Mid, Entry, Intern, Unknown)
- First_Seen (YYYY-MM-DD)
- Last_Seen (YYYY-MM-DD)

## Configuration

Edit top of file:
- `PLAYWRIGHT_DOMAINS`: Companies needing Playwright
- `PLAYWRIGHT_URL_DOMAINS`: URL patterns needing Playwright
- `ATS_PATTERNS`: Patterns for job URLs
- `ROLE_RE`: What counts as a job title
- `FORBIDDEN_URL_RE`: Pages to skip (about, blog, pricing, etc.)
- `FORBIDDEN_TEXT_RE`: Link text to skip (demo, webinar, webinar, etc.)

## Fallback Chain for Location

1. Try to extract from detail page (HTML)
2. Fall back to extracted from raw title before cleaning
3. Fall back to location found in job card context
4. Default to empty string

## Fallback Chain for Title

1. Try Workday-specific selectors (if myworkdayjobs.com)
2. Try `<h1>` from detail page
3. Fall back to anchor text + clean_title()
4. Validate with ROLE_RE

## Rate Limiting

- Per-domain, 1 second minimum between requests
- 20 max connections, 10 keep-alive
- Retries with exponential backoff on 429

## Database

SQLite `seen_jobs.db` tracks:
- url_hash (primary key)
- job_url
- company
- title
- first_seen (YYYY-MM-DD)
- last_seen (YYYY-MM-DD)

Deduplication across runs prevents duplicate CSV rows.

## Performance

- 5 concurrent detail page fetches per company
- Capped at 300 candidates per company
- Lever/Greenhouse API calls replace HTML scraping (much faster)

