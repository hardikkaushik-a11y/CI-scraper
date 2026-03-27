# Actian Competitive Hiring Intelligence Pipeline

Real-time competitive intelligence platform that scrapes job postings from 64 competitor companies, enriches them with Claude AI analysis, and provides an interactive executive dashboard with AI-powered insights.

## Overview

This pipeline automates competitor hiring analysis to help Actian identify strategic threats, understand competitor GTM expansion, detect AI/ML investment trends, and benchmark against industry peers.

**Current Coverage:** 34+ companies scraping | 1,198+ jobs enriched | 18 companies with threat signals

## Architecture

### 1. **Scraper** (`scraper.py`)
- Async job scraping from multiple ATS platforms (Greenhouse, Lever, Ashby, BambooHR, Workable)
- Per-domain rate limiting, SQLite dedup, 365-day rolling window
- Title deduplication (max 3 per company), 200-job hard cap per company
- Fallback to Playwright for JS-heavy sites

**ATS Coverage:**
- Greenhouse API (9 companies)
- Lever API (4 companies)
- Ashby API (7 companies)
- BambooHR API (1 company)
- Workable API (2 companies)
- Playwright HTML scraping (~12 companies)

### 2. **Enricher** (`enrich.py`)
- Claude Haiku job classification (title + description → function, product focus, seniority)
- Claude Sonnet strategic signals (company-level threat scoring, implications, hiring patterns)
- Skill extraction (89.7% coverage, 150+ skills detected)
- Relevancy scoring (skills + product + seniority + company group)
- "Other" elimination—all jobs classified into valid categories

**Output:**
- `jobs_enriched.csv` — 1,198 enriched jobs with Function, Product_Focus, Relevancy, Trend, Skills
- `signals.json` — 18 strategic signals with threat levels, implications, hiring intensity

### 3. **Dashboard** (`dashboard.html`)
- Single-file interactive frontend (no build step)
- Two pages: Market Pulse (KPIs + charts) and Strategic Signals (threat analysis)
- Interactive chart clicking (filter by company, seniority, function, product focus)
- Actian brand colors (red #E31937, navy #1B2A4A, cyan #00B4D8)
- Legal/People/HR roles excluded
- Vector DB / AI companies flagged as strategic priority

### 4. **AI Assistant** (`assistant_backend.py`)
- Flask backend with Claude Sonnet for dynamic Q&A
- Reasoning from live competitive data (1,198 jobs + 18 signals)
- Suggested prompts auto-generated from data
- Custom Query flow (3-step guided analysis)
- Session-based dashboard filtering and highlighting
- Exponential backoff retry logic (429/529 errors)

## Quick Start

### Setup

```bash
# Clone repo
git clone https://github.com/hardikkaushik-a11y/CI-scraper.git
cd CI-scraper

# Install dependencies
pip install -r requirements.txt

# Set Anthropic API key
export ANTHROPIC_API_KEY=sk-ant-...
```

### Run Scraper & Enricher

```bash
# Scrape jobs from all 64 competitors
python scraper.py

# Enrich with Claude signals
python enrich.py

# Output:
# - jobs_raw.csv (raw scraper output)
# - jobs_enriched.csv (enriched, classified, with relevancy scores)
# - signals.json (strategic signals with threat levels)
```

### Start Dashboard + Assistant

```bash
# Terminal 1: Backend (Claude AI assistant)
python assistant_backend.py
# Listening on http://localhost:5001

# Terminal 2: Serve dashboard
python -m http.server 8080
# Open: http://localhost:8080/dashboard.html
```

## Key Features

### Dashboard Intelligence

- **Market Pulse:** KPIs for hiring velocity, AI investment race, threat index, seniority signal, GTM surge
- **Strategic Signals:** Top 5 threat companies with implications, hiring patterns, recommendations
- **Interactive Charts:** Click to filter; supports company, seniority, function, product focus
- **Threat Scoring:** Based on company group, product focus overlap, hiring intensity, seniority ratio, AI investment
- **Relevancy Scoring:** Skills detected + product focus + seniority + company group + geography
- **Vector DB Priority:** Pinecone, Weaviate, Qdrant, Zilliz flagged with special callout (Actian launching vector product)

### AI Assistant

- **Smart Prompts:** "What's Collate hiring for?", "Show AI/ML job trends", "Which competitors are scaling cloud?"
- **Custom Queries:** Multi-step dialog to understand analysis intent
- **Dashboard Control:** Filter table, highlight sections, apply time filters (session-only)
- **Info Guide:** (i) button explains dashboard structure, metrics, data sources

## Data Model

### jobs_enriched.csv

| Field | Values | Notes |
|-------|--------|-------|
| Company | 34 companies | Dropdown filter on dashboard |
| Job Title | 1,198 | Cleaned, de-duplicated |
| Function | Engineering, Sales, Marketing, etc. | Legal/HR excluded |
| Product_Focus | Platform/Infra, ML/AI, Cloud, etc. | Never "Other" |
| Seniority | Entry, Mid, Senior, Manager, Director, Principal/Staff | |
| Location | City, State, Country | Cleaned |
| Skills | PYTHON, SQL, KUBERNETES, etc. | 89.7% extraction rate |
| Relevancy | 0-17.5 | Job importance to Actian |
| Trend | Y/N | Hiring pattern signal |
| Posting Date | YYYY-MM-DD | 365-day window |

### signals.json

| Field | Meaning |
|-------|---------|
| threat_level | low / medium / high / critical |
| threat_score | 0-100 | weighted points system |
| hiring_intensity | # of jobs / company size |
| dominant_function | Most common job function |
| dominant_product_focus | Most common product focus |
| seniority_ratio | % Senior+ hires |
| signal_summary | One-line inference (e.g., "Heavy cloud platform investment") |
| implications | 5-6 bullets explaining hiring strategy |

## Configuration

### competitors.csv

Add/update competitor job board URLs:

```csv
Company,Career_URL,Company_Group
Snowflake,https://jobs.ashbyhq.com/snowflake,Warehouse/Processing
```

Supported ATS:
- `boards.greenhouse.io/` (Greenhouse)
- `lever.co/` (Lever)
- `ashbyhq.com/` (Ashby)
- `bamboohr.com` (BambooHR)
- `workable.com` (Workable)
- Custom URLs auto-fallback to Playwright

### Threat Scoring Logic

```
Threat Score = Group Proximity (0-3)
             + Product Focus Overlap (0-3)
             + Hiring Intensity (0-2)
             + Seniority Ratio (0-2)
             + AI Investment Bonus (0-2)
             + Vector DB Bonus (if applicable)
```

### Relevancy Scoring Logic

```
Relevancy = Base Score
          + Skills Match (0-5)
          + Product Focus (0-3)
          + Seniority Weight (0-3)
          + Company Group (0-2)
          + Geography (0-2)
```

## Maintenance

### Daily Refresh

GitHub Actions workflow (`scrape.yml`) runs at 06:00 UTC:
- Scrapes all competitors
- Enriches with Claude signals
- Commits results back to repo

### Monitoring

Check dashboard for:
- Companies with 0 jobs (may need URL update)
- Sudden spikes in hiring (threat signal)
- Vector DB / AI hiring trends (strategic priority)
- Function outliers (Legal/HR still leaking?)

### Common Issues

| Issue | Fix |
|-------|-----|
| "0 jobs for Company X" | Check Career_URL in competitors.csv, may need ATS slug update |
| Skills not extracted | Expand SKILL_MAP and patterns in enrich.py |
| Assistant backend 500 error | Check ANTHROPIC_API_KEY env var, retry with exponential backoff |
| Dashboard won't load CSV | Ensure HTTP server running, not `file://` protocol |

## Next Steps

- [ ] Add Workday API extractor (3 companies currently Playwright)
- [ ] Add SmartRecruiters API extractor (Collibra)
- [ ] Expand to ~50+ companies (currently 34)
- [ ] Power BI migration (optional long-term)
- [ ] Custom report generation (PDF export)
- [ ] Slack/email alerts for threat level changes

## Tech Stack

- **Scraper:** Python, httpx (async), Playwright (JS-heavy sites), SQLite (dedup)
- **Enricher:** Python, Claude API (Haiku + Sonnet)
- **Dashboard:** HTML5, Chart.js, vanilla JS (no build step)
- **Assistant:** Flask, Claude Sonnet API
- **CI/CD:** GitHub Actions (daily 06:00 UTC)

## Files

```
.
├── scraper.py              # Job scraping engine
├── enrich.py               # Claude enrichment + signals
├── assistant_backend.py    # Flask + Sonnet backend
├── dashboard.html          # Interactive frontend
├── competitors.csv         # Competitor job board URLs
├── scrape.yml              # GitHub Actions workflow
├── requirements.txt        # Python dependencies
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

## API Keys

Requires `ANTHROPIC_API_KEY` env var (set in `.env` or terminal):

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

No public APIs require authentication (Greenhouse, Lever, Ashby are public).

## License

Internal use only (Actian).

## Questions?

Check the (i) button on the dashboard for a guided tour, or ask the AI assistant any question about the data.
