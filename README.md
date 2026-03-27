# Actian Competitive Intelligence Pipeline

A real-time competitive hiring intelligence system for Actian that scrapes job postings from 64+ competitor companies, enriches them with AI-powered strategic signals, and presents insights via an interactive executive dashboard with AI assistant.

## What It Does

**Scraper** — Pulls job postings from 34 companies using native ATS APIs (Greenhouse, Lever, Ashby, BambooHR, Workable) plus Playwright fallback for custom career pages. Deduplicates, filters for relevancy, and applies 365-day rolling window.

**Enricher** — Classifies jobs by function and product focus, detects seniority, extracts 150+ skills (89.7% coverage), calculates relevancy scores, and generates strategic threat signals using Claude.

**Dashboard** — Interactive web interface with 5 KPIs, 4 filterable charts, signal cards with threat levels, company drill-downs, and executive summaries. Actian-branded with red/navy/cyan color scheme.

**AI Assistant** — Floating chat widget with Claude Sonnet backend that:
- Answers questions about competitive hiring patterns
- Generates dynamic suggested prompts from real data
- Guides custom analysis through clarifying questions
- Highlights relevant dashboard sections in real-time

## Architecture

```
scraper.py (async httpx + Playwright)
    ↓
jobs_raw.csv
    ↓
enrich.py (Claude Haiku classification + Sonnet signals)
    ↓
jobs_enriched.csv → dashboard.html (Chart.js frontend)
signals.json ──→ /
                 └→ assistant_backend.py (Flask + Claude Sonnet)
```

## Setup

### Requirements
- Python 3.9+
- Anthropic API key (`ANTHROPIC_API_KEY` env var)
- Chrome/Chromium for Playwright

### Install

```bash
pip install -r requirements.txt
```

### Environment

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

### Run Scraper

```bash
python scraper.py
```

Outputs: `jobs_raw.csv`, `seen_jobs.db`

### Run Enricher

```bash
python enrich.py
```

Outputs: `jobs_enriched.csv`, `signals.json`

### Start AI Assistant Backend

```bash
python assistant_backend.py
```

Server runs on `http://localhost:5001`

### View Dashboard

```bash
python -m http.server 8080
# Open: http://localhost:8080/dashboard.html
```

## Dashboard Features

- **Market Pulse**: 5 KPIs (total roles, new this week, avg seniority, top threat, AI investment)
- **Charts**: Companies, Seniority, Function, Product Focus (all clickable to filter)
- **Signals Page**: Top 5 threat cards, company drill-down, Market Pulse insights, full threat table
- **AI Assistant**: (?) info button for help, (✦) floating chat bubble for dynamic Q&A
- **Filters**: Company, segment, function, seniority, product focus
- **Export**: CSV download of current filtered view

## Key Metrics

- **Threat Level**: Weighted score based on company group, product focus, hiring intensity, seniority ratio, AI investment
- **Relevancy**: Job importance to Actian (0-17.5 scale) based on skills, product, seniority, company group
- **Trend**: 7-day or 30-day hiring momentum (trending up/down/flat)
- **Signals**: 11 strategic categories (AI/ML, Cloud Scale, Security, GTM, Engineering, etc.)

## Company Coverage

- **Total**: 64 competitors across 8 groups
- **Scraped**: 34 companies (~1,200 roles)
- **Signal-Eligible**: 18 companies (3+ postings)
- **Critical Threat**: Collate (direct threat to Actian)
- **High Threat**: Databricks, MongoDB, Snowflake (hiring intensity)
- **Strategic Watch**: Pinecone, Weaviate, Qdrant, Zilliz (Vector DB)

## File Structure

```
scraper.py           — Job scraping engine
enrich.py            — Enrichment & signal generation
assistant_backend.py — Flask server for AI chat
dashboard.html       — Interactive frontend
competitors.csv      — Company list & career URLs
requirements.txt     — Python dependencies
scrape.yml           — GitHub Actions workflow (daily 06:00 UTC)
.gitignore           — Git exclusions
```

## GitHub Actions

Daily scheduled scrape at **06:00 UTC**. Commits results to `jobs_raw.csv` and `jobs_enriched.csv`.

## Future Enhancements

- Power BI migration for enterprise dashboarding
- Workday API extractor for 10+ additional companies
- SmartRecruiters integration for Collibra
- Deep-dive analysis dashboards per company group
- Win/loss analysis module
- GTM competitive benchmarking

## License

Internal use only — Actian Corporation

---

**Built with**: Python, httpx, Playwright, Claude API, Chart.js, Flask
