# Actian Competitive Intelligence Pipeline

A real-time competitive hiring intelligence system for Actian that scrapes job postings from 64+ competitor companies, enriches them with AI-powered strategic signals, and presents insights via interactive dashboards (executive & team-specific) with AI assistant.

## What It Does

**Scraper** (`src/scraper.py`) — Pulls job postings from 34 companies using native ATS APIs (Greenhouse, Lever, Ashby, BambooHR, Workable) plus Playwright fallback for custom career pages. Deduplicates, filters for relevancy, and applies 365-day rolling window.

**Enricher** (`src/enrich.py`) — Classifies jobs by function and product focus, detects seniority, extracts 150+ skills (89.7% coverage), calculates relevancy scores, and generates strategic threat signals using Claude Haiku + Sonnet.

**Executive Dashboard** (`dashboard/v1/dashboard.html`) — Interactive web interface with 5 KPIs, 4 filterable charts, signal cards with threat levels, company drill-downs, and executive summaries. Actian-branded with red/navy/cyan color scheme.

**Team Dashboard** (`dashboard/v2/dashboard_v2.html`) — Enhanced 7-tab interface with team-specific views (Marketing, Product, SDRs, PMM, Executives) plus Market Overview and Strategic Signals. Includes location mapping, velocity analysis, skill-based filtering, and role progression tracking.

**AI Assistant** (`src/assistant_backend.py`) — Flask backend with Claude Sonnet semantic layer that:
- Answers questions about competitive hiring patterns with real-time metrics
- Generates dynamic suggested prompts from competitive_signals.json
- Guides custom analysis through clarifying questions
- Delivers context-aware dashboard drill-down actions
- Normalized velocity calculation (daily-rate based, capped at 500%)
- Payload-optimized for Groq API (top 20 companies, truncated context)

## Quick Start

### Requirements
- Python 3.11+ (3.9 works, but 3.11 recommended)
- Anthropic API key (`ANTHROPIC_API_KEY` env var)
- Chrome/Chromium for Playwright

### Install & Run

```bash
# Clone repo and navigate
cd ~/Downloads/Scraper

# Install dependencies
pip install -r requirements.txt

# Set API key
export ANTHROPIC_API_KEY=sk-ant-...

# Run scraper (pulls fresh jobs)
python src/scraper.py

# Run enricher (classifies & signals)
python src/enrich.py

# Start AI backend (optional, for chat widget)
python src/assistant_backend.py  # Runs on http://localhost:5001

# View dashboard locally
python -m http.server 8000
# Open: http://localhost:8000/dashboard/v2/dashboard_v2.html
```

## Directory Structure

```
.
├── src/
│   ├── scraper.py               # Async scraper (Playwright + 5 ATS APIs)
│   ├── enrich.py                # Job enrichment & threat signals
│   └── assistant_backend.py      # Flask + Claude Sonnet semantic layer
│
├── dashboard/
│   ├── v1/
│   │   └── dashboard.html        # Original executive dashboard (2-tab)
│   └── v2/
│       └── dashboard_v2.html     # New team dashboard (7-tab, production)
│
├── data/
│   ├── competitors.csv           # 63 companies, 8 groups, ATS URLs
│   ├── jobs_raw.csv              # Raw scraped jobs (~1,200 roles)
│   ├── jobs_enriched.csv         # Enriched with signals & threat
│   ├── seen_jobs.db              # Deduplication database
│   └── signals.json              # Strategic signal output
│
├── docs/
│   ├── CHANGELOG_2026.md         # Q1 2026 feature updates & fixes
│   └── .mcp.json                 # code-review-graph MCP config
│
├── archive/
│   └── old-docs/                 # Historical documentation
│
├── requirements.txt              # Python dependencies
├── scrape.yml                    # GitHub Actions daily workflow (06:00 UTC)
├── .gitignore                    # Git exclusions
└── README.md                     # This file
```

## Key Features

### Dashboard (v2 - Production)

| Tab | View | Key Metrics |
|-----|------|-------------|
| **Overview** | Market pulse, company/function/seniority distribution, location heatmap | Total roles, weekly velocity, avg seniority, top threat, AI % |
| **Signals** | Strategic threat cards, signal taxonomy, market-level insights | CRITICAL/HIGH threats, launch announcements, hiring trends |
| **Marketing** | Marketing/GTM team hiring, budget signals, thought leadership investment | PMM roles, content ops, brand-focused positions |
| **Product** | Product/PM team hiring, roadmap signals, feature team patterns | Product Manager, Analytics, UX Designer roles |
| **SDRs** | Sales development team expansion, sales engineering, customer success | BDR/AE roles, quota-bearer roles, ramp velocity |
| **PMM** | Product marketing managers, competitive intelligence, messaging experts | Product Marketing, Analyst Relations, Solutions Marketing |
| **Executives** | C-level/VP hiring, threat grid, executive mobility patterns | Top 20 threat companies, hiring velocity grid |

### Enrichment Pipeline

- **Function Classification**: Engineering, Marketing, Product, Sales, SDR, PMM, Ops, Finance, Legal, HR, Exec, Other (always remapped)
- **Product Focus**: 40+ mappings (Data Management, Cloud, Analytics, Security, AI/Vector, etc.)
- **Seniority Detection**: Junior, Mid, Senior, Lead, Principal, Manager, Director, VP, C-Suite
- **Skill Extraction**: 150+ skills with relevancy scoring (89.7% coverage, up from 15.9%)
- **Threat Scoring**: Volume bonuses (n≥150 +3, n≥100 +2), threshold (n≥150 AND score≥5 = CRITICAL)
- **Relevancy Scoring**: 0-17.5 scale based on skills, product area, seniority, company group

### AI Semantic Layer

Generates per-company metrics for context-aware recommendations:
- **Hiring Velocity**: Daily-rate normalized (d30/30) / (d60/60) * 100, capped at 500%
- **AI Investment %**: Percentage of roles in AI/ML product focus
- **Competitive Overlap %**: % roles in Actian-competitive product areas
- **Senior %**: Percentage Senior+ roles
- **Engineering %**: Percentage engineering roles
- **GTM %**: Percentage Marketing/SDR/Sales roles
- **Market Pressure Index**: Weighted threat score across all companies

### AI Assistant Widget

- Floating chat bubble (bottom-right, accessible on all tabs)
- Dynamic suggestion prompts based on current dashboard state
- Semantic queries with context awareness
- Dashboard drill-down actions (filter by company, function, segment)
- Full markdown support with LaTeX rendering
- Message history within session

## Metrics & Threat Levels

### Threat Levels
- **CRITICAL**: Score ≥5 AND hiring volume ≥150 (MongoDB, Datadog, Snowflake, Databricks, Salesforce, Fivetran, OneTrust, Pinecone)
- **HIGH**: Score ≥4 OR hiring volume ≥100
- **MEDIUM**: Score 2-3 OR hiring volume 30-99
- **LOW**: Score <2 OR hiring volume <30

### Velocity Status
- **🔥 Accelerating**: Velocity >100 (hiring faster than previous 60-day baseline)
- **➡️ Stable**: Velocity 85-115 (consistent hiring rate)
- **❄️ Decelerating**: Velocity <85 (slower than baseline)

### Company Groups
1. **Data Platforms**: MongoDB, Databricks, Snowflake, Clickhouse, Cassandra, Redis
2. **Observability**: Datadog, Dynatrace, Elastic, Splunk, New Relic, Sumo Logic
3. **Data Integration**: Fivetran, Airbyte, Talend/Qlik, Informatica, Apache NiFi
4. **Cloud Infrastructure**: AWS, Azure, Google Cloud, DigitalOcean, Linode
5. **Vector/AI**: Pinecone, Weaviate, Qdrant, Zilliz, Milvus, Vespa
6. **Governance**: Collibra, Alation, OneTrust, Decube
7. **Analytics**: Tableau, Qlik, Sisense, Domo, Looker, Microsoft BI
8. **Platform/Enterprise**: Salesforce, SAP, Oracle, Workday

## Setup Details

### Python Versions
- **Recommended**: 3.11+ (installed via miniforge)
- **Minimum**: 3.9 (system default, but older)
- **MCP**: code-review-graph requires 3.11

### Environment Variables
```bash
ANTHROPIC_API_KEY=sk-ant-...        # Required for scraper & enricher
GROQ_API_KEY=...                    # Optional (for assistant_backend.py)
GITHUB_TOKEN=ghp_...                # Optional (for GitHub Actions)
```

### Playwright Setup
```bash
# Install browser binaries
playwright install chromium
```

### GitHub Actions Workflow
- **Trigger**: Daily at 06:00 UTC (configured in `scrape.yml`)
- **Actions**: Scrape → Enrich → Commit results
- **Retry**: Autostash on pull conflicts (`git pull --rebase --autostash`)
- **Files Updated**: jobs_raw.csv, jobs_enriched.csv, signals.json

## Performance Notes

| Operation | Time | Output Size |
|-----------|------|-------------|
| Full scrape | ~20 min | 1,200 roles, 2.5MB CSV |
| Enrichment | ~8 min | Classification + signals |
| Dashboard load | <1s | 500+ jobs rendered |
| AI query | 2-5s | Semantic layer + LLM |

## Development & Tooling

### Code Review Graph
Dependency analysis and community detection:
```bash
code-review-graph build        # Parse all Python files
code-review-graph query        # Explore call graph
generate_wiki_tool            # Auto-generate community wiki
```

### Testing Dashboards Locally
```bash
# Start simple HTTP server (use this, not preview_start)
python -m http.server 8000

# Visit:
# - http://localhost:8000/dashboard/v1/dashboard.html (original)
# - http://localhost:8000/dashboard/v2/dashboard_v2.html (new)
```

## Phase 2 & 3 (Future)

**Phase 2: Live Signal Enrichment**
- Real-time signal updates from news feeds
- Announcement detection (product launches, partnerships)
- Executive movement tracking
- Investment/funding alerts

**Phase 3: Team Alert Routing**
- Slack webhook integration per team (Marketing → #competitive-marketing, etc.)
- Customizable alert thresholds
- Weekly/daily digest options
- Custom signal subscriptions

## Known Issues & Workarounds

| Issue | Workaround |
|-------|-----------|
| Salesforce ATS slow | Loads detail pages for each job (accuracy over speed) |
| Qlik Eightfold JS rendering | Playwright + pid param required |
| Location short codes clash (us→australia) | Word-boundary enforcement in resolveLocation() |
| Groq 413 payload | Context truncation (top 20 companies) |
| Velocity edge case (0 jobs in prior_60) | Daily-rate normalization + cap at 500% |

## GitHub Workflow

This repo uses:
- **Source of Truth**: GitHub (always `git pull` before editing)
- **Auto-trigger**: Any change to src/, dashboard/, data/competitors.csv runs workflow
- **Commit Convention**: `refactor: ...`, `feat: ...`, `fix: ...`
- **File Handling**: `git mv` for renames, never direct rm/cp

## License

Internal use only — Actian Corporation

---

**Built with**: Python 3.11, httpx, Playwright, Claude API (Haiku/Sonnet), Chart.js 4.4.1, Flask, Groq Llama 3.3 70B, Leaflet.js

**Last Updated**: Q1 2026 (see `docs/CHANGELOG_2026.md` for full history)
