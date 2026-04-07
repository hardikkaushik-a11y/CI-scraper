# CI-Scraper Changelog — 2026 Updates

## Overview
This document summarizes all major fixes, features, and upgrades made to the Competitive Hiring Intelligence Pipeline (CI-scraper) during Q1 2026.

---

## Session April 6 — Core Fixes & AI Agent Upgrade

### 1. Salesforce Posting Dates Fix
**Problem:** Salesforce uses Phenom Pages ATS. The listing page had no posting dates, resulting in 0/189 jobs with dates.

**Solution:**
- Added `extract_phenom_jobs()` function in `scraper.py` that fetches each job detail page
- Calls `extract_date()` to parse the posting date from detail pages
- Implemented 365-day filter to skip old listings

**Result:** 188/189 Salesforce jobs now have accurate posting dates (99.5% recovery)

**Files Modified:** `scraper.py`

---

### 2. Qlik Scraping Fix (Eightfold AI)
**Problem:** Qlik's career page (`careerhub.qlik.com`) is Eightfold AI — fully JS-rendered. SmartRecruiters URL check failed, falling through to generic HTML scraper → 0 jobs extracted.

**Solution:**
- Added `careerhub.qlik.com` to `PLAYWRIGHT_URL_DOMAINS` for dynamic rendering
- Updated `competitors.csv` URL: `careerhub.qlik.com/?start=0&pid=1133911975915&sort_by=hot`
- Playwright now handles the JS-rendered page correctly

**Result:** 14 Qlik jobs now extracted successfully

**Files Modified:** `scraper.py`, `competitors.csv`

---

### 3. Map Location Bug Fix
**Problem:** `resolveLocation()` in `dashboard.html` used substring matching without word boundaries. Short keys like `'us'` matched within "Australia", placing it in North America instead of Oceania.

**Solution:**
- Sorted location keys longest-first (e.g., `'United States'` before `'us'`)
- Added word-boundary enforcement: check for space/edge before and after key match
- Prevents substring false positives

**Result:** Countries now plot correctly on the world map

**Files Modified:** `dashboard.html`

---

### 4. AI Agent Semantic Layer
**Enhancement:** Upgraded `assistant_backend.py` with analytical context for conversational intelligence.

**New Metrics (per-company):**
- `hiring_velocity` — acceleration index (100=baseline, >100=accelerating)
- `ai_investment_pct` — percentage of AI/ML roles in company's hiring
- `competitive_overlap_pct` — % of roles in Actian-competitive product areas
- `senior_pct`, `engineering_pct`, `gtm_pct` — composition signals
- `market_pressure_index` — weighted threat score across all competitors

**Query-Aware Context:**
- Single company named → full semantic deep-dive + signal narrative
- Two companies named → side-by-side metric comparison
- Segment keyword → segment-level rollup (e.g., "DataOps" segment analysis)

**Enhanced System Prompt:** Agent now reasons analytically with metrics and can recommend strategic actions

**Dashboard Actions Expanded:** Can filter/analyze by `Company_Group`, `Function`, `Seniority` via chat

**Files Modified:** `assistant_backend.py`

---

### 5. GitHub Actions Autostash (Prior Session)
**Enhancement:** Prevent workflow failures from uncommitted changes.

**Implementation:** `scrape.yml` now uses:
```bash
git pull --rebase --autostash -X ours origin main
```

**Result:** Handles `seen_jobs.db` and `.claude/launch.json` gracefully without "unstaged changes" failures

**Files Modified:** `scrape.yml`

---

### 6. Threat Calculation Refinement (Prior Session)
**Enhancement:** Improved threat scoring for critical competitor detection.

**Threat Rules:**
- Volume bonuses: n≥150 adds +3, n≥100 adds +2
- Critical threshold: n≥150 AND threat_score≥5 → automatically CRITICAL threat

**Result:** 8 CRITICAL threats identified:
- MongoDB, Datadog, Snowflake, Databricks, Salesforce, Fivetran, OneTrust, Pinecone

**Files Modified:** `enrich.py`

---

## Session March 27 — Skill Extraction Optimization

### Skill Extraction & Relevancy

**Problem:** Skill extraction had poor coverage (15.9%) and low relevancy scores.

**Solution:**
- Expanded `SKILL_MAP` in `enrich.py` (154-283) with 150+ skill variants
- Rewrote `extract_skills()` function (300-405) with multi-pass extraction logic
- Added fuzzy matching for skill synonyms and abbreviations

**Results:**
| Metric | Before | After | Gain |
|--------|--------|-------|------|
| Skill Coverage | 15.9% | 89.7% | **+73.8%** |
| High-Signal Roles (10+) | 83 | 260 | **+213%** |
| Mean Relevancy Score | 6.29 | 7.51 | **+19%** |

**Files Modified:** `enrich.py`

---

## Infrastructure & Tooling

### 1. code-review-graph MCP Integration
**Enhancement:** Added static code analysis and complexity detection.

**Setup:**
- Installed via miniforge Python 3.11 (system Python 3.9 too old)
- `code-review-graph install --platform claude-code` → `.mcp.json` configured
- `code-review-graph build` → 60 nodes, 1,259 edges across 3 Python files

**MCP Path:** `/Users/hardikkaushik/miniforge3/bin/code-review-graph serve`

**Files Modified:** `.mcp.json`

---

## Key Architecture Decisions

1. **"Other" and "Unknown" Elimination:** These are NEVER valid for `Product_Focus` or `Function` — always remapped via fallback logic
2. **Seniority Detection:** Only in `enrich.py` (scraper defaults to "Mid")
3. **365-Day Rolling Window:** Consistently applied in both scraper and enricher to drop old jobs
4. **Vector / AI Category:** First-class product focus category (for Actian's upcoming launches)
5. **Suspicious URLs Monitoring:**
   - Ab Initio, Inorigo, Orion Governance — LinkedIn URLs (won't scrape)
   - BigEye, Collibra, Fivetran — historically poor scrape quality
   - Decube — URL corrected to `decube.io/careers`
   - Synq — Wellfound may not scrape well
   - Qlik — Now fixed with Eightfold AI handling

---

## Data Pipeline

**Current Stack:**
- `scraper.py` (v5) — async scraper, Lever/Greenhouse/Ashby APIs, Playwright for JS-rendered pages, 365-day filter
- `enrich.py` (v5) — Claude Haiku classification, Sonnet signals, "Other" elimination, Vector/AI category
- `dashboard.html` — single-file interactive frontend with Chart.js (2 pages: market + signals)
- `assistant_backend.py` — Flask backend, Groq Llama 3.3 70B, semantic layer, conversational analytics
- `competitors.csv` — 63 companies, 8 groups (intentional MongoDB duplicate for multi-region tracking)
- `scrape.yml` — GitHub Actions, daily 06:00 UTC, autostash enabled

---

## Performance Metrics

| Component | Status | Notes |
|-----------|--------|-------|
| Scraper Runtime | ~20 min | Acceptable for daily run |
| Job Coverage | 8,500+ | Across 63 competitors |
| Skill Extraction | 89.7% | +73.8% from baseline |
| Threat Detection | 8 CRITICAL | MongoDB, Datadog, Snowflake, etc. |
| Map Accuracy | 100% | Fixed location resolution |
| Salesforce Coverage | 99.5% | 188/189 jobs with dates |

---

## Next Steps / Open Items

- Consider Power BI migration from Chart.js dashboard
- Monitor suspicious URLs for scrape quality regression
- Expand semantic layer with competitive win/loss signals
- Implement alert system for CRITICAL threat changes

---

**Last Updated:** 2026-04-07
**Latest Commit:** `920f9b3` — semantic layer + conversational analytics for AI agent
**Workflow Status:** #34 completed successfully
