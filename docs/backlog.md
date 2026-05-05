# CI App — Stakeholder Backlog
_Captured: 2026-04-27_

Items from stakeholder discussion. Not yet scoped or implemented.

---

## 1. Confluence Battlecard Links per Company

Add a per-company link in the app pointing to the Confluence battlecard for that vendor.

**Open questions:**
- Where in the UI? (verdict card header? company drilldown? both?)
- Single link per company or per-team variant (PMM/SDR/Product battlecards differ)?
- Hardcode in `competitors.csv` or external `battlecards.json`?
- Auth — Confluence requires SSO; do we deep-link or land on Confluence home?

**Suggested approach:** new column `battlecard_url` in `competitors.csv`, render as a "📋 Battlecard" pill on the company card in dashboard v3.

---

## 2. Cross-Functional Business Areas per Vendor

Today each verdict has a single `product_area` (e.g. Data Intelligence). Vendors increasingly ship across multiple areas — e.g. a vendor's AI assistant spans Data Intelligence + Data Observability.

**Current schema:**
```json
"product_area": "Data Intelligence"   // single value
```

**Proposed schema:**
```json
"product_areas": ["Data Intelligence", "Data Observability"],
"primary_product_area": "Data Intelligence"
```

**Implications:**
- `verdict_engine.py` — emit array of areas, derived from signal/news/launch types
- Dashboard v3 — verdicts surface in multiple lens filters (a verdict tagged Data Intel + Data Obs shows up in both team views)
- `team_routing.py` — routing already multi-team; product_area can follow same pattern
- V2_PRODUCT_AREA_MAP currently 1:1 — needs updating to allow multi-mapping

**Open questions:**
- How does dashboard show "primary vs secondary" area? Bold + faded? Tags?
- Do we re-classify historical verdicts or only forward?

---

## 3. Expand News Search Themes (Beyond Current Vendor Set)

Today `news_scraper.py` only scrapes the 11-company allowlist. Stakeholder wants horizon-scanning across themes — picking up indicators from companies not yet on our radar.

**Themes to add:**
| Theme | Sample search/RSS targets |
|-------|---------------------------|
| Agentic BI | thoughtspot, sigma, mode |
| Conversational analytics | new entrants in NL→SQL |
| Embedded analytics | sisense, gooddata, embedded.io |
| Semantic / metrics layer | cube, dbt semantic, malloy |
| AI-ready data | data products platforms |
| Data products / data contracts | dataops.live, gable.ai |
| Data governance / AI governance | privacy/AI governance pure-plays |
| Data observability / quality / lineage | Beyond Bigeye/MC/Acceldata — sift, anomalo |
| Metadata mgmt / catalog / knowledge graph | Beyond Atlan/Collibra/Alation — castor (now Coalesce), datafold |
| Unstructured data | unstructured.io, llamaindex enterprise |
| Vector / retrieval / RAG | Beyond Pinecone/Qdrant/Milvus — weaviate, chroma, vespa |
| Lakehouse / warehouse / HTAP / Postgres | Tabular, motherduck, neon, supabase |
| ETL / ELT / reverse ETL | Fivetran, Hightouch, Airbyte, Census |
| MCP servers for data | Anthropic MCP ecosystem — anything data-related |
| Agent observability | Arize, LangSmith, Helicone, Patronus |

**Architecture choice:**
- **Option A:** Theme-based scraper alongside vendor scraper. Searches Google News / RSS aggregators per theme keyword, classifies hits by theme + relevance.
- **Option B:** Expand `competitors.csv` to ~50 vendors (one or two per theme), keep current per-vendor scraper.
- **Option C:** Both — vendor-scraper for the 11 core, theme-scraper for horizon scanning. Theme hits feed a "Market Movement" panel separate from per-vendor verdicts.

**Suggested:** Option C. Theme hits are noisier, lower-signal, better suited to a separate digest section than per-company verdicts.

**New file proposal:** `src/theme_scraper.py` → `data/theme_signals.json` → new dashboard panel "Market Themes" showing top theme movements weekly.

---

## Order of Operations (suggested)

1. **First:** Cross-functional product areas (#2). Schema change cascades through everything else, do it before adding more data sources.
2. **Then:** Battlecard links (#1). Cosmetic / additive, no risk.
3. **Last:** Theme expansion (#3). Largest scope. Build once schema (#2) supports multi-area tagging.
