# Post-API Comparison — Is DeepSeek Worth It?
_Captured: 2026-04-27 | First CI run with DEEPSEEK_API_KEY active_

Direct side-by-side against `pre_api_baseline.md`. Same companies, same data window, same schema — only the LLM is new.

---

## TL;DR — Verdict on the API

**Yes. The API is unambiguously worth the ~$1.20/month.**

| Quality dimension | Pre-API (rule-based) | Post-API (DeepSeek V4-Flash) |
|------------------|----------------------|------------------------------|
| Verdict specificity | Counts + templates | Specific roles, dates, geo, competitive moves |
| Hiring brief reasoning | Same 6 templates per company | Per-company analyst inferences |
| `recommended_action` | "PMM: refresh battlecard within 2 weeks" (every co.) | Tied to specific named events / launches / accounts |
| Theme density | 7/11 verdicts had themes | 10/11 verdicts have themes |
| Cost | $0 | ~$0.04/day |

The two areas where post-API output is **unchanged** are competitive signals and news items — but only because of the dedup cache (`seen_signals.json`, `seen_news.json`). New items going forward will be LLM-classified. This run only re-classified verdicts and hiring signals because everything else was already-seen.

---

## 1. Verdicts — Side by Side

### Atlan

**BEFORE (rule-based template):**
> Atlan is assembling senior leadership (73% senior+ roles) — signals a strategic pivot or new product line launch in Platform / Infra. Product launches detected: Atlan Activate: Atlan's latest product launches. 2 upcoming event(s) confirmed — active GTM motion.
>
> *Recommended action:* PMM: verify Atlan's new capability does not undercut Actian's embedded lineage and governance at the data source in active competitive evaluations; update battlecard if feature gap confirmed.

**AFTER (DeepSeek):**
> Atlan is scaling its data intelligence platform with a focus on agentic governance and conversational search, as evidenced by its April 29, 2026 product launch event 'Atlan Activate' featuring conversational search, data quality, and agentic stewards. This is reinforced by a high-intensity engineering hiring campaign concentrated in India (4 senior platform/security roles) for global platform infrastructure and security automation, with no ML engineer hires, suggesting a platform-first, AI-later approach.
>
> *Recommended action:* Product and PMM should prepare a competitive response within 30 days: create a positioning document highlighting Actian's real-time analytics and hybrid integration strengths vs. Atlan's governance-only approach, and target the Gartner EMEA Data & Analytics Summit (May 11, 2026) to counter Atlan's booth demos with Actian's proven enterprise reliability.

**What changed:**
- Specific date ("April 29, 2026"), specific geography ("India"), specific count ("4 senior platform/security roles")
- Inference about strategy ("platform-first, AI-later")
- Action references a specific named event with a date

---

### Collibra

**BEFORE:**
> Collibra is assembling senior leadership (69% senior+ roles) — signals a strategic pivot or new product line launch in Platform / Infra. Product launches detected: Introducing the Collibra AI Command Center; AI Governance dashboard.
>
> *Recommended action:* PMM + SDRs: refresh Collibra battlecard within 2 weeks — new platform capability directly challenges Actian's embedded lineage and governance at the data source; lead with Actian's deployment flexibility as the counter-position.

**AFTER:**
> Collibra is transitioning from a data catalog vendor to an AI-powered data intelligence platform. Launch of AI Command Center (Apr 2026) and AI Governance Dashboard (Mar 2026) provides 360-degree visibility and governance across all AI systems. Hiring of 6+ AI/ML engineer roles across Brussels, Raleigh, and Prague, and 10+ GTM roles focused on US public sector, West Coast, and UK, signals scaling of AI product development and enterprise sales push into Actian's core segments.
>
> *Recommended action:* Product should accelerate Actian's vector/AI product launch to Q3 2026 with integrated IoT and real-time data lineage, while PMM develops a 'Governance-Ready AI' counter-message targeting regulated verticals; SDRs should identify accounts attending Collibra roadshows (Munich, SF, Paris) for preemptive outreach highlighting Actian's multi-modal integration and lower TCO.

**What changed:**
- Names specific cities for hiring ("Brussels, Raleigh, Prague" and "US public sector, West Coast, UK")
- Strategic narrative: "transitioning from data catalog vendor to AI-powered platform"
- Action specifies a target date ("Q3 2026"), specific message ("Governance-Ready AI"), and specific roadshow cities for SDRs to target

---

### Snowflake

**BEFORE:**
> Snowflake is doubling down on data engineering with 5 pipeline/integration roles — direct competitive move against Actian in the data integration space. Product launches detected: Introducing DCM Projects: Declarative Infrastructure Management for Sn; Snowflake Launches Project SnowWork, Bringing Outcome-Driven AI to Eve.

**AFTER:**
> Snowflake is executing a multi-front offensive: (1) aggressive sales hiring (10+ AE roles in FSI, government, APAC) and (2) launching three AI-native products in Q1-Q2 2026: DCM Projects (declarative infrastructure), Project SnowWork (outcome-driven AI), and Cortex Code (AI coding agent). The events are heavily skewed toward AI, with 10+ AI-focused workshops, hackathons, and roadshows in April 2026 alone.
>
> *Recommended action:* PMM should immediately create a competitive Battle Card for 'Snowflake Cortex Code vs Actian Vector DB' and distribute to SDRs by end of week; Product should accelerate the Actian AI agent capability to match SnowWork's outcome-driven AI narrative, emphasizing Actian's hybrid integration advantage.

**What changed:**
- Reframes the picture as "multi-front offensive" with named product/launch combinations
- Quantifies events ("10+ AI-focused workshops")
- Action gives a specific battle card name with deadline ("end of week")

---

## 2. Hiring Signals (Briefs) — Side by Side

### Acceldata (21 postings)

**BEFORE — Templated implications:**
1. "Cloud infrastructure hiring (3 roles) suggests expanding multi-cloud deployment options — potential new managed service or marketplace listing"
2. "Heavy infrastructure hiring (7 roles) signals scaling for enterprise workloads — possibly preparing for 10x traffic growth"
3. "Acceldata operates in Other — indirect competitive pressure but potential partnership opportunity"
4. "Distributed hiring across 6 locations indicates global expansion or multi-hub strategy"
5. "Hiring across 7 product areas suggests portfolio expansion or major platform consolidation"
6. "Aggressive 21-role hiring surge indicates rapid product iteration"

**AFTER — Reasoned per-company:**
1. "Concentrating 4 principal/staff engineers (ODP, Hadoop, Cloud Support) in Kitchener + Bengaluru + California suggests a single high-priority platform rebuild or new product launch around open data and Hadoop modernization"
2. "Hiring 2 Technical Account Managers for the Acceldata Data Observability Cloud (ADOC) in US and UK indicates they are scaling post-sales customer success for an existing product, not just building"
3. "The pairing of Site Reliability Engineer (Hadoop Modernization) and multiple Hadoop Product Support Engineers reveals a deliberate strategy to support and modernize complex Hadoop environments"

**What changed:** Pre-API was 6 generic implications applicable to any company. Post-API references actual job titles ("ODP", "ADOC", "Hadoop Modernization SRE"), specific locations ("Kitchener", "Bengaluru"), and infers a coherent strategy ("Hadoop modernization play") that's specific to Acceldata.

**Watch-for examples (post-API):**
- "Watch for Acceldata to announce a new 'Hadoop to Cloud' migration service or product at a major data conference (e.g., Data Summit, Snowflake Summit)"
- "Watch for the job postings for 'Open Data Platform (ODP)' roles to specify Apache Iceberg, Delta Lake, or similar open table formats"

vs. pre-API generic: *"Track Acceldata's job posting velocity — sustained increase signals strategic investment acceleration"*

---

## 3. Theme Density (Semantic Layer)

| | Pre-API | Post-API |
|--|---------|----------|
| Verdicts with themes | 7/11 | 10/11 |
| Unique themes detected | 4 | 5 |
| Multi-area verdicts | Snowflake only | Atlan + Snowflake |

**Themes now visible across verdicts:**
- Catalog / Metadata / Knowledge Graph (Atlan, Collibra)
- Data / AI Governance (Atlan, Collibra)
- Observability / Quality / Lineage (Atlan, Bigeye, Acceldata, Monte Carlo)
- Lakehouse / Warehouse / HTAP / Postgres (Databricks)
- Vector / RAG / Retrieval (Pinecone, Qdrant)

This unlocks the cross-vendor theme view in the dashboard — searching "governance" or "lakehouse" now surfaces every relevant vendor regardless of their primary `product_area`.

---

## 4. What Did NOT Change This Run (and Why)

| Layer | Status | Reason |
|-------|--------|--------|
| Competitive signal summaries | Unchanged | `seen_signals.json` dedup — items already classified pre-API are not re-run |
| News item summaries | Unchanged | `seen_news.json` dedup — same reason |

**This is correct behavior** — re-running LLM on already-classified items would burn cost without gain. New items going forward (next CI run finds new launches/events/news) will get full LLM classification with summaries + themes attached.

**To force re-classification of all existing items** (one-time, optional): delete `data/seen_signals.json` and `data/seen_news.json`, trigger a CI run. Cost: ~$0.20 one-time across all 91 existing items.

---

## 5. Cost Confirmation

Estimated daily run cost from last 24h:
- ~80K input tokens × $0.14/M = ~$0.011
- ~91K output tokens × $0.28/M = ~$0.025
- **Total: ~$0.036/day → ~$1.10/month**

Real measurement aligns with the pre-flight estimate. No surprises.

---

## 6. Recommendation

**Keep DeepSeek active.** The verdict and hiring brief uplift alone justifies the ~$1.20/month — they went from generic templates to analyst-quality narrative with specific roles, dates, geography, and competitive responses. Once the existing comp_signals/news dedup window rolls forward (90 days), every item in the dataset will be LLM-classified, and the dashboard search/filter quality compounds further.

**Optional next move:** delete `seen_signals.json` and `seen_news.json` once to backfill summaries on the existing 91 items — costs ~$0.20 one-time and makes the launches/events/news views match the verdict quality immediately rather than rolling in over 90 days.
