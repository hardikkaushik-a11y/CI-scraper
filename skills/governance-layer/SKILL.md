---
name: governance-layer
description: Add an enterprise-grade governance layer (source citations, data classification labels, pipeline audit manifest) to any data pipeline or dashboard project. Use when the user asks to "add governance", make a project "enterprise-ready", add "audit trail", "source citations", "traceability", "data classification", or wants to demonstrate readiness to handle internal/sensitive data. Pattern is additive — does not change source data schemas or break existing pipelines.
---

# Governance Layer Skill

Adds three additive governance primitives to any data project:

1. **Source citations** — every output entity links back to its underlying sources (clickable, dated, classified)
2. **Data classification labels** — visible PUBLIC / INTERNAL / CONFIDENTIAL / RESTRICTED badges in the UI and generated files
3. **Pipeline manifest** — JSON audit trail of every build (sources, record counts, timestamps, classification levels)

These three together transform a "demo project" into something an enterprise-buyer or governance-aware stakeholder will recognize as production-credible. The pattern is additive: no schema changes to source data, no breaking changes to existing pipelines.

This skill was developed and proven on the Actian CI competitive intelligence platform (the reference implementation is documented at the bottom of this file).

---

## When to apply this skill

Apply when ANY of the following are true:
- The user is building a dashboard, report, or platform that surfaces AI-generated claims/verdicts/insights
- The user wants to demonstrate the project is "ready for internal data" or "ready for production"
- A reviewer asked "where does this data come from?" or "how do we audit this?"
- The user is preparing a stakeholder pitch and wants to differentiate from "just a dashboard"
- The user is building a Move-2 or Move-3 step in an internal-data adoption roadmap

DO NOT apply when:
- The project is a one-off script with no UI
- There is no concept of "output entity" being claimed (e.g., pure ETL with no insight layer)
- The user hasn't asked for it and no governance signal is present

---

## Step 1 — Discover the project shape

Before writing any code, identify:

| What to find | Why |
|---|---|
| **Build script** (e.g. `build_*.py`, `generate_*.py`) | Where to inject manifest writing + source collection |
| **Template / UI file** (e.g. `template.html`, React component) | Where to render classification badges + source chips |
| **Source data files** (e.g. `data/*.json`, `data/*.csv`) | Inputs to count for the manifest |
| **Output entity** (e.g. competitor, customer, product) | What gets the source-citation list attached to it |
| **Detail view component** (drawer, modal, accordion) | Where the source chips render |
| **Footer / header component** | Where the classification badge shows |

Read those files. Understand the data flow before changing anything.

---

## Step 2 — Source Citations

For each output entity, collect every source record from upstream files that references it. Attach as a `sources` array on the entity.

### Python pattern (build script)

```python
def build_sources(entity_name, *source_lists, max_sources=8):
    """Collect all source records mentioning this entity, dedup by URL, sort newest first."""
    out = []
    seen_urls = set()
    for src_list in source_lists:
        for record in src_list or []:
            if not _matches_entity(record, entity_name):
                continue
            url = record.get("url")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            out.append({
                "type": record.get("type") or record.get("news_type") or "signal",
                "title": record.get("title", ""),
                "url": url,
                "date": record.get("published_date") or record.get("date", ""),
                "sourceType": record.get("source", "external"),
            })
    out.sort(key=lambda s: s.get("date", ""), reverse=True)
    return out[:max_sources]
```

Wire into the entity dict:
```python
entity = {
    "name": company,
    "verdict": verdict_text,
    # ... existing fields ...
    "sources": build_sources(company, signals_list, news_list),
}
```

### React/JSX pattern (template)

In the detail-view component (e.g. drawer, modal):

```jsx
{entity.sources && entity.sources.length > 0 && (
  <section className="drw-sec drw-sources">
    <h4>Sources</h4>
    <div className="drw-source-list">
      {entity.sources.map((s, i) => (
        <a key={i} href={s.url} target="_blank" rel="noreferrer" className="drw-source-chip">
          <span className="drw-source-type">{s.type}</span>
          <span className="drw-source-title">{s.title}</span>
          <span className="drw-source-date">{s.date}</span>
        </a>
      ))}
    </div>
  </section>
)}
```

CSS (subtle, below main content):
```css
.drw-source-list { display: flex; flex-direction: column; gap: 6px; }
.drw-source-chip { display: flex; gap: 8px; align-items: center; padding: 6px 10px;
                   background: var(--surface-2); border-radius: 6px;
                   font-size: 12px; text-decoration: none; color: inherit; }
.drw-source-chip:hover { background: var(--surface-3); }
.drw-source-type { text-transform: uppercase; font-size: 10px; opacity: 0.6;
                   letter-spacing: 0.5px; min-width: 60px; }
.drw-source-title { flex: 1; }
.drw-source-date { opacity: 0.5; font-variant-numeric: tabular-nums; }
```

---

## Step 3 — Data Classification Labels

Surface a classification badge at three places: footer, detail-view header, and HTML comment in the generated file.

### Allowed values
`PUBLIC` · `INTERNAL` · `CONFIDENTIAL` · `RESTRICTED`

Default to `PUBLIC` unless the user specifies otherwise. Lowercase the display ("public data") to keep the UI subtle but unambiguous.

### Footer pattern (template)
```jsx
<footer>
  <span>v{version} · {stats}</span>
  <span className="classification">· public data</span>
</footer>
```

### Detail-view header pattern
```jsx
<div className="drw-header">
  <h3>{entity.name}</h3>
  <span className="drw-classification">PUBLIC</span>
</div>
```

CSS:
```css
.classification, .drw-classification {
  font-size: 11px; opacity: 0.6; letter-spacing: 1px;
  text-transform: uppercase; padding: 2px 6px;
  border: 1px solid currentColor; border-radius: 3px;
}
```

### HTML comment pattern (build script)
Inject at the top of generated HTML output, after `<!DOCTYPE html>`:
```python
html_comment = (
    f"<!-- Pipeline · run: {run_at} · "
    f"{entity_count} {entity_label} · {classification.lower()} data -->\n"
)
output_html = output_html.replace("<!DOCTYPE html>", f"<!DOCTYPE html>\n{html_comment}", 1)
```

---

## Step 4 — Pipeline Manifest

At the end of the build script, write a JSON file documenting the run.

```python
import json
from datetime import datetime, timezone
from pathlib import Path

def write_manifest(out_dir: Path, sources: dict, output_stats: dict, classification="PUBLIC"):
    """
    sources: {"signals.json": {"records": 11, "data_classification": "PUBLIC"}, ...}
    output_stats: {"competitors": 11, "launches": 47, ...}
    """
    manifest = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sources": sources,
        "output": output_stats,
        "default_classification": classification,
    }
    manifest_path = out_dir / "pipeline_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return manifest
```

Call it from `main()`:
```python
manifest = write_manifest(
    out_dir=Path("data"),
    sources={
        "signals.json":              {"records": len(signals),      "data_classification": "PUBLIC"},
        "competitive_signals.json":  {"records": len(comp_signals), "data_classification": "PUBLIC"},
        "news.json":                 {"records": len(news),         "data_classification": "PUBLIC"},
        "intelligence_verdicts.json":{"records": len(verdicts),     "data_classification": "PUBLIC"},
    },
    output_stats={
        "competitors": len(competitors),
        "launches": len(launches),
        "events": len(events),
    },
)
```

---

## Step 5 — Verify (don't claim done until checked)

After implementation, verify:
- [ ] Re-run the build script → no errors
- [ ] Open the generated HTML/UI → classification badge visible in footer
- [ ] Open a detail view → classification visible in header, sources section renders if data has sources
- [ ] Click a source chip → opens correct external URL in new tab
- [ ] Check `pipeline_manifest.json` exists, is valid JSON, has correct counts
- [ ] View source of generated HTML → see `<!-- Pipeline · run: ... -->` comment near top
- [ ] Re-run build twice → manifest updates (timestamp changes), badges don't duplicate

---

## Idempotency rules

- Always **replace** the HTML comment, never append (use `.replace(..., count=1)` or regex with anchor)
- Manifest is **overwritten** on each build, never appended
- Badge rendering is conditional — guard with `entity.sources?.length > 0` so empty arrays don't render empty sections
- Source dedup must be deterministic (dedup key = URL, sort key = date desc)

---

## Files to NOT modify

- Source data JSON/CSV — governance is added at the build layer only
- Existing scraper scripts — they already produce the data we're attributing
- Existing API/scraping logic — no schema changes

---

## Move 2 / Move 3 framing (when user asks "what's next?")

This skill ships **Move 1: governance for public data**. The architecture is designed so the same code works for internal data — the only thing that changes is the classification level.

| Stage | Classification | Data | Risk profile |
|---|---|---|---|
| **Move 1** (this skill) | `PUBLIC` | External sources only (web, RSS, ATS APIs) | Zero — already public |
| **Move 2** | `PUBLIC` + `INTERNAL` mixed | Adds CRM/analytics on top | Per-source classification, RBAC at UI level |
| **Move 3** | `INTERNAL` / `CONFIDENTIAL` | Full internal data integration | Auth, audit, retention policies |

Tell the user: "The governance layer you have now is exactly what enterprise stakeholders look for. Moving to internal data later is a configuration change, not an architecture change."

---

## Reference implementation

The Actian CI platform implements this pattern. Files:

- **Build script:** `scripts/build_dashboard_v3.py` — has `build_sources()` function, `write_manifest()` at end of `main()`, HTML comment injection
- **Template:** `dashboard/v3/template_v3.html` — `BriefDrawer` source section, footer classification, drawer header badge
- **Manifest output:** `data/pipeline_manifest.json`

If the user has access to that repo, point them at it as a working example. Otherwise, use the patterns above directly.

---

## Quick-start checklist for new projects

When user says "add governance to this project":

1. [ ] Identify build script, template, source files, output entity, detail view (Step 1)
2. [ ] Add `build_sources()` to build script; attach `sources` array to each entity (Step 2)
3. [ ] Render source chips in detail view; add CSS (Step 2)
4. [ ] Add classification badge to footer + detail header (Step 3)
5. [ ] Inject HTML comment at top of generated output (Step 3)
6. [ ] Add `write_manifest()` and call from `main()` (Step 4)
7. [ ] Verify all six checks pass (Step 5)
8. [ ] Commit with message: `Add governance layer (sources, classification, manifest)`
