"""
build_semantic_layer.py — Precompute the Ask CI semantic layer
──────────────────────────────────────────────────────────────
Runs once daily in CI. Replaces the runtime computation that
assistant_backend.py used to do per request.

Inputs:  data/jobs_enriched.csv, signals.json, intelligence_verdicts.json,
         competitive_signals.json, news.json, roadmaps.json
Output:  data/semantic_layer.json — single ~50KB blob the Vercel worker
         reads at request time, no live computation needed.

Pure Python, no LLM calls, no network. Idempotent.
"""

import csv
import json
from datetime import datetime, date
from pathlib import Path
from collections import defaultdict

from geo import country_from_location

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR  = REPO_ROOT / "data"
OUT_PATH  = DATA_DIR / "semantic_layer.json"

EXCLUDED_FUNCTIONS = {"Legal", "People/HR"}


# ══════════════════════════════════════════════════════════════════════════
# LOADERS
# ══════════════════════════════════════════════════════════════════════════

def _load_csv(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("Function") in EXCLUDED_FUNCTIONS:
                continue
            try:
                row["_relevancy"] = float(row.get("Relevancy_to_Actian") or 0)
            except (ValueError, TypeError):
                row["_relevancy"] = 0.0
            try:
                row["_days"] = int(float(row.get("Days Since Posted") or 9999))
            except (ValueError, TypeError):
                row["_days"] = 9999
            row["_country"] = country_from_location(row.get("Location", ""))
            rows.append(row)
    return rows


def _load_json(path: Path):
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _count(rows: list[dict], key: str) -> dict:
    out: dict[str, int] = defaultdict(int)
    for r in rows:
        v = r.get(key)
        if v:
            out[v] += 1
    return dict(sorted(out.items(), key=lambda x: -x[1]))


# ══════════════════════════════════════════════════════════════════════════
# PER-COMPANY METRICS (mirrors assistant_backend.build_semantic_layer)
# ══════════════════════════════════════════════════════════════════════════

def per_company_metrics(rows: list[dict], signals: list[dict]) -> dict:
    company_counts = _count(rows, "Company")
    sig_by_company = {s.get("company", ""): s for s in signals}
    metrics = {}

    for company, total in company_counts.items():
        c_rows = [r for r in rows if r["Company"] == company]
        recent_30 = [r for r in c_rows if r["_days"] <= 30]
        prior_60  = [r for r in c_rows if 30 < r["_days"] <= 90]

        # Hiring velocity (recent 30d daily rate vs prior 60d daily rate)
        daily_recent = len(recent_30) / 30
        daily_prior  = len(prior_60) / 60
        if daily_prior == 0:
            velocity = min(200, round(daily_recent * 100)) if daily_recent > 0 else 100
        else:
            velocity = min(500, round(daily_recent / daily_prior * 100))

        # AI investment ratio
        ai_roles = [
            r for r in c_rows
            if r.get("Function") in ("AI/ML & Vector", "Engineering")
            and any(k in (r.get("Job Title", "") + r.get("Product_Focus", "")).lower()
                    for k in ["ai", "ml", "llm", "vector", "machine learning", "deep learning", "rag"])
        ]
        ai_pct = round(len(ai_roles) / total * 100) if total else 0

        # Competitive overlap
        direct_focus = {"ETL/Integration", "Data Governance", "Data Observability", "Data Quality", "Vector / AI"}
        overlap_roles = [r for r in c_rows if r.get("Product_Focus") in direct_focus]
        overlap_pct = round(len(overlap_roles) / total * 100) if total else 0

        senior_levels = {"Director+", "Principal/Staff", "Manager", "Senior"}
        senior_pct = round(len([r for r in c_rows if r.get("Seniority") in senior_levels]) / total * 100) if total else 0
        eng_pct    = round(len([r for r in c_rows if r.get("Function") == "Engineering"]) / total * 100) if total else 0
        gtm_pct    = round(len([r for r in c_rows if r.get("Function") in ("Sales", "Marketing", "Customer Success")]) / total * 100) if total else 0

        mean_rel = round(sum(r["_relevancy"] for r in c_rows) / total, 1) if total else 0
        high_rel = len([r for r in c_rows if r["_relevancy"] >= 10])

        dom_fn = next(iter(_count(c_rows, "Function")), "—")
        dom_pf = next(iter(_count(c_rows, "Product_Focus")), "—")

        sig    = sig_by_company.get(company, {})
        threat = (sig.get("threat_level") or "low").lower()

        # Geo footprint
        country_counts = _count(c_rows, "_country")
        named_all = {k: v for k, v in country_counts.items() if k != "Unknown"}
        country_top = list((named_all or country_counts).items())[:5]
        recent_country = _count(recent_30, "_country")
        named_recent = {k: v for k, v in recent_country.items() if k != "Unknown"}
        country_recent = list((named_recent or recent_country).items())[:5]

        metrics[company] = {
            "total_roles":         total,
            "threat_level":        threat,
            "hiring_velocity":     velocity,
            "ai_investment_pct":   ai_pct,
            "competitive_overlap_pct": overlap_pct,
            "senior_pct":          senior_pct,
            "engineering_pct":     eng_pct,
            "gtm_pct":             gtm_pct,
            "mean_relevancy":      mean_rel,
            "high_signal_roles":   high_rel,
            "dominant_function":   dom_fn,
            "dominant_product":    dom_pf,
            "recent_30d":          len(recent_30),
            "company_group":       sig.get("company_group", ""),
            "country_top":         country_top,
            "country_recent_30d":  country_recent,
            "primary_country":     country_top[0][0] if country_top else "Unknown",
        }

    return metrics


# ══════════════════════════════════════════════════════════════════════════
# MARKET-LEVEL ROLLUPS
# ══════════════════════════════════════════════════════════════════════════

def market_rollups(rows: list[dict], per_company: dict) -> dict:
    threat_w = {"critical": 3, "high": 2, "medium": 1, "low": 0}
    market_pressure = sum(threat_w.get(m["threat_level"], 0) * m["total_roles"]
                          for m in per_company.values())

    top_velocity = sorted(per_company.items(), key=lambda x: -x[1]["hiring_velocity"])[:5]
    ai_leaders   = sorted(per_company.items(), key=lambda x: -x[1]["ai_investment_pct"])[:5]
    overlap_top  = sorted(per_company.items(), key=lambda x: -x[1]["competitive_overlap_pct"])[:5]

    function_counts = _count(rows, "Function")
    product_counts  = _count(rows, "Product_Focus")
    seniority_counts = _count(rows, "Seniority")

    country_counts        = _count(rows, "_country")
    recent_country_counts = _count([r for r in rows if r["_days"] <= 30], "_country")

    return {
        "market_pressure_index": market_pressure,
        "top_velocity":   [{"company": c, "velocity": m["hiring_velocity"], "threat": m["threat_level"]} for c, m in top_velocity],
        "top_ai_leaders": [{"company": c, "ai_pct": m["ai_investment_pct"]} for c, m in ai_leaders],
        "top_overlap":    [{"company": c, "overlap_pct": m["competitive_overlap_pct"]} for c, m in overlap_top],
        "top_functions":  dict(list(function_counts.items())[:5]),
        "top_products":   dict(list(product_counts.items())[:5]),
        "seniority":      dict(list(seniority_counts.items())[:8]),
        "top_countries":  dict(list(country_counts.items())[:10]),
        "top_countries_30d": dict(list(recent_country_counts.items())[:10]),
    }


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    print("[semantic-layer] Building...")

    rows     = _load_csv(DATA_DIR / "jobs_enriched.csv")
    signals  = _load_json(DATA_DIR / "signals.json")
    verdicts = _load_json(DATA_DIR / "intelligence_verdicts.json")
    comps    = _load_json(DATA_DIR / "competitive_signals.json")
    news     = _load_json(DATA_DIR / "news.json")
    roadmaps = _load_json(DATA_DIR / "roadmaps.json")

    print(f"  {len(rows)} jobs, {len(signals)} hiring signals, {len(verdicts)} verdicts, "
          f"{len(comps)} comp signals, {len(news)} news items, {len(roadmaps)} roadmaps")

    per_co = per_company_metrics(rows, signals)
    market = market_rollups(rows, per_co)

    # Compact verdicts/roadmaps lookup keyed by company — Ask CI worker reads this
    verdicts_by_co = {
        v.get("company"): {
            "what_is_happening":      v.get("what_is_happening", ""),
            "why_it_matters":          v.get("why_it_matters", ""),
            "primary_interpretation":  v.get("primary_interpretation", ""),
            "recommended_action":      v.get("recommended_action", ""),
            "competitive_impact":      v.get("competitive_impact", {}),
            "themes":                  v.get("themes", []),
            "product_areas":           v.get("product_areas", []),
            "team_routing":            v.get("team_routing", []),
        }
        for v in verdicts
    }

    roadmaps_by_co = {
        r.get("company"): {
            "source":                  r.get("source"),
            "summary":                 r.get("summary", ""),
            "pillars":                 r.get("pillars", []),
            "timeline_estimate":       r.get("timeline_estimate", ""),
            "overall_confidence":      r.get("overall_confidence", ""),
            "actian_competitive_impact": r.get("actian_competitive_impact", ""),
            "what_to_watch_for":       r.get("what_to_watch_for", []),
        }
        for r in roadmaps
    }

    # Top recent comp signals & news (worker uses for "what shipped recently")
    recent_comps = sorted(comps, key=lambda x: x.get("published_date", ""), reverse=True)[:25]
    recent_news  = sorted(news,  key=lambda x: x.get("published_date", ""), reverse=True)[:25]

    out = {
        "snapshot": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totals": {
            "jobs":              len(rows),
            "companies":         len({r["Company"] for r in rows}),
            "comp_signals":      len(comps),
            "news":              len(news),
            "verdicts":          len(verdicts),
            "roadmaps":          len(roadmaps),
        },
        "market":           market,
        "per_company":      per_co,
        "verdicts_by_co":   verdicts_by_co,
        "roadmaps_by_co":   roadmaps_by_co,
        "recent_comp_signals": [
            {"company": s.get("company"), "type": s.get("type"), "title": s.get("title", "")[:120],
             "summary": s.get("summary", "")[:240], "date": s.get("published_date", "")}
            for s in recent_comps
        ],
        "recent_news": [
            {"company": n.get("company"), "type": n.get("news_type"), "title": n.get("title", "")[:120],
             "summary": n.get("summary", "")[:240], "date": n.get("published_date", "")}
            for n in recent_news
        ],
    }

    OUT_PATH.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[semantic-layer] Done — wrote {OUT_PATH.name} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
