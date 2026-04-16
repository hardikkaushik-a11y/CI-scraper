#!/usr/bin/env python3
"""
build_dashboard_v2.py — Regenerate dashboard/v2/dashboard_v2.html with live data
─────────────────────────────────────────────────────────────────────────────────
Template: dashboard/CI_Platform_demo.html  (NEVER MODIFIED)
Output:   dashboard/v2/dashboard_v2.html   (regenerated each run)

Transforms live data from JSON/CSV into the schema expected by the demo UI,
then replaces the hardcoded JS constants (SIGNALS, VERDICTS, LAUNCHES,
SLACK_MSGS, JOBS) with real data.
"""

import csv
import json
import re
from datetime import datetime, date
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────
TEMPLATE   = Path("dashboard/CI_Platform_demo.html")
OUTPUT     = Path("dashboard/v2/dashboard_v2.html")
JOBS_CSV   = Path("data/jobs_enriched_v2.csv")
SIGNALS_J  = Path("data/signals.json")
VERDICTS_J = Path("data/intelligence_verdicts.json")
COMP_SIG_J = Path("data/competitive_signals.json")

# ─── V2 companies ─────────────────────────────────────────────────────────────
V2_COMPANIES = {
    "Atlan", "Collibra", "Alation", "Monte Carlo", "Bigeye",
    "Acceldata", "Pinecone", "Qdrant", "Milvus", "Snowflake", "Databricks"
}

THREAT_SCOPE = {"critical": 5, "high": 4, "medium": 3, "low": 2}

# ─── Loaders ─────────────────────────────────────────────────────────────────
def load_csv(path):
    if not path.exists():
        print(f"⚠ {path} not found"); return []
    with open(path) as f:
        return list(csv.DictReader(f))

def load_json(path):
    if not path.exists():
        print(f"⚠ {path} not found"); return []
    with open(path) as f:
        return json.load(f)

# ─── Helpers ─────────────────────────────────────────────────────────────────
def ai_count(jobs, company):
    return sum(1 for j in jobs if j["Company"] == company
               and any(k in j.get("Product_Focus","") for k in ["AI","ML","Vector"]))

def senior_count(jobs, company):
    return sum(1 for j in jobs if j["Company"] == company
               and j.get("Seniority","") in {"Senior","Director","VP","Principal","Executive","Manager"})

def fmt_date(raw):
    if not raw:
        return ""
    # already a nice string
    if not re.match(r"^\d{4}-\d{2}-\d{2}", str(raw)):
        return str(raw)
    try:
        d = datetime.strptime(str(raw)[:10], "%Y-%m-%d")
        return d.strftime("%b %-d, %Y")
    except Exception:
        return str(raw)[:10]

def routing_from_verdict(v, threat):
    routes = set()
    area = v.get("product_area", "").lower()
    sig  = v.get("signal_type", "").lower()
    if threat in ("critical","high"):
        routes.update(["Executives","PMM"])
    if "product" in area or "catalog" in area or "platform" in area:
        routes.add("Product")
    if "event" in sig or "partnership" in sig:
        routes.update(["SDRs","Marketing"])
    if "hiring" in sig:
        routes.add("PMM")
    routes.add("Product")
    return sorted(routes)

def routing_from_type(t):
    return {
        "product_launch":    ["Product","PMM","Marketing"],
        "open_source_release":["Product","SDRs"],
        "event":             ["SDRs","PMM","Marketing"],
        "partnership":       ["Executives","PMM"],
        "funding":           ["Executives","PMM"],
    }.get(t, ["Product"])

def top_signals_for(v, sig_info):
    parts = []
    hiring = sig_info.get("posting_count", 0)
    if hiring:
        parts.append(f"{hiring} open roles in 30 days")
    corr = v.get("hiring_event_correlation", {})
    if isinstance(corr, dict) and corr.get("explanation"):
        parts.append(corr["explanation"][:100])
    ci = v.get("competitive_impact", {})
    if isinstance(ci, dict):
        if ci.get("type_of_move"):
            parts.append(f"{ci['type_of_move'].capitalize()} move detected")
        if ci.get("at_risk_segments"):
            parts.append(f"At risk: {ci['at_risk_segments'][:80]}")
    what = v.get("what_is_happening", "")
    if what and len(parts) < 5:
        # pull the first sentence
        first = what.split(".")[0].strip()
        if first and first not in " ".join(parts):
            parts.append(first[:100])
    return parts[:5]

def team_relevance(threat, routing):
    base = {"product": 2, "marketing": 2, "sdrs": 2, "pmm": 2, "executives": 2}
    bump = {"critical": 3, "high": 2, "medium": 1}.get(threat, 0)
    for r in routing:
        key = r.lower().replace(" ", "")
        if "product" in key:    base["product"]    = min(5, base["product"]    + bump)
        if "marketing" in key:  base["marketing"]  = min(5, base["marketing"]  + bump)
        if "sdr" in key:        base["sdrs"]       = min(5, base["sdrs"]       + bump)
        if "pmm" in key:        base["pmm"]        = min(5, base["pmm"]        + bump)
        if "exec" in key:       base["executives"] = min(5, base["executives"] + bump)
    return base

def signal_types_from(signal_type_str):
    src = signal_type_str.lower()
    out = []
    if "hiring" in src:     out.append("hiring")
    if "launch" in src:     out.append("launch")
    if "event" in src:      out.append("event")
    if "partner" in src:    out.append("partnership")
    if "funding" in src:    out.append("funding")
    return out or ["hiring"]

# ─── Transformers ─────────────────────────────────────────────────────────────
def build_signals(raw_signals, jobs):
    out = []
    for s in raw_signals:
        if s.get("company") not in V2_COMPANIES:
            continue
        company = s["company"]
        impl = s.get("implications", [])
        if isinstance(impl, str):
            try: impl = json.loads(impl)
            except Exception: impl = [impl]
        out.append({
            "company":    company,
            "segment":    s.get("company_group", ""),
            "threat":     s.get("threat_level", "low").lower(),
            "intensity":  s.get("hiring_intensity", "low").capitalize(),
            "dominant":   s.get("dominant_product_focus", ""),
            "ai_roles":   ai_count(jobs, company),
            "senior":     senior_count(jobs, company),
            "hiring":     int(s.get("posting_count", 0)),
            "summary":    s.get("signal_summary", ""),
            "implications": impl[:3],
        })
    # sort by threat severity
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    out.sort(key=lambda x: order.get(x["threat"], 4))
    return out

def build_verdicts(raw_verdicts, signals_by_company):
    out = []
    for v in raw_verdicts:
        if v.get("company") not in V2_COMPANIES:
            continue
        company = v["company"]
        sig     = signals_by_company.get(company, {})
        threat  = sig.get("threat_level", "medium").lower()
        routing = routing_from_verdict(v, threat)
        out.append({
            "company":      company,
            "segment":      sig.get("company_group", v.get("product_area", "")),
            "threat":       threat,
            "impact_scope": THREAT_SCOPE.get(threat, 2),
            "confidence":   v.get("confidence", "medium"),
            "verdict":      v.get("primary_interpretation", ""),
            "what":         v.get("what_is_happening", ""),
            "why":          v.get("why_it_matters", ""),
            "action":       v.get("recommended_action", ""),
            "routing":      routing,
            "signals":      signal_types_from(v.get("signal_type", "hiring")),
            "top_signals":  top_signals_for(v, sig),
            "team_relevance": team_relevance(threat, routing),
        })
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    out.sort(key=lambda x: order.get(x["threat"], 4))
    return out

def build_launches(comp_signals):
    out = []
    for c in comp_signals:
        if c.get("company") not in V2_COMPANIES:
            continue
        t = c.get("type", "")
        out.append({
            "company":   c["company"],
            "type":      "oss_release" if t == "open_source_release" else t,
            "title":     c.get("title", ""),
            "summary":   c.get("summary", ""),
            "impact":    "",
            "relevance": c.get("actian_relevance", "medium"),
            "date":      fmt_date(c.get("event_date") or c.get("published_date", "")),
            "routing":   routing_from_type(t),
        })
    return out

def build_jobs(jobs, limit=60):
    out = []
    for j in jobs:
        try:
            days = int(float(j.get("Days Since Posted", 0) or 0))
        except Exception:
            days = 0
        try:
            score = round(float(j.get("Relevancy_to_Actian", 0) or 0), 1)
        except Exception:
            score = 0.0
        out.append({
            "c": j.get("Company", ""),
            "t": j.get("Job Title", ""),
            "s": j.get("Seniority", ""),
            "f": j.get("Function", ""),
            "p": j.get("Product_Focus", ""),
            "l": j.get("Location", ""),
            "d": days,
            "r": score,
        })
    # sort by relevance score desc, then days asc
    out.sort(key=lambda x: (-x["r"], x["d"]))
    return out[:limit]

def build_slack_msgs(verdicts_data, comp_signals):
    """Derive Slack preview messages from real verdicts and launches."""
    msgs = []
    today = date.today().strftime("%b %-d")
    for v in verdicts_data[:3]:
        threat = v["threat"]
        channel = "#competitive-signals" if threat in ("critical","high") else "#competitive-product"
        label = f"{'CRITICAL' if threat == 'critical' else 'HIGH'} THREAT — Intelligence Verdict"
        body = (
            f"{v['what']}\n\n"
            f"<span class='sy'>Why it matters:</span> {v['why']}\n"
            f"<span class='sa'>Actian action:</span> {v['action']}\n\n"
            f"<span class='sl'>→ View full verdict in dashboard</span>\n"
            f"<span class='sd'>Updated {v.get('last_updated', today)}</span>"
        )
        msgs.append({
            "channel": channel,
            "ts": today,
            "company": v["company"],
            "label": label,
            "body": body,
        })
    for c in comp_signals:
        if c.get("type") in ("product_launch","open_source_release") and c.get("actian_relevance") == "high":
            msgs.append({
                "channel": "#competitive-product",
                "ts": fmt_date(c.get("published_date", today)),
                "company": c["company"],
                "label": f"Product Launch — {c.get('product_area','')}",
                "body": (
                    f"{c.get('summary','')}\n\n"
                    f"<span class='sl'>→ View in dashboard</span>"
                ),
            })
        if len(msgs) >= 5:
            break
    return msgs

# ─── Inject into HTML ─────────────────────────────────────────────────────────
def replace_js_const(html, name, data):
    """Replace `const NAME=[...];` in HTML with real data."""
    json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    pattern = rf"const {name}=\[.*?\];"
    replacement = f"const {name}={json_str};"
    new_html, count = re.subn(pattern, replacement, html, flags=re.DOTALL)
    if count == 0:
        print(f"  ⚠ Could not find 'const {name}=[...]' in template")
    else:
        print(f"  ✓ Replaced {name} ({len(data)} items)")
    return new_html

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 78)
    print("build_dashboard_v2.py — Generating dashboard from CI_Platform_demo.html")
    print("=" * 78)

    # Load
    jobs        = load_csv(JOBS_CSV)
    raw_signals = load_json(SIGNALS_J)
    raw_verdicts= load_json(VERDICTS_J)
    comp_signals= load_json(COMP_SIG_J)

    # Filter to V2 companies
    raw_signals  = [s for s in raw_signals  if s.get("company") in V2_COMPANIES]
    raw_verdicts = [v for v in raw_verdicts if v.get("company") in V2_COMPANIES]
    comp_signals = [c for c in comp_signals if c.get("company") in V2_COMPANIES]

    print(f"  Jobs: {len(jobs)} | Signals: {len(raw_signals)} | "
          f"Verdicts: {len(raw_verdicts)} | Comp signals: {len(comp_signals)}")

    # Build lookup
    signals_by_company = {s["company"]: s for s in raw_signals}

    # Transform
    signals_data  = build_signals(raw_signals, jobs)
    verdicts_data = build_verdicts(raw_verdicts, signals_by_company)
    launches_data = build_launches(comp_signals)
    jobs_data     = build_jobs(jobs)
    slack_data    = build_slack_msgs(verdicts_data, comp_signals)

    # Read template (never modified)
    if not TEMPLATE.exists():
        print(f"✗ Template not found: {TEMPLATE}"); return False
    html = TEMPLATE.read_text(encoding="utf-8")
    print(f"\n  Template size: {len(html)/1024:.1f} KB")

    # Inject data
    print("\nInjecting data:")
    html = replace_js_const(html, "SIGNALS",    signals_data)
    html = replace_js_const(html, "VERDICTS",   verdicts_data)
    html = replace_js_const(html, "LAUNCHES",   launches_data)
    html = replace_js_const(html, "SLACK_MSGS", slack_data)
    html = replace_js_const(html, "JOBS",       jobs_data)

    # Write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Written: {OUTPUT}  ({len(html)/1024:.1f} KB)")
    print("=" * 78)
    return True

if __name__ == "__main__":
    ok = main()
    exit(0 if ok else 1)
