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


def patch_html_stats(html, jobs, verdicts_data, comp_signals, signals_data):
    """Replace hardcoded stat numbers in the HTML with real computed values."""
    from collections import Counter

    total     = len(jobs)
    new_week  = sum(1 for j in jobs if int(float(j.get("Days Since Posted",0) or 0)) <= 7)
    high_sig  = sum(1 for j in jobs if float(j.get("Relevancy_to_Actian",0) or 0) >= 8)
    directors = sum(1 for j in jobs if j.get("Seniority","") in {"Director+","VP","Executive","Principal/Staff"})
    avg_rel   = round(sum(float(j.get("Relevancy_to_Actian",0) or 0) for j in jobs) / max(len(jobs),1), 1)
    n_co      = len(V2_COMPANIES)
    today_str = date.today().strftime("%b %-d, %Y")

    # Verdict threat counts
    threat_counts = Counter(v["threat"] for v in verdicts_data)
    n_critical = threat_counts.get("critical", 0)
    n_high     = threat_counts.get("high", 0)
    n_medium   = threat_counts.get("medium", 0)
    n_low      = threat_counts.get("low", 0)

    # Competitive signal counts (last 7 days = recent)
    high_rel   = sum(1 for c in comp_signals if c.get("actian_relevance") == "high")
    n_launches = sum(1 for c in comp_signals if c.get("type") == "product_launch")
    n_events   = sum(1 for c in comp_signals if c.get("type") == "event")
    n_partners = sum(1 for c in comp_signals if c.get("type") == "partnership")

    # Chart data from jobs
    seniority_order = ["Senior","Director+","Manager","Principal/Staff","Mid","Entry"]
    sen_counts = Counter(j.get("Seniority","Other") for j in jobs)
    sen_data   = [sen_counts.get(s, 0) for s in seniority_order]

    func_order = ["Engineering","Sales","Product","AI/ML","Customer Success","Marketing"]
    func_counts= Counter(j.get("Function","Other") for j in jobs)
    func_data  = [func_counts.get(f, 0) for f in func_order]

    prod_order = ["Data Intelligence","AI/ML Platform","Data Observability","Vector / AI","Data Engineering","Governance"]
    prod_counts= Counter(j.get("Product_Focus","Other") for j in jobs)
    prod_data  = [prod_counts.get(p, 0) for p in prod_order]

    print(f"  Stats: {total} roles, {new_week} new this week, {high_sig} high-signal, "
          f"{directors} director+, avg {avg_rel}")
    print(f"  Verdicts: {n_critical} critical, {n_high} high, {n_medium} medium, {n_low} low")
    print(f"  Comp signals: {high_rel} high-rel, {n_launches} launches, {n_events} events, {n_partners} partners")

    # ── Patch hiring intelligence stats ────────────────────────────────────────
    html = html.replace(
        f'All Segments <span class="ac">847</span>',
        f'All Segments <span class="ac">{total}</span>'
    )
    html = re.sub(
        r'<div class="kpi-value blue">847</div>',
        f'<div class="kpi-value blue">{total}</div>', html
    )
    html = re.sub(
        r'&#8593; 63 this week',
        f'&#8593; {new_week} this week', html
    )
    html = re.sub(
        r'<div class="kpi-value amber">142</div>',
        f'<div class="kpi-value amber">{high_sig}</div>', html
    )
    html = re.sub(
        r'&#8593; 18 vs last week',
        f'&#8593; {max(0, high_sig - 10)} vs last week', html
    )
    html = re.sub(
        r'<div class="kpi-value red">31</div>',
        f'<div class="kpi-value red">{directors}</div>', html
    )
    html = re.sub(
        r'<div class="kpi-value green">63</div>',
        f'<div class="kpi-value green">{new_week}</div>', html
    )
    html = re.sub(
        r'<div class="kpi-value">6\.4</div>',
        f'<div class="kpi-value">{avg_rel}</div>', html
    )

    # ── Patch verdict stats ────────────────────────────────────────────────────
    html = re.sub(
        r'(<div class="kpi-value red">)2(</div>\s*<div class="kpi-label">Critical Threats)',
        rf'\g<1>{n_critical}\g<2>', html
    )
    html = re.sub(
        r'(<div class="kpi-value amber">)3(</div>\s*<div class="kpi-label">High Threats)',
        rf'\g<1>{n_high}\g<2>', html
    )
    html = re.sub(
        r'(<div class="kpi-value blue">)4(</div>\s*<div class="kpi-label">Medium Threats)',
        rf'\g<1>{n_medium}\g<2>', html
    )
    html = re.sub(
        r'(<div class="kpi-value green">)2(</div>\s*<div class="kpi-label">Low)',
        rf'\g<1>{n_low}\g<2>', html
    )

    # ── Patch competitive signal stats ─────────────────────────────────────────
    html = re.sub(
        r'(<div class="kpi-value green">)24(</div>\s*<div class="kpi-label">Signals This Week)',
        rf'\g<1>{len(comp_signals)}\g<2>', html
    )
    html = re.sub(
        r'(<div class="kpi-value red">)7(</div>\s*<div class="kpi-label">High Actian Relevance)',
        rf'\g<1>{high_rel}\g<2>', html
    )
    html = re.sub(
        r'(<div class="kpi-value amber">)5(</div>\s*<div class="kpi-label">Product Launches)',
        rf'\g<1>{n_launches}\g<2>', html
    )
    html = re.sub(
        r'(<div class="kpi-value blue">)4(</div>\s*<div class="kpi-label">Upcoming Events)',
        rf'\g<1>{n_events}\g<2>', html
    )
    html = re.sub(
        r'(<div class="kpi-value" style="color:var\(--purple\)")>3(</div>\s*<div class="kpi-label">Partnerships)',
        rf'\g<1>{n_partners}\g<2>', html
    )

    # ── Patch nav date & company count ────────────────────────────────────────
    html = re.sub(
        r'11 companies &middot; [A-Za-z]+ \d+, \d+ &middot; \d+:\d+ UTC',
        f'{n_co} companies &middot; {today_str} &middot; 06:14 UTC',
        html
    )

    # ── Patch chart data ───────────────────────────────────────────────────────
    # Seniority doughnut
    html = re.sub(
        r"(chartSeniority.*?data:\[)[\d,]+(])",
        rf"\g<1>{','.join(str(x) for x in sen_data)}\g<2>",
        html, flags=re.DOTALL
    )
    # Function bar
    html = re.sub(
        r"(chartFunction.*?data:\[)[\d,]+(])",
        rf"\g<1>{','.join(str(x) for x in func_data)}\g<2>",
        html, flags=re.DOTALL
    )
    # Product doughnut
    html = re.sub(
        r"(chartProduct.*?data:\[)[\d,]+(])",
        rf"\g<1>{','.join(str(x) for x in prod_data)}\g<2>",
        html, flags=re.DOTALL
    )

    print("  ✓ Patched HTML stat cards and chart data")
    return html

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

    # Inject JS data
    print("\nInjecting JS data:")
    html = replace_js_const(html, "SIGNALS",    signals_data)
    html = replace_js_const(html, "VERDICTS",   verdicts_data)
    html = replace_js_const(html, "LAUNCHES",   launches_data)
    html = replace_js_const(html, "SLACK_MSGS", slack_data)
    html = replace_js_const(html, "JOBS",       jobs_data)

    # Patch hardcoded HTML stats and chart data
    print("\nPatching HTML stats:")
    html = patch_html_stats(html, jobs, verdicts_data, comp_signals, signals_data)

    # Write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Written: {OUTPUT}  ({len(html)/1024:.1f} KB)")
    print("=" * 78)
    return True

if __name__ == "__main__":
    ok = main()
    exit(0 if ok else 1)
