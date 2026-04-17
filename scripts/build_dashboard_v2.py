#!/usr/bin/env python3
"""
build_dashboard_v2.py — Phase 3 CI Dashboard Builder
Reads pipeline data, injects into template, writes dashboard_v2.html
NO regex surgery. NO chatbot injection. ONLY data replacement.
"""
import json, re, csv
from pathlib import Path
from datetime import date, datetime, timezone

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parent.parent
TEMPLATE   = ROOT / "dashboard/v2/template_v2.html"
OUTPUT     = ROOT / "dashboard/v2/dashboard_v2.html"
SIGNALS_J  = ROOT / "data/signals.json"
VERDICTS_J = ROOT / "data/intelligence_verdicts.json"
COMP_SIG_J = ROOT / "data/competitive_signals.json"
JOBS_CSV   = ROOT / "data/jobs_enriched_v2.csv"

# ── V2 Companies ───────────────────────────────────────────────────────────────
V2_COMPANIES = {
    "Atlan","Collibra","Alation",
    "Monte Carlo","Bigeye","Acceldata",
    "Pinecone","Qdrant","Milvus",
    "Snowflake","Databricks"
}

# ── Segment mapping: pipeline value → dashboard display value ──────────────────
SEGMENT_MAP = {
    "Data Intelligence":    "Data Intelligence",
    "Data Observability":   "Data Observability",
    "Vector DB / AI":       "Vector AI",
    "VectorAI":             "Vector AI",
    "AI Analyst":           "AI Analyst",
    "Warehouse/Processing": "AI Analyst",
}

# ── Threat/intensity maps ──────────────────────────────────────────────────────
INTENSITY_MAP = {"high": "High", "medium": "Medium", "low": "Low", "High": "High", "Medium": "Medium", "Low": "Low"}

# ── Routing maps (demo uses lowercase team keys) ───────────────────────────────
ROUTING_TEAM_MAP = {
    "Product": "product", "PMM": "pmm", "SDRs": "sdrs",
    "Marketing": "marketing", "Executives": "executives",
    "Sales": "sdrs", "Engineering": "product",
}

def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  ⚠ Could not load {path.name}: {e}")
        return []

def load_csv(path):
    rows = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"  ⚠ Could not load {path.name}: {e}")
    return rows

def fmt_date(d_str):
    if not d_str:
        return ""
    try:
        dt = datetime.fromisoformat(str(d_str))
        return dt.strftime("%b %-d, %Y")
    except:
        return str(d_str)

# ── Data builders ──────────────────────────────────────────────────────────────

def build_signals(raw_signals, jobs):
    """Build SIGNALS array for the Strategic Signals page."""
    out = []
    company_jobs = {}
    for j in jobs:
        c = j.get("Company","")
        if c not in company_jobs:
            company_jobs[c] = []
        company_jobs[c].append(j)

    for s in raw_signals:
        company = s.get("company","")
        if company not in V2_COMPANIES:
            continue
        seg_raw = s.get("company_group", s.get("product_area", s.get("segment","")))
        segment = SEGMENT_MAP.get(seg_raw)
        if not segment:
            continue

        cjobs = company_jobs.get(company, [])
        ai_roles = sum(1 for j in cjobs if "AI" in j.get("Product_Focus","") or "ML" in j.get("Product_Focus","") or "AI" in j.get("Function",""))
        senior = sum(1 for j in cjobs if j.get("Seniority","") in {"Director+","VP","Executive","Principal/Staff","Director"})
        hiring = len(cjobs)

        intensity_raw = s.get("hiring_intensity", s.get("intensity","medium"))
        intensity = INTENSITY_MAP.get(intensity_raw, "Medium")

        implications = s.get("key_implications", s.get("implications", []))
        if isinstance(implications, str):
            implications = [implications]

        out.append({
            "company": company,
            "segment": segment,
            "threat": s.get("threat_level", s.get("threat","medium")).lower(),
            "intensity": intensity,
            "dominant": s.get("dominant_function", s.get("dominant","Engineering")),
            "ai_roles": ai_roles,
            "senior": senior,
            "hiring": hiring if hiring > 0 else s.get("open_roles", 0),
            "summary": s.get("signal_summary", s.get("summary","")),
            "implications": implications[:3] if implications else [],
        })
    return out

def build_verdicts(raw_verdicts, signals_by_company):
    """Build VERDICTS array for the Intelligence Verdicts page."""
    out = []
    for v in raw_verdicts:
        company = v.get("company","")
        if company not in V2_COMPANIES:
            continue
        seg_raw = v.get("product_area", v.get("segment",""))
        segment = SEGMENT_MAP.get(seg_raw, seg_raw)

        routing_raw = v.get("team_routing", v.get("routing", []))
        routing = [r for r in routing_raw if r in {"Product","PMM","SDRs","Marketing","Executives","Sales Engineering"}]

        team_rel_raw = v.get("team_relevance", {})
        team_rel = {
            "product":    team_rel_raw.get("product", 3),
            "marketing":  team_rel_raw.get("marketing", 2),
            "sdrs":       team_rel_raw.get("sdrs", 2),
            "pmm":        team_rel_raw.get("pmm", 3),
            "executives": team_rel_raw.get("executives", 2),
        }

        out.append({
            "company":       company,
            "segment":       segment,
            "threat":        v.get("threat_level", v.get("threat","medium")).lower(),
            "impact_scope":  v.get("impact_scope", 3),
            "confidence":    v.get("confidence","low"),
            "verdict":       v.get("verdict",""),
            "what":          v.get("what_is_happening", v.get("what","")),
            "why":           v.get("why_it_matters", v.get("why","")),
            "action":        v.get("recommended_action", v.get("actian_action", v.get("action",""))),
            "routing":       routing,
            "signals":       v.get("signal_types", v.get("signals",["hiring"])),
            "top_signals":   v.get("top_signals", [])[:5],
            "team_relevance": team_rel,
        })
    return out

def build_launches(comp_signals):
    """Build LAUNCHES array for Launches & Events page."""
    TYPE_MAP = {
        "open_source_release": "oss_release",
        "funding":             "blog_post",
    }
    out = []
    for c in comp_signals:
        company = c.get("company","")
        if company not in V2_COMPANIES:
            continue
        type_raw = c.get("type","blog_post")
        ltype = TYPE_MAP.get(type_raw, type_raw)

        rel = c.get("actian_relevance", c.get("relevance","low"))

        routing_raw = c.get("routing", [])
        if not routing_raw:
            if ltype in ("product_launch","oss_release"):
                routing_raw = ["Product","PMM","Marketing"]
            elif ltype == "event":
                routing_raw = ["SDRs","PMM","Marketing"]
            elif ltype == "partnership":
                routing_raw = ["Executives","PMM"]
            else:
                routing_raw = ["Product"]

        out.append({
            "company":   company,
            "type":      ltype,
            "title":     c.get("title",""),
            "summary":   c.get("summary",""),
            "impact":    c.get("impact",""),
            "relevance": rel,
            "date":      fmt_date(c.get("published_date", c.get("date",""))),
            "routing":   routing_raw,
        })
    return out

def build_jobs(jobs):
    """Build JOBS array for Hiring Intelligence page."""
    out = []
    for j in jobs:
        company = j.get("Company","")
        if company not in V2_COMPANIES:
            continue
        try:
            rel = round(float(j.get("Relevancy_to_Actian", 0) or 0), 1)
        except:
            rel = 0.0
        try:
            days = int(float(j.get("Days Since Posted", 0) or 0))
        except:
            days = 0

        seg_raw = j.get("Product_Focus","")
        out.append({
            "c": company,
            "t": j.get("Job Title",""),
            "s": j.get("Seniority","Mid"),
            "f": j.get("Function","Engineering"),
            "p": seg_raw,
            "l": j.get("Location","Remote"),
            "d": days,
            "r": rel,
            "u": j.get("Job Link",""),
            "sk": j.get("Primary_Skill",""),
        })
    out.sort(key=lambda x: -x["r"])
    return out[:60]

def build_slack_msgs(verdicts_data, comp_signals):
    """Build SLACK_MSGS with simplified routing.

    Routing rules:
    - ALL signals/verdicts → #actian-competitive-intel
    - Marketing-relevant (product_launch, event, partnership) ALSO → @marketing
    """
    msgs = []
    today = date.today().strftime("%b %-d")

    # Verdict-based messages → #actian-competitive-intel
    for v in verdicts_data[:4]:
        threat = v["threat"]
        if threat not in ("critical", "high"):
            continue
        what = v.get("what","").replace("\n"," ").strip()[:200]
        why  = v.get("why","").replace("\n"," ").strip()[:150]
        action = v.get("action","").replace("\n"," ").strip()[:150]
        body = (
            f"{what}\\n\\n"
            f"<span class='sy'>Why it matters:</span> {why}\\n"
            f"<span class='sa'>Actian action:</span> {action}\\n\\n"
            f"<span class='sl'>→ View full verdict in dashboard</span>"
        )
        msgs.append({
            "channel": "#actian-competitive-intel",
            "ts": today,
            "company": v["company"],
            "label": f"{'CRITICAL' if threat=='critical' else 'HIGH'} THREAT — {v.get('segment','')}",
            "body": body,
        })

    # Launch/event-based messages
    marketing_types = {"product_launch", "event", "partnership"}
    for c in comp_signals:
        if c.get("company") not in V2_COMPANIES:
            continue
        if c.get("actian_relevance","low") != "high":
            continue
        ltype = c.get("type","")
        summary = c.get("summary","").replace("\n"," ").strip()[:200]
        body = (
            f"{summary}\\n\\n"
            f"<span class='sl'>→ View in Launches & Events</span>"
        )
        # All launches → #actian-competitive-intel
        msgs.append({
            "channel": "#actian-competitive-intel",
            "ts": today,
            "company": c["company"],
            "label": f"{ltype.replace('_',' ').title()} — {c.get('product_area',c.get('company',''))}",
            "body": body,
        })
        # Marketing-relevant ALSO → @marketing
        if ltype in marketing_types:
            msgs.append({
                "channel": "@marketing",
                "ts": today,
                "company": c["company"],
                "label": f"{ltype.replace('_',' ').title()} — {c.get('product_area',c.get('company',''))}",
                "body": body,
            })
        if len(msgs) >= 8:
            break

    return msgs[:8]

def replace_js_const(html, name, data):
    """Replace const NAME=[...]; in HTML with real data. JSON-safe."""
    json_str = json.dumps(data, ensure_ascii=False, separators=(",",":"))
    pattern = rf"const {name}=\[.*?\];"
    replacement = f"const {name}={json_str};"
    new_html, count = re.subn(pattern, replacement, html, flags=re.DOTALL)
    if count == 0:
        print(f"  ⚠ Could not find 'const {name}=[...]' in template")
    else:
        print(f"  ✓ {name} ({len(data)} items)")
    return new_html

def patch_stats(html, jobs, signals_data, verdicts_data, comp_signals):
    """Patch hardcoded stat numbers in the HTML."""
    total    = len(jobs)
    new_week = sum(1 for j in jobs if int(float(j.get("Days Since Posted",0) or 0)) <= 7)
    high_sig = sum(1 for j in jobs if float(j.get("Relevancy_to_Actian",0) or 0) >= 8)
    directors= sum(1 for j in jobs if j.get("Seniority","") in {"Director+","VP","Executive","Principal/Staff","Director"})
    avg_rel  = (sum(float(j.get("Relevancy_to_Actian",0) or 0) for j in jobs) / max(len(jobs),1))
    n_cos    = len(V2_COMPANIES)

    # Segment counts
    seg_counts = {}
    for j in jobs:
        c = j.get("Company","")
        seg = ""
        for co, s in [("Atlan","Data Intelligence"),("Collibra","Data Intelligence"),("Alation","Data Intelligence"),
                      ("Monte Carlo","Data Observability"),("Bigeye","Data Observability"),("Acceldata","Data Observability"),
                      ("Pinecone","Vector AI"),("Qdrant","Vector AI"),("Milvus","Vector AI"),
                      ("Snowflake","AI Analyst"),("Databricks","AI Analyst")]:
            if c == co:
                seg = s
                break
        if seg:
            seg_counts[seg] = seg_counts.get(seg, 0) + 1

    threat_counts = {"critical":0,"high":0,"medium":0,"low":0}
    for v in verdicts_data:
        t = v.get("threat","medium").lower()
        threat_counts[t] = threat_counts.get(t, 0) + 1

    now_utc = datetime.now(timezone.utc).strftime("%b %-d, %Y · %H:%M UTC")

    # Use precise unique strings — each maps to exactly one element in the template
    replacements = [
        # KPI cards — use full label context to ensure uniqueness
        ('kpi-value blue">847</div><div class="kpi-label">Total Roles Tracked',
         f'kpi-value blue">{total}</div><div class="kpi-label">Total Roles Tracked'),
        ('kpi-value amber">142</div><div class="kpi-label">High Signal',
         f'kpi-value amber">{high_sig}</div><div class="kpi-label">High Signal'),
        ('kpi-value red">31</div><div class="kpi-label">Director+ Roles',
         f'kpi-value red">{directors}</div><div class="kpi-label">Director+ Roles'),
        ('kpi-value green">63</div><div class="kpi-label">New This Week',
         f'kpi-value green">{new_week}</div><div class="kpi-label">New This Week'),
        ('kpi-value">6.4</div><div class="kpi-label">Avg. Relevancy Score',
         f'kpi-value">{avg_rel:.1f}</div><div class="kpi-label">Avg. Relevancy Score'),
        # Area pill counts
        (f'All Segments <span class="ac">847',  f'All Segments <span class="ac">{total}'),
        (f'Data Intelligence <span class="ac">184', f'Data Intelligence <span class="ac">{seg_counts.get("Data Intelligence",0)}'),
        (f'Data Observability <span class="ac">104', f'Data Observability <span class="ac">{seg_counts.get("Data Observability",0)}'),
        (f'VectorAI <span class="ac">101', f'VectorAI <span class="ac">{seg_counts.get("Vector AI",0)}'),
        (f'AI Analyst <span class="ac">153', f'AI Analyst <span class="ac">{seg_counts.get("AI Analyst",0)}'),
        # Verdict KPI counts
        ('kpi-value red">2</div><div class="kpi-label">Critical Threats',
         f'kpi-value red">{threat_counts["critical"]}</div><div class="kpi-label">Critical Threats'),
        ('kpi-value amber">3</div><div class="kpi-label">High Threats',
         f'kpi-value amber">{threat_counts["high"]}</div><div class="kpi-label">High Threats'),
        ('kpi-value blue">4</div><div class="kpi-label">Medium Threats',
         f'kpi-value blue">{threat_counts["medium"]}</div><div class="kpi-label">Medium Threats'),
        # Timestamp — template uses &middot; HTML entity
        ('11 companies &middot; Apr 9, 2026 &middot; 06:14 UTC', f'{n_cos} companies &middot; {now_utc}'),
    ]

    replaced = 0
    for old, new in replacements:
        if old in html:
            html = html.replace(old, new, 1)
            replaced += 1
    print(f"  {replaced}/{len(replacements)} stat replacements applied")

    # Chart data for seniority
    from collections import Counter
    sen_labels = ["Senior","Director+","Manager","Principal/Staff","Mid","Entry"]
    sen_map = {"Senior":0,"Director+":1,"VP":1,"Executive":1,"Manager":2,"Principal/Staff":3,"Mid":4,"Entry":5}
    sen_counts = [0]*6
    for j in jobs:
        if j.get("Company","") in V2_COMPANIES:
            idx = sen_map.get(j.get("Seniority","Mid"), 4)
            sen_counts[idx] += 1

    func_labels = ["Engineering","Sales","Product","AI/ML","Customer Success","Marketing"]
    func_map = {"Engineering":0,"Sales":1,"Product":2,"AI/ML":3,"Customer Success":4,"Marketing":5}
    func_counts = [0]*6
    for j in jobs:
        if j.get("Company","") in V2_COMPANIES:
            f = j.get("Function","")
            idx = func_map.get(f)
            if idx is not None:
                func_counts[idx] += 1

    prod_labels = ["Data Intelligence","AI/ML Platform","Data Observability","Vector DB","Data Engineering","Governance"]
    prod_map = {"Data Intelligence":0,"AI/ML Platform":1,"Data Observability":2,"Vector DB":3,"Data Engineering":4,"Governance":5}
    prod_counts = [0]*6
    for j in jobs:
        if j.get("Company","") in V2_COMPANIES:
            p = j.get("Product_Focus","")
            idx = prod_map.get(p)
            if idx is not None:
                prod_counts[idx] += 1

    # Replace chart data arrays
    html = re.sub(
        r"(chartSeniority.*?data:\{labels:\[.*?\],datasets:\[\{data:)\[.*?\]",
        lambda m: m.group(1) + json.dumps(sen_counts),
        html
    )
    html = re.sub(
        r"(chartFunction.*?data:\{labels:\[.*?\],datasets:\[\{data:)\[.*?\]",
        lambda m: m.group(1) + json.dumps(func_counts),
        html
    )
    html = re.sub(
        r"(chartProduct.*?data:\{labels:\[.*?\],datasets:\[\{data:)\[.*?\]",
        lambda m: m.group(1) + json.dumps(prod_counts),
        html
    )

    print(f"  Stats: {total} roles, {new_week} new, {high_sig} high-signal, {directors} director+, avg {avg_rel:.1f}")
    return html

def patch_pulse(html, jobs, signals_data, verdicts_data):
    """Replace hardcoded Market Pulse cards with real pipeline data."""
    from collections import Counter

    # Threat level from signals (use raw signals_data which are dicts from build_signals)
    # Load fresh from file so we get the actual threat_level field
    sigs_raw = load_json(SIGNALS_J)
    sigs_raw = [s for s in sigs_raw if s.get("company") in V2_COMPANIES]
    threat_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
    criticals = [s for s in sigs_raw if s.get("threat_level") == "critical"]
    highs     = [s for s in sigs_raw if s.get("threat_level") == "high"]

    # Overall market status
    if len(criticals) >= 3:
        mkt_status = "Critical"
        mkt_color  = "var(--accent)"
    elif len(criticals) >= 1:
        mkt_status = "High"
        mkt_color  = "#b91c1c"
    elif len(highs) >= 2:
        mkt_status = "Elevated"
        mkt_color  = "var(--amber)"
    else:
        mkt_status = "Moderate"
        mkt_color  = "var(--green)"

    # Market detail
    job_cnt = Counter(j.get("Company") for j in jobs)
    mkt_detail_cos = sorted(criticals, key=lambda s: job_cnt.get(s["company"], 0), reverse=True)[:3]
    mkt_detail = ", ".join(s["company"] for s in mkt_detail_cos)
    if mkt_detail:
        mkt_detail += f" — {len(criticals)} critical-level threat{'s' if len(criticals)!=1 else ''}"
    else:
        mkt_detail = f"{len(highs)} high-threat companies tracked"

    # Top threat = highest-job-count critical company
    top_threat_co = "—"
    top_threat_detail = "No critical threats detected"
    if criticals:
        top = sorted(criticals, key=lambda s: job_cnt.get(s["company"], 0), reverse=True)[0]
        top_threat_co = top["company"]
        j_count = job_cnt.get(top["company"], 0)
        threat_sig = top.get("signal_summary", "")[:80] if top.get("signal_summary") else ""
        top_threat_detail = f"{j_count} open roles &mdash; {threat_sig}" if threat_sig else f"{j_count} active open roles tracked"
    elif highs:
        top = sorted(highs, key=lambda s: job_cnt.get(s["company"], 0), reverse=True)[0]
        top_threat_co = top["company"]
        top_threat_detail = f"{job_cnt.get(top['company'],0)} roles &mdash; high-intensity hiring"

    # AI/ML roles
    aiml_jobs = [j for j in jobs if j.get("Function","") in ("AI/ML", "Data Science") or
                 "ML" in j.get("Function","") or "AI" in j.get("Product_Focus","")]
    aiml_count = len(aiml_jobs)
    aiml_cos   = len(set(j.get("Company") for j in aiml_jobs))

    # VectorAI watch — count vector companies actively hiring
    vector_cos  = {"Pinecone","Qdrant","Milvus"}
    vec_active  = [s for s in sigs_raw if s.get("company") in vector_cos and
                   s.get("hiring_intensity","low") in ("medium","high")]
    vec_names   = ", ".join(sorted(set(s["company"] for s in vec_active))) or "Pinecone, Qdrant, Milvus"
    vec_count   = len(set(s["company"] for s in vec_active)) or len(vector_cos)

    # GTM expansion — companies with Sales/Marketing hiring
    gtm_cos = set(j.get("Company") for j in jobs if j.get("Function","") in ("Sales","Marketing","Business Development"))
    gtm_count = len(gtm_cos)

    # Director+ surge
    dir_jobs  = [j for j in jobs if j.get("Seniority","") in ("Director+","VP","Executive")]
    dir_cos   = sorted(set(j.get("Company") for j in dir_jobs),
                       key=lambda c: sum(1 for jj in dir_jobs if jj.get("Company")==c), reverse=True)
    dir_count = len(dir_cos)
    dir_names = ", ".join(dir_cos[:5])

    def card(label, value, color, detail):
        return (f'<div class="pulse-card"><div class="pulse-label">{label}</div>'
                f'<div class="pulse-value" style="color:{color}">{value}</div>'
                f'<div class="pulse-detail">{detail}</div></div>')

    # ── Roles page pulse grid (6 cards) ──────────────────────────────────────────
    OLD_ROLES_PULSE = (
        '    <div class="pulse-card"><div class="pulse-label">Market Status</div>'
        '<div class="pulse-value" style="color:var(--accent)">Critical</div>'
        '<div class="pulse-detail">Atlan + Databricks accelerating AI catalog &mdash; direct Actian overlap</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">Top Threat</div>'
        '<div class="pulse-value" style="color:#b91c1c">Atlan</div>'
        '<div class="pulse-detail">MCP launch + 12 AI/ML hires + Activate 2026 Apr 29</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">AI/ML Surge</div>'
        '<div class="pulse-value" style="color:var(--cyan)">+41%</div>'
        '<div class="pulse-detail">AI/ML hiring up across Data Intelligence vs 30 days ago</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">VectorAI Watch</div>'
        '<div class="pulse-value" style="color:var(--purple)">3</div>'
        '<div class="pulse-detail">Pinecone, Qdrant, Milvus all hiring GTM this week</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">GTM Expansion</div>'
        '<div class="pulse-value" style="color:var(--green)">6</div>'
        '<div class="pulse-detail">Companies expanding go-to-market in EMEA this week</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">Director+ Surge</div>'
        '<div class="pulse-value" style="color:var(--amber)">5</div>'
        '<div class="pulse-detail">Leadership hiring: Atlan, Databricks, Snowflake, Collibra, Pinecone</div></div>'
    )
    NEW_ROLES_PULSE = "\n".join([
        "    " + card("Market Status", mkt_status, mkt_color, mkt_detail),
        "    " + card("Top Threat", top_threat_co, "#b91c1c", top_threat_detail),
        "    " + card("AI/ML Roles", str(aiml_count), "var(--cyan)", f"AI/ML-specific roles across {aiml_cos} companies"),
        "    " + card("VectorAI Watch", str(vec_count), "var(--purple)", f"{vec_names} &mdash; all actively hiring"),
        "    " + card("GTM Expansion", str(gtm_count), "var(--green)", "Companies with active sales &amp; marketing hiring"),
        "    " + card("Director+ Surge", str(dir_count), "var(--amber)", f"Leadership roles: {dir_names}"),
    ])
    if OLD_ROLES_PULSE in html:
        html = html.replace(OLD_ROLES_PULSE, NEW_ROLES_PULSE, 1)
        print("  ✓ Roles page pulse updated")
    else:
        print("  ⚠ Roles pulse block not found — skipped")

    # ── Signals page pulse grid (4 cards) ────────────────────────────────────────
    OLD_SIGS_PULSE = (
        '    <div class="pulse-card"><div class="pulse-label">Market Pulse</div>'
        '<div class="pulse-value" style="color:var(--accent)">Critical</div>'
        '<div class="pulse-detail">Atlan + Databricks accelerating AI catalog &mdash; direct Actian overlap</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">Top Threat</div>'
        '<div class="pulse-value" style="color:#b91c1c">Atlan</div>'
        '<div class="pulse-detail">MCP agent launch + 12 AI/ML hires + Activate 2026 event April 29</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">AI/ML Surge</div>'
        '<div class="pulse-value" style="color:var(--cyan)">+41%</div>'
        '<div class="pulse-detail">AI/ML hiring up across Data Intelligence segment vs. 30 days ago</div></div>\n'
        '    <div class="pulse-card"><div class="pulse-label">Vector DB Watch</div>'
        '<div class="pulse-value" style="color:var(--purple)">3</div>'
        '<div class="pulse-detail">Pinecone, Qdrant, Milvus all hiring GTM &mdash; market heating up</div></div>'
    )
    NEW_SIGS_PULSE = "\n".join([
        "    " + card("Market Pulse", mkt_status, mkt_color, mkt_detail),
        "    " + card("Top Threat", top_threat_co, "#b91c1c", top_threat_detail),
        "    " + card("AI/ML Roles", str(aiml_count), "var(--cyan)", f"AI/ML-specific roles across {aiml_cos} companies"),
        "    " + card("VectorAI Watch", str(vec_count), "var(--purple)", f"{vec_names} &mdash; all actively hiring"),
    ])
    if OLD_SIGS_PULSE in html:
        html = html.replace(OLD_SIGS_PULSE, NEW_SIGS_PULSE, 1)
        print("  ✓ Signals page pulse updated")
    else:
        print("  ⚠ Signals pulse block not found — skipped")

    return html


def main():
    print("=" * 70)
    print("build_dashboard_v2.py — Phase 3 CI Dashboard Builder")
    print("=" * 70)

    if not TEMPLATE.exists():
        print(f"✗ Template not found: {TEMPLATE}")
        return False

    jobs         = load_csv(JOBS_CSV)
    raw_signals  = load_json(SIGNALS_J)
    raw_verdicts = load_json(VERDICTS_J)
    comp_signals = load_json(COMP_SIG_J)

    # Filter to V2 companies
    raw_signals  = [s for s in raw_signals  if s.get("company") in V2_COMPANIES]
    raw_verdicts = [v for v in raw_verdicts if v.get("company") in V2_COMPANIES]
    comp_signals = [c for c in comp_signals if c.get("company") in V2_COMPANIES]

    print(f"\n  Jobs: {len(jobs)} | Signals: {len(raw_signals)} | Verdicts: {len(raw_verdicts)} | Comp signals: {len(comp_signals)}")

    signals_by_company = {s["company"]: s for s in raw_signals}

    signals_data  = build_signals(raw_signals, jobs)
    verdicts_data = build_verdicts(raw_verdicts, signals_by_company)
    launches_data = build_launches(comp_signals)
    jobs_data     = build_jobs(jobs)
    slack_data    = build_slack_msgs(verdicts_data, comp_signals)

    html = TEMPLATE.read_text(encoding="utf-8")
    print(f"\n  Template: {len(html)/1024:.1f} KB")

    print("\nInjecting data:")
    html = replace_js_const(html, "SIGNALS",    signals_data)
    html = replace_js_const(html, "VERDICTS",   verdicts_data)
    html = replace_js_const(html, "LAUNCHES",   launches_data)
    html = replace_js_const(html, "SLACK_MSGS", slack_data)
    html = replace_js_const(html, "JOBS",       jobs_data)

    print("\nPatching stats:")
    html = patch_stats(html, jobs, signals_data, verdicts_data, comp_signals)
    print("\nPatching pulse cards:")
    html = patch_pulse(html, jobs, signals_data, verdicts_data)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Written: {OUTPUT}  ({len(html)/1024:.1f} KB)")
    print("=" * 70)
    return True

if __name__ == "__main__":
    ok = main()
    exit(0 if ok else 1)
