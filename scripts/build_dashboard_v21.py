#!/usr/bin/env python3
"""
build_dashboard_v21.py — v2.1 CI Dashboard Builder
v2 visual style + all v2 functionality + News Intelligence page
"""
import json, re, csv, sys
from pathlib import Path
from datetime import date, datetime, timezone

ROOT       = Path(__file__).resolve().parent.parent
TEMPLATE   = ROOT / "dashboard/v2.1/template_v21.html"
OUTPUT     = ROOT / "dashboard/v2.1/dashboard_v21.html"
SIGNALS_J  = ROOT / "data/signals.json"
VERDICTS_J = ROOT / "data/intelligence_verdicts.json"
COMP_SIG_J = ROOT / "data/competitive_signals.json"
NEWS_J     = ROOT / "data/news.json"
JOBS_CSV   = ROOT / "data/jobs_enriched_v2.csv"

V2_COMPANIES = {
    "Atlan","Collibra","Alation",
    "Monte Carlo","Bigeye","Acceldata",
    "Pinecone","Qdrant","Milvus",
    "Snowflake","Databricks"
}

SEGMENT_MAP = {
    "Data Intelligence":    "Data Intelligence",
    "Data Observability":   "Data Observability",
    "Vector DB / AI":       "Vector AI",
    "VectorAI":             "Vector AI",
    "AI Analyst":           "AI Analyst",
    "Warehouse/Processing": "AI Analyst",
}

INTENSITY_MAP = {"high":"High","medium":"Medium","low":"Low","High":"High","Medium":"Medium","Low":"Low"}

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
            for row in csv.DictReader(f):
                rows.append(row)
    except Exception as e:
        print(f"  ⚠ Could not load {path.name}: {e}")
    return rows

def fmt_date(d_str):
    if not d_str: return ""
    try:
        dt = datetime.fromisoformat(str(d_str))
        return dt.strftime("%b %-d, %Y")
    except:
        return str(d_str)

# ── Data builders ──────────────────────────────────────────────────────────────

def build_signals(raw_signals, jobs, verdicts):
    out = []
    company_jobs = {}
    for j in jobs:
        c = j.get("Company","")
        company_jobs.setdefault(c, []).append(j)

    built = set()
    for s in raw_signals:
        company = s.get("company","")
        if company not in V2_COMPANIES: continue
        seg_raw = s.get("company_group", s.get("product_area", s.get("segment","")))
        segment = SEGMENT_MAP.get(seg_raw)
        if not segment: continue
        cjobs = company_jobs.get(company, [])
        ai_roles = sum(1 for j in cjobs if "AI" in j.get("Product_Focus","") or "ML" in j.get("Product_Focus","") or "AI" in j.get("Function",""))
        senior = sum(1 for j in cjobs if j.get("Seniority","") in {"Director+","VP","Executive","Principal/Staff","Director"})
        implications = s.get("key_implications", s.get("implications", []))
        if isinstance(implications, str): implications = [implications]
        out.append({
            "company":     company,
            "segment":     segment,
            "threat":      s.get("threat_level", s.get("threat","medium")).lower(),
            "intensity":   INTENSITY_MAP.get(s.get("hiring_intensity", s.get("intensity","medium")), "Medium"),
            "dominant":    s.get("dominant_function", s.get("dominant","Engineering")),
            "ai_roles":    ai_roles,
            "senior":      senior,
            "hiring":      len(cjobs) if cjobs else s.get("open_roles", 0),
            "summary":     s.get("signal_summary", s.get("summary","")),
            "implications": implications[:3],
        })
        built.add(company)

    for company in V2_COMPANIES:
        if company in built: continue
        v = verdicts.get(company, {})
        if v:
            seg_raw = v.get("product_area", "Data Intelligence")
            out.append({
                "company":     company,
                "segment":     SEGMENT_MAP.get(seg_raw, "Data Intelligence"),
                "threat":      (v.get("threat_level") or "medium").lower(),
                "intensity":   "Medium",
                "dominant":    "Engineering",
                "ai_roles":    0, "senior": 0, "hiring": 0,
                "summary":     v.get("what_is_happening", ""),
                "implications": [],
            })
    return out

def build_verdicts(raw_verdicts, signals_by_company):
    IMPACT_SCOPE = {"platform":5,"market":5,"product":4,"feature":3,"none":1}
    THREAT_SCORE = {"critical":5,"high":4,"medium":3,"low":2}

    def infer_routing(threat, signal_type, impact_level):
        r = ["Executives","PMM","Product"] if threat in ("critical","high") else (["Product","PMM"] if threat=="medium" else ["Product"])
        if "event" in (signal_type or "") and "SDRs" not in r: r.append("SDRs")
        if impact_level == "platform" and "Executives" not in r: r.append("Executives")
        return r

    def infer_team_relevance(threat, impact_level):
        base = THREAT_SCORE.get(threat, 3)
        eb = 1 if impact_level in ("platform","market") else 0
        return {"product":min(5,base),"marketing":min(5,base-1),"sdrs":min(5,base-1),"pmm":min(5,base),"executives":min(5,base-1+eb)}

    def build_top_signals(v):
        pts = []
        corr = v.get("hiring_event_correlation", {})
        if corr.get("explanation"): pts.append(corr["explanation"])
        ci = v.get("competitive_impact", {})
        if ci.get("overlap_with_actian"): pts.append(f"Actian overlap: {ci['overlap_with_actian']}")
        if ci.get("at_risk_segments"): pts.append(f"At-risk segments: {ci['at_risk_segments']}")
        if ci.get("type_of_move"): pts.append(f"Move type: {ci['type_of_move']}")
        if v.get("confidence_reasoning"): pts.append(v["confidence_reasoning"])
        return pts[:5]

    def build_verdict_text(v):
        primary = v.get("primary_interpretation","").strip()
        alt = v.get("alternative_interpretation","").strip()
        if primary and alt: return f"{primary} Alternative read: {alt}"
        return primary or v.get("what_is_happening","").strip()

    def parse_signal_types(v):
        raw = v.get("signal_type","hiring")
        if "+" in raw: return [s.strip() for s in raw.split("+")]
        return [raw] if raw and raw != "none" else ["hiring"]

    def fmt_last_updated(v):
        lu = v.get("last_updated","")
        if not lu: return date.today().strftime("%b %-d, %Y")
        try: return datetime.strptime(lu, "%Y-%m-%d").strftime("%b %-d, %Y")
        except: return lu

    out = []
    for v in raw_verdicts:
        company = v.get("company","")
        if company not in V2_COMPANIES: continue
        seg_raw    = v.get("product_area", v.get("segment",""))
        segment    = SEGMENT_MAP.get(seg_raw, seg_raw)
        signal_type  = v.get("signal_type","hiring")
        impact_level = v.get("impact_level","feature")
        sig = signals_by_company.get(company, {})
        threat = v.get("threat_level", v.get("threat", sig.get("threat_level","medium"))).lower()
        routing_raw = v.get("team_routing", v.get("routing", []))
        routing = [r for r in routing_raw if r in {"Product","PMM","SDRs","Marketing","Executives","Sales Engineering"}]
        if not routing: routing = infer_routing(threat, signal_type, impact_level)
        team_rel_raw = v.get("team_relevance", {})
        if team_rel_raw:
            team_rel = {"product":team_rel_raw.get("product",3),"marketing":team_rel_raw.get("marketing",2),"sdrs":team_rel_raw.get("sdrs",2),"pmm":team_rel_raw.get("pmm",3),"executives":team_rel_raw.get("executives",2)}
        else:
            team_rel = infer_team_relevance(threat, impact_level)
        top_signals = v.get("top_signals", []) or build_top_signals(v)
        out.append({
            "company":        company,
            "segment":        segment,
            "threat":         threat,
            "impact_scope":   v.get("impact_scope", IMPACT_SCOPE.get(impact_level, 3)),
            "confidence":     v.get("confidence","low"),
            "verdict":        build_verdict_text(v),
            "what":           v.get("what_is_happening", v.get("what","")),
            "why":            v.get("why_it_matters",    v.get("why","")),
            "action":         v.get("recommended_action", v.get("actian_action", v.get("action",""))),
            "routing":        routing,
            "signals":        parse_signal_types(v),
            "top_signals":    top_signals[:5],
            "team_relevance": team_rel,
            "last_updated":   fmt_last_updated(v),
        })
    return out

def build_launches(comp_signals):
    TYPE_MAP = {"open_source_release":"oss_release","funding":"blog_post"}
    out = []
    for c in comp_signals:
        company = c.get("company","")
        if company not in V2_COMPANIES: continue
        type_raw = c.get("type","blog_post")
        ltype = TYPE_MAP.get(type_raw, type_raw)
        rel = c.get("actian_relevance", c.get("relevance","low"))
        routing_raw = c.get("team_routing", c.get("routing", []))
        if not routing_raw:
            if ltype in ("product_launch","oss_release"): routing_raw = ["Product","PMM","Marketing"]
            elif ltype == "event": routing_raw = ["SDRs","PMM","Marketing"]
            elif ltype == "partnership": routing_raw = ["Executives","PMM"]
            else: routing_raw = ["Product"]
        out.append({
            "company":  company,
            "type":     ltype,
            "title":    c.get("title",""),
            "summary":  c.get("summary",""),
            "impact":   c.get("impact",""),
            "relevance": rel,
            "date":     fmt_date(c.get("published_date", c.get("date",""))),
            "routing":  routing_raw,
        })
    return out

def build_news(raw_news):
    """Build NEWS array for News Intelligence page."""
    RELEVANCE_ORDER = {"high":0,"medium":1,"low":2}
    out = []
    for n in raw_news:
        company = n.get("company","")
        if company not in V2_COMPANIES: continue
        seg_raw = n.get("product_area","")
        segment = SEGMENT_MAP.get(seg_raw, seg_raw)
        routing = n.get("team_routing", [])
        if not routing:
            news_type = n.get("news_type","blog_post")
            type_routing = {
                "funding":        ["Executives","PMM"],
                "acquisition":    ["Executives","PMM","Product"],
                "leadership":     ["Executives","PMM"],
                "partnership":    ["PMM","SDRs","Marketing"],
                "pricing":        ["SDRs","Marketing","PMM"],
                "product_launch": ["Product","PMM","Marketing"],
                "feature":        ["Product","PMM"],
                "layoff":         ["Executives","SDRs"],
                "event":          ["Marketing","SDRs","PMM"],
            }
            routing = type_routing.get(news_type, ["PMM"])
        out.append({
            "company":   company,
            "segment":   segment,
            "type":      n.get("news_type","blog_post"),
            "title":     n.get("title",""),
            "summary":   n.get("summary",""),
            "relevance": n.get("actian_relevance","low"),
            "date":      fmt_date(n.get("published_date","")),
            "routing":   routing,
            "tags":      n.get("tags",[]),
            "url":       n.get("url",""),
        })
    out.sort(key=lambda x: (RELEVANCE_ORDER.get(x["relevance"],2), -(ord(x["date"][0]) if x["date"] else 0)))
    return out

def build_jobs(jobs):
    out = []
    for j in jobs:
        company = j.get("Company","")
        if company not in V2_COMPANIES: continue
        try: rel = round(float(j.get("Relevancy_to_Actian",0) or 0), 1)
        except: rel = 0.0
        try: days = int(float(j.get("Days Since Posted",0) or 0))
        except: days = 0
        out.append({
            "c": company, "t": j.get("Job Title",""), "s": j.get("Seniority","Mid"),
            "f": j.get("Function","Engineering"), "p": j.get("Product_Focus",""),
            "l": j.get("Location","Remote"), "d": days, "r": rel,
            "u": j.get("Job Link",""), "sk": j.get("Primary_Skill",""),
            "a": j.get("product_area",""),
        })
    out.sort(key=lambda x: -x["r"])
    return out

def build_slack_msgs(verdicts_data, comp_signals):
    msgs = []
    today = date.today().strftime("%b %-d")
    for v in verdicts_data[:4]:
        if v["threat"] not in ("critical","high"): continue
        what  = v.get("what","").replace("\n"," ").strip()[:200]
        why   = v.get("why","").replace("\n"," ").strip()[:150]
        action= v.get("action","").replace("\n"," ").strip()[:150]
        body  = (f"{what}\\n\\n<span class='sy'>Why it matters:</span> {why}\\n"
                 f"<span class='sa'>Actian action:</span> {action}\\n\\n"
                 f"<span class='sl'>→ View full verdict in dashboard</span>")
        msgs.append({"channel":"#actian-competitive-intel","ts":today,"company":v["company"],"label":f"{'CRITICAL' if v['threat']=='critical' else 'HIGH'} THREAT — {v.get('segment','')}","body":body})
    for c in comp_signals:
        if c.get("company") not in V2_COMPANIES: continue
        if c.get("actian_relevance","low") != "high": continue
        ltype = c.get("type","")
        summary = c.get("summary","").replace("\n"," ").strip()[:200]
        body = f"{summary}\\n\\n<span class='sl'>→ View in Launches & Events</span>"
        msgs.append({"channel":"#actian-competitive-intel","ts":today,"company":c["company"],"label":f"{ltype.replace('_',' ').title()} — {c.get('product_area',c.get('company',''))}","body":body})
        if ltype in {"product_launch","event","partnership"}:
            msgs.append({"channel":"@marketing","ts":today,"company":c["company"],"label":f"{ltype.replace('_',' ').title()} — {c.get('product_area',c.get('company',''))}","body":body})
        if len(msgs) >= 8: break
    return msgs[:8]

def replace_js_const(html, name, data):
    json_str = json.dumps(data, ensure_ascii=False, separators=(",",":"))
    pattern = rf"const {name}=\[.*?\];"
    new_html, count = re.subn(pattern, f"const {name}={json_str};", html, flags=re.DOTALL)
    if count == 0: print(f"  ⚠ Could not find 'const {name}=[...]' in template")
    else: print(f"  ✓ {name} ({len(data)} items)")
    return new_html

def patch_stats(html, jobs, signals_data, verdicts_data, comp_signals, news_data):
    total    = len(jobs)
    new_week = sum(1 for j in jobs if int(float(j.get("Days Since Posted",0) or 0)) <= 7)
    high_sig = sum(1 for j in jobs if float(j.get("Relevancy_to_Actian",0) or 0) >= 8)
    directors= sum(1 for j in jobs if j.get("Seniority","") in {"Director+","VP","Executive","Principal/Staff","Director"})
    avg_rel  = sum(float(j.get("Relevancy_to_Actian",0) or 0) for j in jobs) / max(len(jobs),1)
    n_cos    = len(V2_COMPANIES)

    seg_counts = {}
    CO_SEG = {"Atlan":"Data Intelligence","Collibra":"Data Intelligence","Alation":"Data Intelligence",
              "Monte Carlo":"Data Observability","Bigeye":"Data Observability","Acceldata":"Data Observability",
              "Pinecone":"Vector AI","Qdrant":"Vector AI","Milvus":"Vector AI",
              "Snowflake":"AI Analyst","Databricks":"AI Analyst"}
    for j in jobs:
        seg = CO_SEG.get(j.get("Company",""))
        if seg: seg_counts[seg] = seg_counts.get(seg,0) + 1

    threat_counts = {"critical":0,"high":0,"medium":0,"low":0}
    for v in verdicts_data:
        t = v.get("threat","medium").lower()
        if t in threat_counts: threat_counts[t] += 1

    now_utc = datetime.now(timezone.utc).strftime("%b %-d, %Y · %H:%M UTC")

    replacements = [
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
        (f'All Segments <span class="ac">847',   f'All Segments <span class="ac">{total}'),
        (f'Data Intelligence <span class="ac">184', f'Data Intelligence <span class="ac">{seg_counts.get("Data Intelligence",0)}'),
        (f'Data Observability <span class="ac">104', f'Data Observability <span class="ac">{seg_counts.get("Data Observability",0)}'),
        (f'VectorAI <span class="ac">101',   f'VectorAI <span class="ac">{seg_counts.get("Vector AI",0)}'),
        (f'AI Analyst <span class="ac">153', f'AI Analyst <span class="ac">{seg_counts.get("AI Analyst",0)}'),
        ('kpi-value red">2</div><div class="kpi-label">Critical Threats',
         f'kpi-value red">{threat_counts["critical"]}</div><div class="kpi-label">Critical Threats'),
        ('kpi-value amber">3</div><div class="kpi-label">High Threats',
         f'kpi-value amber">{threat_counts["high"]}</div><div class="kpi-label">High Threats'),
        ('kpi-value blue">4</div><div class="kpi-label">Medium Threats',
         f'kpi-value blue">{threat_counts["medium"]}</div><div class="kpi-label">Medium Threats'),
        ('11 companies &middot; Apr 9, 2026 &middot; 06:14 UTC', f'{n_cos} companies &middot; {now_utc}'),
    ]
    replaced = 0
    for old, new in replacements:
        if old in html:
            html = html.replace(old, new, 1)
            replaced += 1
    print(f"  {replaced}/{len(replacements)} stat replacements applied")
    print(f"  Stats: {total} roles, {new_week} new, {high_sig} high-signal, avg {avg_rel:.1f}")
    return html

def patch_pulse(html, jobs, signals_data, verdicts_data):
    from collections import Counter
    sigs_raw = load_json(SIGNALS_J)
    sigs_raw = [s for s in sigs_raw if s.get("company") in V2_COMPANIES]

    criticals = [s for s in sigs_raw if s.get("threat_level")=="critical"]
    highs     = [s for s in sigs_raw if s.get("threat_level")=="high"]

    if len(criticals) >= 3:   mkt_status,mkt_color = "Critical","var(--accent)"
    elif len(criticals) >= 1: mkt_status,mkt_color = "High","#b91c1c"
    elif len(highs) >= 2:     mkt_status,mkt_color = "Elevated","var(--amber)"
    else:                     mkt_status,mkt_color = "Moderate","var(--green)"

    job_cnt = Counter(j.get("Company") for j in jobs)
    mkt_detail_cos = sorted(criticals, key=lambda s: job_cnt.get(s["company"],0), reverse=True)[:3]
    mkt_detail = ", ".join(s["company"] for s in mkt_detail_cos)
    if mkt_detail: mkt_detail += f" — {len(criticals)} critical-level threat{'s' if len(criticals)!=1 else ''}"
    else: mkt_detail = f"{len(highs)} high-threat companies tracked"

    top_threat_co = "—"
    top_threat_detail = "No critical threats detected"
    if criticals:
        top = sorted(criticals, key=lambda s: job_cnt.get(s["company"],0), reverse=True)[0]
        top_threat_co = top["company"]
        j_count = job_cnt.get(top["company"],0)
        threat_sig = top.get("signal_summary","")[:80] if top.get("signal_summary") else ""
        top_threat_detail = f"{j_count} open roles &mdash; {threat_sig}" if threat_sig else f"{j_count} active open roles tracked"
    elif highs:
        top = sorted(highs, key=lambda s: job_cnt.get(s["company"],0), reverse=True)[0]
        top_threat_co = top["company"]
        top_threat_detail = f"{job_cnt.get(top['company'],0)} roles &mdash; high-intensity hiring"

    aiml_jobs  = [j for j in jobs if j.get("Function","") in ("AI/ML","Data Science") or "ML" in j.get("Function","") or "AI" in j.get("Product_Focus","")]
    aiml_count = len(aiml_jobs)
    aiml_cos   = len(set(j.get("Company") for j in aiml_jobs))
    vector_cos = {"Pinecone","Qdrant","Milvus"}
    vec_active = [s for s in sigs_raw if s.get("company") in vector_cos and s.get("hiring_intensity","low") in ("medium","high")]
    vec_names  = ", ".join(sorted(set(s["company"] for s in vec_active))) or "Pinecone, Qdrant, Milvus"
    vec_count  = len(set(s["company"] for s in vec_active)) or len(vector_cos)
    gtm_cos    = set(j.get("Company") for j in jobs if j.get("Function","") in ("Sales","Marketing","Business Development"))
    gtm_count  = len(gtm_cos)
    dir_jobs   = [j for j in jobs if j.get("Seniority","") in ("Director+","VP","Executive")]
    dir_cos    = sorted(set(j.get("Company") for j in dir_jobs), key=lambda c: sum(1 for jj in dir_jobs if jj.get("Company")==c), reverse=True)
    dir_count  = len(dir_cos)
    dir_names  = ", ".join(dir_cos[:5])

    def card(label, value, color, detail):
        return (f'<div class="pulse-card"><div class="pulse-label">{label}</div>'
                f'<div class="pulse-value" style="color:{color}">{value}</div>'
                f'<div class="pulse-detail">{detail}</div></div>')

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
        "    "+card("Market Status", mkt_status, mkt_color, mkt_detail),
        "    "+card("Top Threat", top_threat_co, "#b91c1c", top_threat_detail),
        "    "+card("AI/ML Roles", str(aiml_count), "var(--cyan)", f"AI/ML-specific roles across {aiml_cos} companies"),
        "    "+card("VectorAI Watch", str(vec_count), "var(--purple)", f"{vec_names} &mdash; all actively hiring"),
        "    "+card("GTM Expansion", str(gtm_count), "var(--green)", "Companies with active sales &amp; marketing hiring"),
        "    "+card("Director+ Surge", str(dir_count), "var(--amber)", f"Leadership roles: {dir_names}"),
    ])
    if OLD_ROLES_PULSE in html:
        html = html.replace(OLD_ROLES_PULSE, NEW_ROLES_PULSE, 1)
        print("  ✓ Roles page pulse updated")
    else:
        print("  ⚠ Roles pulse block not found — skipped")

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
        "    "+card("Market Pulse", mkt_status, mkt_color, mkt_detail),
        "    "+card("Top Threat", top_threat_co, "#b91c1c", top_threat_detail),
        "    "+card("AI/ML Roles", str(aiml_count), "var(--cyan)", f"AI/ML-specific roles across {aiml_cos} companies"),
        "    "+card("VectorAI Watch", str(vec_count), "var(--purple)", f"{vec_names} &mdash; all actively hiring"),
    ])
    if OLD_SIGS_PULSE in html:
        html = html.replace(OLD_SIGS_PULSE, NEW_SIGS_PULSE, 1)
        print("  ✓ Signals page pulse updated")
    else:
        print("  ⚠ Signals pulse block not found — skipped")

    return html


def main():
    print("=" * 70)
    print("build_dashboard_v21.py — v2.1 CI Dashboard Builder")
    print("=" * 70)

    if not TEMPLATE.exists():
        print(f"✗ Template not found: {TEMPLATE}")
        return False

    jobs         = load_csv(JOBS_CSV)
    raw_signals  = load_json(SIGNALS_J)
    raw_verdicts = load_json(VERDICTS_J)
    comp_signals = load_json(COMP_SIG_J)
    raw_news     = load_json(NEWS_J)

    raw_signals  = [s for s in raw_signals  if s.get("company") in V2_COMPANIES]
    raw_verdicts = [v for v in raw_verdicts  if v.get("company") in V2_COMPANIES]
    comp_signals = [c for c in comp_signals  if c.get("company") in V2_COMPANIES]
    raw_news     = [n for n in raw_news      if n.get("company") in V2_COMPANIES]

    print(f"\n  Jobs: {len(jobs)} | Signals: {len(raw_signals)} | Verdicts: {len(raw_verdicts)} | Comp signals: {len(comp_signals)} | News: {len(raw_news)}")

    signals_by_company  = {s["company"]: s for s in raw_signals}
    verdicts_by_company = {v["company"]: v for v in raw_verdicts}

    signals_data  = build_signals(raw_signals, jobs, verdicts_by_company)
    verdicts_data = build_verdicts(raw_verdicts, signals_by_company)
    launches_data = build_launches(comp_signals)
    news_data     = build_news(raw_news)
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
    html = replace_js_const(html, "NEWS",       news_data)

    print("\nPatching stats:")
    html = patch_stats(html, jobs, signals_data, verdicts_data, comp_signals, news_data)
    print("\nPatching pulse cards:")
    html = patch_pulse(html, jobs, signals_data, verdicts_data)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Written: {OUTPUT}  ({len(html)/1024:.1f} KB)")
    print("=" * 70)
    return True

if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
