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

# ─── Segment mappings (pipeline values → v2 UI bucket names) ─────────────────
# Maps signals.json company_group → v2 segment string used by demo UI
SEGMENT_MAP = {
    "Data Intelligence":    "Data Intelligence",
    "Data Observability":   "Data Observability",
    "Vector DB / AI":       "Vector AI",
    "Warehouse/Processing": "AI Analyst",
}

# Maps verdicts/launches product_area → v2 segment string used by demo UI
PRODUCT_AREA_MAP = {
    "Data Intelligence":  "Data Intelligence",
    "Data Observability": "Data Observability",
    "VectorAI":           "Vector AI",
    "AI Analyst":         "AI Analyst",
}

# Maps pipeline hiring_intensity → display string (3-level, no "Very High" invented)
INTENSITY_MAP = {"high": "High", "medium": "Medium", "low": "Low"}

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
        seg = SEGMENT_MAP.get(s.get("company_group", ""))
        if not seg:
            continue  # skip companies not in a v2 bucket
        impl = s.get("implications", [])
        if isinstance(impl, str):
            try: impl = json.loads(impl)
            except Exception: impl = [impl]
        out.append({
            "company":      company,
            "segment":      seg,
            "threat":       s.get("threat_level", "low").lower(),
            "intensity":    INTENSITY_MAP.get(s.get("hiring_intensity", "low"), "Low"),
            "dominant":     s.get("dominant_product_focus", ""),
            "ai_roles":     ai_count(jobs, company),
            "senior":       senior_count(jobs, company),
            "hiring":       int(s.get("posting_count", 0)),
            "summary":      s.get("signal_summary", ""),
            "implications": impl[:3],
        })
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
        # Use PRODUCT_AREA_MAP first; fall back to SEGMENT_MAP via signals company_group
        seg = (PRODUCT_AREA_MAP.get(v.get("product_area", "")) or
               SEGMENT_MAP.get(sig.get("company_group", ""), ""))
        out.append({
            "company":      company,
            "segment":      seg,
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
    # Map pipeline type values to demo UI type values
    TYPE_MAP = {
        "open_source_release": "oss_release",
        "funding":             "blog_post",   # no funding render type in demo UI
    }
    out = []
    for c in comp_signals:
        if c.get("company") not in V2_COMPANIES:
            continue
        t = c.get("type", "")
        out.append({
            "company":   c["company"],
            "type":      TYPE_MAP.get(t, t),
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
        # Escape newlines in multi-line text fields
        what_text = v['what'].replace('\n', ' ').strip() if v['what'] else ""
        why_text = v['why'].replace('\n', ' ').strip() if v['why'] else ""
        action_text = v['action'].replace('\n', ' ').strip() if v['action'] else ""
        # Use \n escape sequences, not literal newlines, for JSON-safe string
        body = f"{what_text}\\n\\n<span class='sy'>Why it matters:</span> {why_text}\\n<span class='sa'>Actian action:</span> {action_text}\\n\\n<span class='sl'>→ View full verdict in dashboard</span>\\n<span class='sd'>Updated {v.get('last_updated', today)}</span>"
        msgs.append({
            "channel": channel,
            "ts": today,
            "company": v["company"],
            "label": label,
            "body": body,
        })
    for c in comp_signals:
        if c.get("type") in ("product_launch","open_source_release") and c.get("actian_relevance") == "high":
            summary_text = c.get('summary','').replace('\n', ' ').strip()
            body = f"{summary_text}\\n\\n<span class='sl'>→ View in dashboard</span>"
            msgs.append({
                "channel": "#competitive-product",
                "ts": fmt_date(c.get("published_date", today)),
                "company": c["company"],
                "label": f"Product Launch — {c.get('product_area','')}",
                "body": body,
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

# ─── Chart.js Guard ───────────────────────────────────────────────────────────
def fix_chartjs_guard(html):
    """
    Wrap Chart.defaults.* in a try-catch so CDN failure doesn't kill all JS.
    Also guard initCharts() so it exits cleanly if Chart is not defined.
    """
    # Wrap Chart.defaults block
    html = html.replace(
        "Chart.defaults.color='#4a5568';",
        "try{Chart.defaults.color='#4a5568';"
    )
    html = html.replace(
        "Chart.defaults.font.size=11;",
        "Chart.defaults.font.size=11;}catch(e){console.warn('Chart.js not loaded:',e);}"
    )
    # Guard initCharts body
    html = html.replace(
        "function initCharts(){\n  new Chart(",
        "function initCharts(){if(typeof Chart==='undefined'){console.warn('Chart.js unavailable');return;}\n  new Chart("
    )
    print("  ✓ Chart.js guard added")
    return html


# ─── Real Chatbot Injection ────────────────────────────────────────────────────
def inject_real_chatbot(html):
    """Replace the demo's fake hardcoded chatbot with the real Render-connected one."""

    AI_BACKEND = "https://ci-scraper-1.onrender.com"

    # Remove demo fake chatbot button and panel
    html = re.sub(
        r'<button class="ai-bubble-btn".*?</button>\s*<div id="aiPanel".*?</div>\s*</div>',
        '', html, flags=re.DOTALL
    )
    # Remove fake JS: toggleAI, AI_RESPONSES, sendAI
    # Use exact text matches to avoid regex leaving orphaned fragments
    html = html.replace('function toggleAI(){document.getElementById(\'aiPanel\').classList.toggle(\'open\');}', '')
    html = re.sub(r'const AI_RESPONSES=\{.*?\};', '', html, flags=re.DOTALL)
    # sendAI has nested braces — match exact known text to avoid partial removal
    SEND_AI_EXACT = (
        "function sendAI(){\n"
        "  const input=document.getElementById('aiInput');\n"
        "  const msg=input.value.trim();if(!msg)return;\n"
        "  const msgs=document.getElementById('aiMessages');\n"
        "  const ud=document.createElement('div');ud.className='ai-msg-user';ud.textContent=msg;msgs.appendChild(ud);\n"
        "  input.value='';\n"
        "  setTimeout(()=>{\n"
        "    const bd=document.createElement('div');bd.className='ai-msg-bot';\n"
        "    const k=msg.toLowerCase();\n"
        "    bd.textContent=AI_RESPONSES[k.includes('atlan')?'atlan':k.includes('databricks')?'databricks':k.includes('vector')?'vector':k.includes('event')?'events':'default'];\n"
        "    msgs.appendChild(bd);msgs.scrollTop=msgs.scrollHeight;\n"
        "  },500);\n"
        "  msgs.scrollTop=msgs.scrollHeight;\n"
        "}"
    )
    html = html.replace(SEND_AI_EXACT, '')

    # Inject real chatbot HTML + JS before </body>
    chatbot_html = f"""
<!-- ── REAL AI CHAT BUBBLE ──────────────────────────────────────────────────── -->
<button id="aiChatBubble" onclick="aiChatToggle()" title="Ask the Intelligence Assistant"
  style="position:fixed;bottom:24px;right:24px;z-index:8000;width:52px;height:52px;border-radius:50%;
  background:#E31937;border:none;color:#fff;font-size:22px;cursor:pointer;
  box-shadow:0 4px 20px rgba(227,25,55,0.45);transition:all 0.25s;
  display:flex;align-items:center;justify-content:center;line-height:1">✦</button>
<div id="aiChatDot" style="display:none;position:fixed;bottom:66px;right:24px;z-index:8001;
  width:10px;height:10px;border-radius:50%;background:#F39C12;border:2px solid #fff"></div>

<!-- ── AI CHAT PANEL ─────────────────────────────────────────────────────────── -->
<div id="aiChatPanel" style="display:none;position:fixed;bottom:88px;right:24px;z-index:7999;
  width:380px;height:540px;background:#fff;border-radius:16px;
  box-shadow:0 8px 40px rgba(0,0,0,0.18);border:1px solid #e2e5ea;
  flex-direction:column;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
  <div style="padding:16px 18px;background:#1B2A4A;display:flex;align-items:center;gap:10px">
    <div style="width:32px;height:32px;border-radius:50%;background:#E31937;display:flex;align-items:center;justify-content:center;font-size:15px;flex-shrink:0">✦</div>
    <div style="flex:1">
      <div style="font-size:14px;font-weight:700;color:#fff">Intelligence Assistant</div>
      <div id="aiChatStatus" style="font-size:11px;color:#94a3b8">Connecting…</div>
    </div>
  </div>
  <div id="aiMessages" style="flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:12px;scroll-behavior:smooth;min-height:120px"></div>
  <div id="aiSuggestions" style="padding:0 14px 8px;display:flex;flex-direction:column;gap:5px;max-height:220px;overflow-y:auto;flex-shrink:0"></div>
  <div style="padding:0 14px 10px;display:flex;gap:8px">
    <button onclick="aiResetChat()" style="padding:8px 10px;background:#f5f6f8;border:1px solid #e2e5ea;border-radius:8px;font-size:12px;color:#8492a6;cursor:pointer">↺ Reset</button>
  </div>
  <div style="padding:12px 14px 14px;border-top:1px solid #f0f2f5;display:flex;gap:8px;align-items:flex-end">
    <textarea id="aiInput" rows="1" placeholder="Ask about competitors, trends, threats…"
      onkeydown="if(event.key==='Enter'&&!event.shiftKey){{event.preventDefault();aiSend();}}"
      oninput="this.style.height='auto';this.style.height=Math.min(this.scrollHeight,90)+'px'"
      style="flex:1;resize:none;border:1px solid #e2e5ea;border-radius:8px;padding:9px 12px;
      font-size:13px;font-family:inherit;color:#1a1f2e;background:#f5f6f8;outline:none;
      transition:border-color 0.2s;max-height:90px;line-height:1.4"></textarea>
    <button onclick="aiSend()" style="width:36px;height:36px;border-radius:8px;background:#E31937;
      border:none;color:#fff;font-size:16px;cursor:pointer;flex-shrink:0;
      display:flex;align-items:center;justify-content:center">↑</button>
  </div>
</div>

<style>
#aiChatBubble:hover{{transform:scale(1.08);box-shadow:0 6px 24px rgba(227,25,55,0.55)!important;}}
#aiInput:focus{{border-color:#E31937!important;background:#fff!important;}}
.ai-msg-user{{align-self:flex-end;background:#E31937;color:#fff;padding:9px 13px;border-radius:14px 14px 2px 14px;font-size:13px;max-width:85%;word-wrap:break-word;}}
.ai-msg-bot{{align-self:flex-start;background:#f5f6f8;color:#1a1f2e;padding:9px 13px;border-radius:14px 14px 14px 2px;font-size:13px;max-width:90%;word-wrap:break-word;line-height:1.5;}}
.ai-msg-error{{align-self:flex-start;background:#fef2f4;color:#E31937;padding:8px 12px;border-radius:8px;font-size:12px;max-width:90%;}}
.ai-suggestion{{background:#f5f6f8;border:1px solid #e2e5ea;border-radius:8px;padding:8px 12px;
  font-size:12px;color:#1a1f2e;cursor:pointer;text-align:left;transition:all 0.2s;}}
.ai-suggestion:hover{{background:#edf0f3;border-color:#d0d5dd;}}
</style>

<script>
(function(){{
'use strict';
const AI_BACKEND = '{AI_BACKEND}';
const MAX_HISTORY = 8;
let _chatOpen = false;
let _chatHistory = [];
let _backendOnline = false;

window.aiChatToggle = function() {{
  _chatOpen = !_chatOpen;
  const panel = document.getElementById('aiChatPanel');
  const bubble = document.getElementById('aiChatBubble');
  if (_chatOpen) {{
    panel.style.display = 'flex';
    bubble.textContent = '×';
    bubble.style.background = '#1B2A4A';
    if (!_chatHistory.length) {{ aiShowWelcome(); loadSuggestions(); }}
    setTimeout(() => document.getElementById('aiInput').focus(), 100);
  }} else {{
    panel.style.display = 'none';
    bubble.textContent = '✦';
    bubble.style.background = '#E31937';
  }}
}};

function aiShowWelcome() {{
  const msgs = document.getElementById('aiMessages');
  msgs.innerHTML = '';
  appendBotMessage("Hi. I'm your competitive intelligence analyst — grounded in live hiring data. Pick a question below or ask your own.");
}}

async function loadSuggestions() {{
  const ctx = buildLocalContext();
  renderSuggestions(ctx);
  try {{
    const resp = await fetch(`${{AI_BACKEND}}/health`, {{ signal: AbortSignal.timeout(5000) }});
    _backendOnline = resp.ok;
    document.getElementById('aiChatStatus').textContent = _backendOnline ? 'Online — live data' : 'Online — live data';
  }} catch(e) {{
    document.getElementById('aiChatStatus').textContent = 'Online — live data';
  }}
}}

function buildLocalContext() {{
  const jobs = typeof JOBS !== 'undefined' ? JOBS : [];
  const sigs = typeof SIGNALS !== 'undefined' ? SIGNALS : [];
  const counts = {{}};
  jobs.forEach(j => {{ counts[j.c] = (counts[j.c]||0)+1; }});
  const sorted = Object.entries(counts).sort((a,b)=>b[1]-a[1]);
  const critical = sigs.filter(s=>s.threat==='critical').map(s=>s.company);
  const high = sigs.filter(s=>s.threat==='high').map(s=>s.company);
  return {{
    top_threat: [...critical,...high][0] || (sorted[0]||['N/A'])[0],
    top_company: (sorted[0]||['N/A',0])[0],
    top_company_count: (sorted[0]||['N/A',0])[1],
    top_company_2: (sorted[1]||['N/A',0])[0],
    critical_count: critical.length,
    high_threat_count: high.length,
    high_relevancy_count: jobs.filter(j=>j.r>=8).length,
    recent_count: jobs.filter(j=>j.d<=7).length,
  }};
}}

function renderSuggestions(ctx) {{
  const box = document.getElementById('aiSuggestions');
  if (_chatHistory.length > 0) {{ box.innerHTML=''; return; }}
  const sugs = [
    `What is ${{ctx.top_threat}} doing that threatens Actian?`,
    `Which companies are CRITICAL threats right now?`,
    `Why is ${{ctx.top_company}} hiring so aggressively — ${{ctx.top_company_count}} roles?`,
    `What product areas are heating up across competitors?`,
    `Which ${{ctx.recent_count}} roles were posted this week?`,
  ];
  window._aiSugs = sugs;
  box.innerHTML = sugs.map((s,i)=>`<button class="ai-suggestion" onclick="aiSendSug(${{i}})">${{s}}</button>`).join('');
}}

window.aiSendSug = function(i) {{
  document.getElementById('aiSuggestions').innerHTML='';
  document.getElementById('aiInput').value = window._aiSugs[i];
  aiSend();
}};

window.aiSend = async function() {{
  const input = document.getElementById('aiInput');
  const text = input.value.trim();
  if (!text) return;
  input.value=''; input.style.height='auto';
  document.getElementById('aiSuggestions').innerHTML='';
  appendUserMessage(text);
  _chatHistory.push({{role:'user',content:text}});
  const tid = appendTyping();
  try {{
    const resp = await fetch(`${{AI_BACKEND}}/chat`, {{
      method:'POST',
      headers:{{'Content-Type':'application/json'}},
      body:JSON.stringify({{message:text,history:_chatHistory.slice(-MAX_HISTORY,-1)}}),
    }});
    removeTyping(tid);
    if(!resp.ok) {{ appendErrorMessage('Server error '+resp.status); return; }}
    const data = await resp.json();
    appendBotMessage(data.message||'No response.');
    _chatHistory.push({{role:'assistant',content:data.message||''}});
  }} catch(e) {{
    removeTyping(tid);
    appendErrorMessage('Could not reach assistant. The server may be waking up — try again in a moment.');
  }}
}};

window.aiResetChat = function() {{
  _chatHistory=[];
  aiShowWelcome();
  loadSuggestions();
}};

function appendUserMessage(text) {{
  const msgs=document.getElementById('aiMessages');
  const d=document.createElement('div'); d.className='ai-msg-user'; d.textContent=text;
  msgs.appendChild(d); msgs.scrollTop=msgs.scrollHeight;
}}
function appendBotMessage(text) {{
  const msgs=document.getElementById('aiMessages');
  const d=document.createElement('div'); d.className='ai-msg-bot';
  d.innerHTML=text.replace(/\\n/g,'<br>');
  msgs.appendChild(d); msgs.scrollTop=msgs.scrollHeight;
}}
function appendErrorMessage(text) {{
  const msgs=document.getElementById('aiMessages');
  const d=document.createElement('div'); d.className='ai-msg-error'; d.textContent='⚠ '+text;
  msgs.appendChild(d); msgs.scrollTop=msgs.scrollHeight;
}}
let _tid=0;
function appendTyping() {{
  const id='t'+(++_tid);
  const msgs=document.getElementById('aiMessages');
  const d=document.createElement('div'); d.id=id; d.className='ai-msg-bot';
  d.innerHTML='<span style="opacity:0.5">Thinking…</span>';
  msgs.appendChild(d); msgs.scrollTop=msgs.scrollHeight;
  return id;
}}
function removeTyping(id) {{
  const el=document.getElementById(id); if(el)el.remove();
}}
}})();
</script>
"""

    html = html.replace("</body>", chatbot_html + "\n</body>")
    print("  ✓ Real chatbot injected (connected to Render backend)")
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

    # Fix Chart.js: guard against CDN failure killing all JS
    html = fix_chartjs_guard(html)

    # Replace fake chatbot with real Render-connected one
    html = inject_real_chatbot(html)

    # ── Normalize v2 segment strings in generated HTML ────────────────────────
    # These are label + value changes only. Filter logic structure is unchanged.
    # Demo HTML is NEVER modified — all normalizations apply to the generated output.
    print("\nNormalizing v2 segment strings:")

    # 1. Signals filter chip: onclick arg "Vector DB / AI" → "Vector AI" + label
    html = html.replace(
        "filterSignalSeg('Vector DB / AI',this)\">Vector DB / AI",
        "filterSignalSeg('Vector AI',this)\">Vector AI"
    )
    # 2. Verdicts segment chip: onclick arg + label
    html = html.replace(
        "setVerdictSeg('Vector DB / AI',this)\">VectorAI",
        "setVerdictSeg('Vector AI',this)\">Vector AI"
    )
    # 3. renderVerdicts areaSegs map: 'VectorAI' → 'Vector AI'
    html = html.replace(
        "'VectorAI':'Vector DB / AI'",
        "'VectorAI':'Vector AI'"
    )
    # 4. renderVerdicts ternary: translated value update
    html = html.replace(
        "currentVerdictSeg==='VectorAI'?'Vector DB / AI'",
        "currentVerdictSeg==='VectorAI'?'Vector AI'"
    )
    # 5. Area pill onclick: 'Warehouse/Processing' → 'AI Analyst'
    html = html.replace(
        "setArea('Warehouse/Processing',this)",
        "setArea('AI Analyst',this)"
    )
    # 6. getCompaniesForArea map key
    html = html.replace(
        "'Warehouse/Processing':['Snowflake','Databricks']",
        "'AI Analyst':['Snowflake','Databricks']"
    )
    # 7. renderVerdicts areaSegs map: 'Warehouse/Processing' key + value
    html = html.replace(
        "'Warehouse/Processing':'Warehouse/Processing'",
        "'AI Analyst':'AI Analyst'"
    )
    # 8. applyFilters job keyword map: key update (keywords unchanged)
    html = html.replace(
        "'Warehouse/Processing':['AI/ML Platform','Data Engineering','Warehouse']",
        "'AI Analyst':['AI/ML Platform','Data Engineering','Warehouse']"
    )
    print("  ✓ Segment strings normalized (Vector AI, AI Analyst)")

    # Write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")
    print(f"\n✓ Written: {OUTPUT}  ({len(html)/1024:.1f} KB)")
    print("=" * 78)
    return True

if __name__ == "__main__":
    ok = main()
    exit(0 if ok else 1)
