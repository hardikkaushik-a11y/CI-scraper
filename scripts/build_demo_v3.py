#!/usr/bin/env python3
"""
build_demo_v3.py — Build demo.html for CV showcase.

Reads from demo_data/ (fake companies), writes dashboard/v3/demo.html.
Run generate_demo_data.py first if demo_data/ is empty.
"""

import sys
from pathlib import Path

REPO = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))

import build_dashboard_v3 as bld

# ── Demo overrides ────────────────────────────────────────────────────────────

DEMO_DIR = REPO / "demo_data"
DEMO_OUTPUT = REPO / "dashboard" / "v3" / "demo.html"

DEMO_PRODUCT_AREA_MAP = {
    "Nexora":       "AI Data Fabric",
    "Polaria":      "Data Intelligence",
    "Vextrix":      "Data Observability",
    "Orbital Labs": "Data Governance",
    "Quanthorne":   "Real-Time Analytics",
    "Stratify":     "MLOps",
    "Krytex":       "Vector / AI",
    "Lumiqa":       "BI & Analytics",
}

DEMO_AREA_TOKENS = {
    "AI Data Fabric":    {"fg": "oklch(0.44 0.13 245)", "bg": "oklch(0.96 0.03 245)", "bd": "oklch(0.88 0.06 245)", "dot": "oklch(0.56 0.14 245)"},
    "Data Intelligence": {"fg": "oklch(0.42 0.14 270)", "bg": "oklch(0.96 0.03 270)", "bd": "oklch(0.88 0.06 270)", "dot": "oklch(0.55 0.16 270)"},
    "Data Observability":{"fg": "oklch(0.42 0.10 195)", "bg": "oklch(0.96 0.03 195)", "bd": "oklch(0.88 0.06 195)", "dot": "oklch(0.55 0.12 195)"},
    "Data Governance":   {"fg": "oklch(0.44 0.12 140)", "bg": "oklch(0.96 0.03 140)", "bd": "oklch(0.88 0.06 140)", "dot": "oklch(0.56 0.14 140)"},
    "Real-Time Analytics":{"fg": "oklch(0.46 0.12 65)", "bg": "oklch(0.97 0.03 65)",  "bd": "oklch(0.88 0.08 65)",  "dot": "oklch(0.66 0.15 65)"},
    "MLOps":             {"fg": "oklch(0.44 0.14 310)", "bg": "oklch(0.96 0.03 310)", "bd": "oklch(0.88 0.06 310)", "dot": "oklch(0.56 0.16 310)"},
    "Vector / AI":       {"fg": "oklch(0.44 0.14 310)", "bg": "oklch(0.96 0.03 310)", "bd": "oklch(0.88 0.06 310)", "dot": "oklch(0.56 0.16 310)"},
    "BI & Analytics":    {"fg": "oklch(0.48 0.01 250)", "bg": "oklch(0.97 0.005 250)","bd": "oklch(0.90 0.01 250)", "dot": "oklch(0.68 0.01 250)"},
}

# ── Brand substitutions in final HTML ────────────────────────────────────────
BRAND_SUBS = [
    # Company name (order matters — most specific first)
    ("Actian CI Platform",     "CI Platform"),
    ("Actian CI",              "CI Platform"),
    ("Actian's",               "Axon Analytics'"),
    ("Actian ",                "Axon Analytics "),
    # Sidebar footer
    ("Morgan Reyes",           "Alex Chen"),
    ("Head of CI · Actian",   "Head of CI · Axon Analytics"),
    # Sidebar subtitle
    ("Competitive Intelligence", "Competitive Intelligence"),  # keep as-is
    # Companies roster count
    ("Roster of 11",           "Roster of 8"),
    # Data eyebrow labels
    ("V2 roster · ",           "Demo roster · "),
    # Chat backend — use /chat-demo endpoint
    ('"/chat"',                '"/chat-demo"'),
    ("}/chat`,",               "}/chat-demo`,"),
    # Hero eyebrow
    ("V2 tracked roster · 11 companies", "Demo · 8 companies"),
    # KpiRow hardcoded sub-labels (approximate)
    ("Atlan · Snowflake",      "Nexora · Polaria"),
    ("6 product · 1 pricing",  "5 product · 2 events"),
    ("Activate · Summit · ALLIE day", "Nexora Summit · Orbital Forum"),
    ("Assigned to 4 teams",    "Assigned to 4 teams"),
]

# ── Patch build module globals ────────────────────────────────────────────────
bld.DATA_DIR          = DEMO_DIR
bld.OUTPUT            = DEMO_OUTPUT
bld.V2_PRODUCT_AREA_MAP = DEMO_PRODUCT_AREA_MAP
bld.AREA_TOKENS       = DEMO_AREA_TOKENS


# ── Override main() to use demo file names ────────────────────────────────────
def demo_main():
    print("build_demo_v3.py — building demo.html...")

    signals     = bld.load_json(DEMO_DIR / "signals_demo.json")
    verdicts    = bld.load_json(DEMO_DIR / "verdicts_demo.json")
    comp_sigs   = bld.load_json(DEMO_DIR / "competitive_signals_demo.json")
    news        = bld.load_json(DEMO_DIR / "news_demo.json")

    print(f"  {len(signals)} companies · {len(verdicts)} verdicts · "
          f"{len(comp_sigs)} competitive signals · {len(news)} news")

    allowed = list(DEMO_PRODUCT_AREA_MAP.keys())
    per_company   = bld.load_function_breakdown(DEMO_DIR / "jobs_demo.csv", allowed)
    function_trends = bld.build_function_trends(per_company, allowed)

    print("  Building COMPETITORS...")
    competitors = bld.build_competitors(
        signals, verdicts, per_company,
        comp_signals=comp_sigs, news=news
    )
    for c in competitors:
        print(f"    - {c['name']:20s} {c['threat']:8s}  {c['postingCount']:3d} postings")

    print("  Building LAUNCHES + EVENTS...")
    launches, events = bld.build_launches_events(comp_sigs)
    print(f"    {len(launches)} launches, {len(events)} events")

    print("  Generating JS data block...")
    data_js = bld.generate_data_js(competitors, launches, events, function_trends, news)

    template_html = bld.TEMPLATE.read_text(encoding="utf-8")
    if "%%DATA_JSON%%" not in template_html:
        print("ERROR: %%DATA_JSON%% placeholder not found!", file=sys.stderr)
        sys.exit(1)

    output_html = template_html.replace("%%DATA_JSON%%", data_js)

    # Apply brand substitutions
    for old, new in BRAND_SUBS:
        output_html = output_html.replace(old, new)

    # Add demo banner CSS + element just before </body>
    demo_banner = """
  <style>
    .demo-banner {
      position: fixed; bottom: 16px; right: 16px; z-index: 9999;
      background: oklch(0.18 0.02 250); color: oklch(0.85 0.05 250);
      border: 1px solid oklch(0.30 0.04 250);
      border-radius: 8px; padding: 8px 14px;
      font-family: ui-monospace, monospace; font-size: 11px;
      letter-spacing: 0.04em; pointer-events: none;
      box-shadow: 0 2px 12px oklch(0 0 0 / 0.3);
    }
    .demo-banner strong { color: oklch(0.72 0.10 245); }
  </style>
  <div class="demo-banner"><strong>DEMO</strong> · synthetic data · AI-powered CI platform</div>
"""
    output_html = output_html.replace("</body>", demo_banner + "</body>")

    DEMO_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    DEMO_OUTPUT.write_text(output_html, encoding="utf-8")
    size_kb = DEMO_OUTPUT.stat().st_size / 1024
    print(f"\n  Done! Output: {DEMO_OUTPUT}")
    print(f"  File size: {size_kb:.1f} KB")


if __name__ == "__main__":
    demo_main()
