#!/usr/bin/env python3
"""
generate_demo_data.py — Create all demo_data/ files for the CV demo dashboard.

Outputs:
  demo_data/jobs_demo.csv
  demo_data/signals_demo.json
  demo_data/verdicts_demo.json
  demo_data/news_demo.json
  demo_data/competitive_signals_demo.json
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

REPO_ROOT = Path(__file__).parent.parent
OUT_DIR   = REPO_ROOT / "demo_data"
OUT_DIR.mkdir(exist_ok=True)

TODAY = datetime.today()

# ── Company definitions ────────────────────────────────────────────────────
COMPANIES = [
    {
        "name": "Nexora",
        "group": "AI Data Fabric",
        "threat": "critical",
        "intensity": "HIGH",
        "roles": 240,
        "area": "AI Data Fabric",
        "dominant_function": "Engineering",
        "dominant_product": "AI/ML Pipeline",
        "description": "AI-native data fabric platform with LLM integrations",
        "ai_pct": 0.28,
        "eng_pct": 0.52,
        "senior_pct": 0.42,
        "gtm_pct": 0.15,
        "velocity": 185,
    },
    {
        "name": "Polaria",
        "group": "Data Intelligence",
        "threat": "critical",
        "intensity": "HIGH",
        "roles": 210,
        "area": "Data Intelligence",
        "dominant_function": "Engineering",
        "dominant_product": "Metadata Catalog",
        "description": "Enterprise metadata catalog and data governance platform",
        "ai_pct": 0.18,
        "eng_pct": 0.48,
        "senior_pct": 0.38,
        "gtm_pct": 0.20,
        "velocity": 162,
    },
    {
        "name": "Vextrix",
        "group": "Data Observability",
        "threat": "high",
        "intensity": "HIGH",
        "roles": 175,
        "area": "Data Observability",
        "dominant_function": "Engineering",
        "dominant_product": "Observability",
        "description": "Data observability and pipeline monitoring SaaS",
        "ai_pct": 0.14,
        "eng_pct": 0.50,
        "senior_pct": 0.35,
        "gtm_pct": 0.22,
        "velocity": 148,
    },
    {
        "name": "Orbital Labs",
        "group": "Data Governance",
        "threat": "high",
        "intensity": "HIGH",
        "roles": 155,
        "area": "Data Governance",
        "dominant_function": "Engineering",
        "dominant_product": "Data Governance",
        "description": "Automated data governance and compliance platform",
        "ai_pct": 0.16,
        "eng_pct": 0.46,
        "senior_pct": 0.40,
        "gtm_pct": 0.24,
        "velocity": 140,
    },
    {
        "name": "Quanthorne",
        "group": "Real-Time Analytics",
        "threat": "high",
        "intensity": "MEDIUM",
        "roles": 130,
        "area": "Real-Time Analytics",
        "dominant_function": "Engineering",
        "dominant_product": "Streaming Analytics",
        "description": "Real-time streaming analytics and event processing engine",
        "ai_pct": 0.12,
        "eng_pct": 0.55,
        "senior_pct": 0.32,
        "gtm_pct": 0.18,
        "velocity": 125,
    },
    {
        "name": "Stratify",
        "group": "MLOps",
        "threat": "medium",
        "intensity": "MEDIUM",
        "roles": 95,
        "area": "MLOps",
        "dominant_function": "AI/ML & Vector",
        "dominant_product": "Model Serving",
        "description": "MLOps platform for model deployment and monitoring",
        "ai_pct": 0.35,
        "eng_pct": 0.42,
        "senior_pct": 0.30,
        "gtm_pct": 0.14,
        "velocity": 108,
    },
    {
        "name": "Krytex",
        "group": "Vector DB",
        "threat": "medium",
        "intensity": "MEDIUM",
        "roles": 85,
        "area": "Vector / AI",
        "dominant_function": "Engineering",
        "dominant_product": "Vector Search",
        "description": "Vector database and semantic search infrastructure",
        "ai_pct": 0.40,
        "eng_pct": 0.48,
        "senior_pct": 0.28,
        "gtm_pct": 0.12,
        "velocity": 115,
    },
    {
        "name": "Lumiqa",
        "group": "BI & Analytics",
        "threat": "low",
        "intensity": "LOW",
        "roles": 60,
        "area": "BI & Analytics",
        "dominant_function": "Engineering",
        "dominant_product": "Business Intelligence",
        "description": "Self-service BI and embedded analytics platform",
        "ai_pct": 0.08,
        "eng_pct": 0.45,
        "senior_pct": 0.25,
        "gtm_pct": 0.28,
        "velocity": 85,
    },
]

# ── Role templates per function ────────────────────────────────────────────
ROLE_TEMPLATES = {
    "Engineering": [
        ("Senior Software Engineer, Data Platform", "Senior", "Data Platform"),
        ("Staff Engineer, Infrastructure", "Principal/Staff", "Cloud Infra"),
        ("Software Engineer II, Backend", "IC", "Backend Systems"),
        ("Senior Engineer, APIs & Integrations", "Senior", "ETL/Integration"),
        ("Principal Engineer, Distributed Systems", "Principal/Staff", "Data Platform"),
        ("Engineering Manager, Core Platform", "Manager", "Data Platform"),
        ("Software Engineer, Frontend", "IC", "UI/UX"),
        ("Senior Software Engineer, Data Pipelines", "Senior", "ETL/Integration"),
        ("Director of Engineering, Platform", "Director+", "Data Platform"),
        ("Site Reliability Engineer", "IC", "Cloud Infra"),
    ],
    "AI/ML & Vector": [
        ("Senior Machine Learning Engineer", "Senior", "AI/ML Pipeline"),
        ("ML Research Scientist", "Senior", "AI/ML Pipeline"),
        ("AI Product Engineer, LLM Features", "IC", "Vector / AI"),
        ("Staff ML Engineer, Model Infrastructure", "Principal/Staff", "AI/ML Pipeline"),
        ("Applied Scientist, NLP", "Senior", "AI/ML Pipeline"),
        ("ML Engineer, Vector Search", "IC", "Vector / AI"),
        ("Director of AI, Platform", "Director+", "AI/ML Pipeline"),
        ("Data Science Manager", "Manager", "Data/Analytics"),
    ],
    "Product Management": [
        ("Product Manager, Core Platform", "IC", "Data Platform"),
        ("Senior PM, Data Intelligence", "Senior", "Data Governance"),
        ("Director of Product, AI Features", "Director+", "AI/ML Pipeline"),
        ("Group Product Manager, Enterprise", "Manager", "Data Platform"),
        ("VP of Product", "Director+", "Data Platform"),
        ("Principal PM, Developer Experience", "Principal/Staff", "Data Platform"),
    ],
    "Sales": [
        ("Enterprise Account Executive", "IC", "Data Platform"),
        ("Senior Sales Engineer", "Senior", "ETL/Integration"),
        ("Regional Vice President, Sales", "Director+", "Data Platform"),
        ("Commercial Account Executive", "IC", "Data Platform"),
        ("Director of Enterprise Sales, EMEA", "Director+", "Data Platform"),
        ("Strategic Account Manager", "Senior", "Data Platform"),
    ],
    "Solution Engineering": [
        ("Solutions Engineer, Enterprise", "IC", "ETL/Integration"),
        ("Senior Solutions Consultant", "Senior", "Data Platform"),
        ("Technical Presales Engineer", "IC", "Data Governance"),
        ("Solutions Architect, AI", "Senior", "AI/ML Pipeline"),
        ("Staff Solutions Engineer", "Principal/Staff", "Data Platform"),
    ],
    "Marketing": [
        ("Product Marketing Manager", "IC", "Data Platform"),
        ("Senior PMM, AI & Platform", "Senior", "AI/ML Pipeline"),
        ("Director of Product Marketing", "Director+", "Data Platform"),
        ("Demand Generation Manager", "IC", "Data Platform"),
        ("Field Marketing Manager", "IC", "Data Platform"),
    ],
    "Customer Success": [
        ("Customer Success Manager", "IC", "Data Platform"),
        ("Senior CSM, Enterprise Accounts", "Senior", "Data Platform"),
        ("Director of Customer Success", "Director+", "Data Platform"),
        ("Technical Account Manager", "IC", "ETL/Integration"),
    ],
    "Data/Analytics": [
        ("Data Analyst, Revenue Operations", "IC", "Data/Analytics"),
        ("Senior Data Scientist", "Senior", "Data/Analytics"),
        ("Analytics Engineer", "IC", "Data/Analytics"),
    ],
    "Partners/Alliances": [
        ("Partner Manager, Cloud Alliances", "IC", "Cloud Infra"),
        ("Director of Technology Partnerships", "Director+", "Data Platform"),
        ("Channel Partner Manager", "IC", "Data Platform"),
    ],
    "Design": [
        ("Product Designer, Platform", "IC", "UI/UX"),
        ("Senior UX Designer", "Senior", "UI/UX"),
        ("Design Systems Lead", "Principal/Staff", "UI/UX"),
    ],
}

FUNCTION_WEIGHTS = {
    "Engineering": 0.50,
    "AI/ML & Vector": 0.14,
    "Sales": 0.12,
    "Product Management": 0.06,
    "Solution Engineering": 0.04,
    "Marketing": 0.05,
    "Customer Success": 0.04,
    "Data/Analytics": 0.02,
    "Partners/Alliances": 0.02,
    "Design": 0.01,
}

LOCATIONS = [
    "San Francisco, CA", "New York, NY", "Austin, TX", "Seattle, WA",
    "Remote", "Boston, MA", "Denver, CO", "Chicago, IL",
    "Amsterdam, Netherlands", "London, UK", "Berlin, Germany",
]

RELEVANCY_BY_PRODUCT = {
    "ETL/Integration": 14.5,
    "Data Governance": 13.0,
    "Data Platform": 10.5,
    "AI/ML Pipeline": 8.0,
    "Vector / AI": 9.0,
    "Cloud Infra": 5.5,
    "Observability": 7.0,
    "Data/Analytics": 4.5,
    "UI/UX": 2.0,
    "Backend Systems": 6.5,
}


def _date_posted(days_ago: int) -> str:
    return (TODAY - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def _days_ago(max_days: int = 90) -> int:
    return random.randint(1, max_days)


def generate_jobs():
    rows = []
    row_id = 1
    for co in COMPANIES:
        n = co["roles"]
        # Adjust function weights based on company profile
        weights = dict(FUNCTION_WEIGHTS)
        if co["ai_pct"] > 0.25:
            weights["AI/ML & Vector"] = co["ai_pct"]
            weights["Engineering"] = co["eng_pct"]
        if co["gtm_pct"] > 0.20:
            weights["Sales"] += 0.03
            weights["Marketing"] += 0.02

        total_w = sum(weights.values())
        funcs = list(weights.keys())
        probs = [weights[f] / total_w for f in funcs]

        for _ in range(n):
            func = random.choices(funcs, weights=probs, k=1)[0]
            if func not in ROLE_TEMPLATES:
                func = "Engineering"
            role = random.choice(ROLE_TEMPLATES[func])
            title, seniority, product_focus = role
            # Slightly vary title
            title_variants = [title]
            if seniority == "IC":
                title_variants.append(title.replace("Software Engineer", "SWE", 1))
            title = random.choice(title_variants)

            days = _days_ago(90)
            location = random.choice(LOCATIONS)
            relevancy = RELEVANCY_BY_PRODUCT.get(product_focus, 5.0)
            # Add small jitter
            relevancy = round(min(17.5, max(0, relevancy + random.uniform(-1, 1))), 1)

            rows.append({
                "Company": co["name"],
                "Job Title": title,
                "Job Link": f"https://careers.{co['name'].lower()}.io/jobs/{row_id}",
                "Location": location,
                "Posting Date": _date_posted(days),
                "Days Since Posted": days,
                "Function": func,
                "Seniority": seniority,
                "Company_Group": co["group"],
                "Product_Focus": product_focus,
                "Product_Focus_Tokens": product_focus.lower().replace("/", " ").replace(" ", "_"),
                "Primary_Skill": product_focus.split("/")[0].strip(),
                "Extracted_Skills": product_focus,
                "Relevancy_to_Actian": relevancy,
                "Trend_Score": round(random.uniform(0.5, 3.5), 2),
                "First_Seen": _date_posted(days + random.randint(0, 7)),
                "Last_Seen": _date_posted(max(0, days - random.randint(0, 5))),
                "product_area": co["area"],
                "Description": "",
            })
            row_id += 1
    return rows


def generate_signals():
    sigs = []
    for co in COMPANIES:
        rows_for_co = [r for r in _jobs_cache if r["Company"] == co["name"]]
        total = len(rows_for_co)

        # Build implications
        impl = []
        if co["ai_pct"] > 0.20:
            impl.append(
                f"Heavy AI/ML hiring ({round(co['ai_pct']*100)}% of roles) signals a major platform "
                f"bet — likely building LLM-native features or autonomous agents to embed in data workflows"
            )
        if co["eng_pct"] > 0.50:
            impl.append(
                f"Engineering-dominant hiring ({round(co['eng_pct']*100)}% of roles) indicates "
                f"{co['name']} is still in build phase — product gaps remain before full GTM push"
            )
        if co["senior_pct"] > 0.38:
            impl.append(
                f"High seniority mix ({round(co['senior_pct']*100)}% Director+/Senior) suggests "
                f"new product line formation, not routine headcount expansion"
            )
        if co["gtm_pct"] > 0.20:
            impl.append(
                f"GTM hiring acceleration ({round(co['gtm_pct']*100)}% Sales/Marketing) — "
                f"{co['name']} is shifting from build to sell; expect aggressive pricing campaigns"
            )
        impl.append(
            f"Dominant focus on {co['dominant_product']} aligns with {co['description'].lower()} — "
            f"direct competitive pressure on Axon Analytics' core market"
        )

        recommended_actions = []
        if co["threat"] == "critical":
            recommended_actions = [
                f"Expedite competitive positioning against {co['name']}'s {co['dominant_product']} narrative",
                f"Brief all AEs on {co['name']} objection-handling — expect inbound comparisons",
                f"Accelerate roadmap items that neutralise {co['name']}'s key differentiator",
            ]
        elif co["threat"] == "high":
            recommended_actions = [
                f"Monitor {co['name']} launches closely over next 60 days",
                f"Ensure battlecard is current and covers latest {co['name']} positioning",
            ]
        else:
            recommended_actions = [
                f"Quarterly review of {co['name']} signal — no immediate action required",
            ]

        watch_for = [
            f"Series C/D funding announcement — would enable accelerated enterprise GTM",
            f"New integration partnerships with Snowflake or Databricks",
            f"Pricing change targeting Axon's SMB accounts",
        ]

        sigs.append({
            "company": co["name"],
            "company_group": co["group"],
            "posting_count": total,
            "signal_summary": (
                f"{co['name']} is in "
                + ("aggressive" if co["threat"] in ("critical", "high") else "steady")
                + f" hiring mode with {total} active roles — "
                + ("building directly into Axon Analytics core market" if co["threat"] == "critical" else co["description"].lower())
            ),
            "implications": impl[:5],
            "watch_for": watch_for,
            "recommended_actions": recommended_actions,
            "hiring_intensity": co["intensity"],
            "dominant_function": co["dominant_function"],
            "dominant_product_focus": co["dominant_product"],
            "threat_level": co["threat"],
            "roadmap": {
                "direction": f"Expanding {co['dominant_product']} capabilities toward enterprise-grade automation",
                "confidence": "High" if co["threat"] in ("critical", "high") else "Medium",
                "timeline": "Next 6–9 months",
                "watch_for": f"Beta launch of {co['dominant_product']} 2.0 platform",
            },
            "last_updated": TODAY.strftime("%Y-%m-%d"),
        })
    return sigs


def generate_verdicts():
    verdicts = []
    for co in COMPANIES:
        threat_cap = co["threat"].capitalize()
        verdicts.append({
            "company": co["name"],
            "product_area": co["area"],
            "signal_type": "hiring_surge" if co["threat"] == "critical" else "steady_growth",
            "impact_level": co["threat"],
            "what_is_happening": (
                f"{co['name']} has posted {co['roles']} roles in the past 30 days, "
                f"with {round(co['eng_pct']*100)}% engineering and {round(co['ai_pct']*100)}% AI/ML — "
                f"indicating {'a major platform build sprint' if co['eng_pct'] > 0.50 else 'sustained product investment'}"
            ),
            "why_it_matters": (
                f"{co['name']} is expanding its {co['dominant_product']} offering, "
                f"which overlaps directly with Axon Analytics' core pipeline and governance capabilities. "
                f"{'Immediate action recommended.' if co['threat'] == 'critical' else 'Monitor closely.'}"
            ),
            "primary_interpretation": (
                f"{'Aggressive' if co['threat'] in ('critical','high') else 'Measured'} "
                f"product expansion — {co['name']} is building toward enterprise parity"
            ),
            "alternative_interpretation": "Hiring may be replacement/attrition, not net new capacity",
            "hiring_event_correlation": f"{co['roles']} postings in 30-day window",
            "competitive_impact": co["threat"],
            "confidence": "High" if co["threat"] == "critical" else "Medium",
            "confidence_reasoning": "Sustained hiring across engineering + product + GTM functions",
            "recommended_action": (
                f"{'Escalate to exec team immediately' if co['threat'] == 'critical' else 'Brief AEs and update battlecard'}"
            ),
            "team_routing": (
                ["Product", "PMM", "Executives", "SDRs"] if co["threat"] == "critical"
                else ["Product", "PMM"] if co["threat"] == "high"
                else ["PMM"]
            ),
            "team_relevance": {
                "Product": (
                    f"{co['name']}'s {co['dominant_product']} hiring signals direct roadmap overlap — "
                    f"review feature parity and accelerate differentiators"
                ),
                "PMM": (
                    f"Update competitive positioning and messaging against {co['name']}'s "
                    f"{co['dominant_product']} narrative"
                ),
                "SDRs": (
                    f"{co['name']} is likely to appear in competitive deals — "
                    f"ensure AEs have current objection-handling playbooks"
                ),
                "Executives": (
                    f"{threat_cap} threat: {co['name']} is building toward feature parity — "
                    f"strategic response required"
                ),
                "Marketing": (
                    f"Air-cover opportunity: amplify Axon Analytics differentiators before "
                    f"{co['name']}'s GTM push lands"
                ),
            },
        })
    return verdicts


def generate_news():
    news_templates = [
        ("product_launch", "{company} Launches {product} for Enterprise Data Teams",
         "A new {product} capability designed to accelerate data pipeline automation and reduce time-to-insight for enterprise customers.",
         "high"),
        ("funding", "{company} Raises $120M Series C to Accelerate AI-Powered {product}",
         "{company} closes $120M Series C led by top-tier VCs, funding continued expansion of its {product} platform.",
         "high"),
        ("leadership", "{company} Appoints Former Databricks VP as Chief Product Officer",
         "Industry veteran joins {company} to lead product strategy and accelerate the next phase of platform growth.",
         "medium"),
        ("product_launch", "{company} Introduces {product} 2.0 with Native LLM Integration",
         "The new release adds LLM-native connectors and automated metadata enrichment, targeting enterprise data teams.",
         "high"),
        ("partnership", "{company} Partners with AWS to Co-Sell {product} on Marketplace",
         "{company} joins the AWS ISV Accelerate Program, enabling direct co-sell motions with AWS enterprise sales teams.",
         "high"),
        ("pricing", "{company} Announces Consumption-Based Pricing for {product}",
         "New pricing model replaces annual seat licenses, reducing friction for SMB adoption and putting pressure on incumbents.",
         "medium"),
        ("product_launch", "{company} Releases Open-Source {product} SDK",
         "Open-source SDK gives developers direct access to {company}'s core APIs, expanding the ecosystem and driving top-of-funnel adoption.",
         "medium"),
    ]

    items = []
    used = set()
    for co in COMPANIES[:6]:  # News for top 6 companies
        pool = [t for t in news_templates if t[0] not in used or True]
        chosen = random.sample(pool, k=min(2, len(pool)))
        for tmpl in chosen:
            news_type, title_t, summary_t, relevance = tmpl
            title = title_t.format(company=co["name"], product=co["dominant_product"])
            summary = summary_t.format(company=co["name"], product=co["dominant_product"])
            days_ago = random.randint(3, 55)
            items.append({
                "company": co["name"],
                "product_area": co["area"],
                "news_type": news_type,
                "title": title,
                "url": f"https://blog.{co['name'].lower()}.io/news/{news_type}-{random.randint(100,999)}",
                "published_date": _date_posted(days_ago),
                "source": "company_blog",
                "summary": summary,
                "actian_relevance": relevance,
                "tags": [co["area"].lower().replace(" ", "_"), news_type],
                "team_routing": ["Product", "PMM", "SDRs"] if relevance == "high" else ["PMM"],
                "event_date": None,
                "scraped_at": TODAY.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })

    items.sort(key=lambda x: x["published_date"], reverse=True)
    return items


def generate_competitive_signals():
    launch_templates = [
        ("product_launch", "{company} {product} — Advanced Pipeline Automation",
         "{company} ships a major upgrade to its {product} with autonomous pipeline repair and real-time anomaly detection.",
         "high"),
        ("product_launch", "{company} Unveils AI-Powered {product} Assistant",
         "New AI assistant embedded in {product} allows data engineers to describe pipelines in natural language.",
         "high"),
        ("product_launch", "{company} {product} — Enterprise Compliance Pack",
         "Adds SOC 2 Type II, GDPR, and HIPAA compliance templates directly into the {product} workflow.",
         "medium"),
        ("event", None, None, "medium"),  # placeholder for events
        ("event", None, None, "low"),
    ]

    event_templates = [
        ("{company} Data Summit 2026 — {company}'s annual user conference",
         "Annual user and developer conference showcasing {product} roadmap and customer success stories."),
        ("{company} + AWS Partner Webinar — Winning with {product}",
         "Co-hosted webinar covering joint go-to-market strategy and customer case studies."),
        ("{company} at Data + AI Summit 2026",
         "{company} presenting keynote and running breakout sessions at the industry's premier data conference."),
        ("{company} Live Demo Day — {product} 2.0 Showcase",
         "Public product demonstration with hands-on labs for enterprise prospects."),
    ]

    items = []
    item_id = 1

    for co in COMPANIES:
        # 1-2 product launches per company
        n_launches = 2 if co["threat"] in ("critical", "high") else 1
        for i in range(n_launches):
            tmpl = random.choice([t for t in launch_templates if t[0] == "product_launch"])
            title = tmpl[1].format(company=co["name"], product=co["dominant_product"])
            summary = tmpl[2].format(company=co["name"], product=co["dominant_product"])
            days_ago = random.randint(5, 50)
            items.append({
                "company": co["name"],
                "product_area": co["area"],
                "type": "product_launch",
                "title": title,
                "url": f"https://{co['name'].lower()}.io/blog/launch-{item_id}",
                "published_date": _date_posted(days_ago),
                "summary": summary,
                "actian_relevance": tmpl[3],
                "tags": [co["area"].lower().replace(" ", "_"), "product_launch"],
                "source_type": "blog",
                "event_date": None,
                "team_routing": ["Product", "PMM", "SDRs"] if tmpl[3] == "high" else ["PMM"],
                "scraped_at": TODAY.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
            item_id += 1

        # 1-2 events per company
        n_events = 2 if co["threat"] in ("critical", "high") else 1
        for i in range(n_events):
            ev_tmpl = random.choice(event_templates)
            ev_title = ev_tmpl[0].format(company=co["name"], product=co["dominant_product"])
            ev_summary = ev_tmpl[1].format(company=co["name"], product=co["dominant_product"])
            # Upcoming events
            days_ahead = random.randint(5, 120)
            event_date = (TODAY + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
            items.append({
                "company": co["name"],
                "product_area": co["area"],
                "type": "event",
                "title": ev_title,
                "url": f"https://events.{co['name'].lower()}.io/event-{item_id}",
                "published_date": _date_posted(random.randint(2, 14)),
                "summary": ev_summary,
                "actian_relevance": "medium",
                "tags": ["event", co["area"].lower().replace(" ", "_")],
                "source_type": "events_page",
                "event_date": event_date,
                "team_routing": ["Marketing", "SDRs"],
                "scraped_at": TODAY.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
            item_id += 1

    return items


# ── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating demo data...")

    # Generate jobs first (needed by signals generator)
    jobs = generate_jobs()
    _jobs_cache = jobs

    # Write CSV
    csv_path = OUT_DIR / "jobs_demo.csv"
    fieldnames = [
        "Company", "Job Title", "Job Link", "Location", "Posting Date",
        "Days Since Posted", "Function", "Seniority", "Company_Group",
        "Product_Focus", "Product_Focus_Tokens", "Primary_Skill",
        "Extracted_Skills", "Relevancy_to_Actian", "Trend_Score",
        "First_Seen", "Last_Seen", "product_area", "Description",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(jobs)
    print(f"  ✓ {csv_path.name}: {len(jobs)} job rows")

    # Signals
    sigs = generate_signals()
    sig_path = OUT_DIR / "signals_demo.json"
    with open(sig_path, "w") as f:
        json.dump(sigs, f, indent=2)
    print(f"  ✓ {sig_path.name}: {len(sigs)} companies")

    # Verdicts
    verdicts = generate_verdicts()
    verdict_path = OUT_DIR / "verdicts_demo.json"
    with open(verdict_path, "w") as f:
        json.dump(verdicts, f, indent=2)
    print(f"  ✓ {verdict_path.name}: {len(verdicts)} verdicts")

    # News
    news = generate_news()
    news_path = OUT_DIR / "news_demo.json"
    with open(news_path, "w") as f:
        json.dump(news, f, indent=2)
    print(f"  ✓ {news_path.name}: {len(news)} items")

    # Competitive signals
    comp_sigs = generate_competitive_signals()
    comp_path = OUT_DIR / "competitive_signals_demo.json"
    with open(comp_path, "w") as f:
        json.dump(comp_sigs, f, indent=2)
    print(f"  ✓ {comp_path.name}: {len(comp_sigs)} items")

    print("\nDone! Run: python scripts/build_demo_v3.py")
else:
    # When imported, provide the cache for other functions
    _jobs_cache = []
