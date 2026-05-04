"""
roadmaps.py — Strategic roadmap intelligence
─────────────────────────────────────────────
Two paths into one output file:

1. PUBLISHED — vendors that publish a public roadmap. Scrape the page,
   feed to DeepSeek to extract structured pillars + timeline.

2. INFERRED — for the other vendors, synthesize a roadmap from existing
   signals (verdict + hiring brief + recent launches + recent news + themes).

Both paths produce the same schema, distinguished by `source: "published" | "inferred"`.

Output: data/roadmaps.json — consumed by build_dashboard_v3 and surfaced in
each company's BriefDrawer as a "Strategic direction" section.
"""

import json
import os
import re
from datetime import date
from pathlib import Path

import httpx

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = "deepseek-chat"

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"

VERDICTS_PATH       = DATA_DIR / "intelligence_verdicts.json"
SIGNALS_PATH        = DATA_DIR / "signals.json"
COMP_SIGNALS_PATH   = DATA_DIR / "competitive_signals.json"
NEWS_PATH           = DATA_DIR / "news.json"
ROADMAPS_PATH       = DATA_DIR / "roadmaps.json"

# Vendors with public roadmaps. Only add an entry here once a URL is verified.
PUBLISHED_ROADMAP_URLS = {
    "Milvus": "https://milvus.io/docs/roadmap.md",
    # Add Qdrant / Pinecone here once URLs are validated
}

# All V2 vendors — those without a published URL get the inferred path
V2_COMPANIES = [
    "Atlan", "Collibra", "Alation",
    "Monte Carlo", "Bigeye", "Acceldata",
    "Pinecone", "Qdrant", "Milvus",
    "Snowflake", "Databricks",
]


# ══════════════════════════════════════════════════════════════════════════
# DEEPSEEK
# ══════════════════════════════════════════════════════════════════════════

def _call_deepseek(system: str, user: str, max_tokens: int = 1500) -> str:
    if not DEEPSEEK_API_KEY:
        return ""
    try:
        r = httpx.post(
            "https://api.deepseek.com/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEEPSEEK_MODEL,
                "max_tokens": max_tokens,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            },
            timeout=120,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"  [WARN] DeepSeek call failed: {e}")
        return ""


def _parse_json(raw: str) -> dict | None:
    if not raw:
        return None
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        text = m.group(0)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # tolerant repair
        repaired = re.sub(r",(\s*[}\]])", r"\1", text)
        repaired = repaired.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
        try:
            return json.loads(repaired)
        except Exception:
            return None


# ══════════════════════════════════════════════════════════════════════════
# PUBLISHED ROADMAP — scrape + extract
# ══════════════════════════════════════════════════════════════════════════

PUBLISHED_SYSTEM = """\
You are Actian's competitive intelligence analyst. You receive the raw text of
a competitor's public product roadmap page. Extract a structured representation.

Output ONLY valid JSON, no markdown fences, with this exact shape:
{
  "title": "<roadmap name or page title>",
  "summary": "<2-3 sentences — the strategic direction this roadmap commits to>",
  "pillars": [
    {
      "name": "<short pillar name (3-7 words)>",
      "evidence": "<concrete features/capabilities listed under this pillar>",
      "confidence": "high"
    }
  ],
  "timeline_estimate": "<Coming soon | Next quarter | 6-12 months | based on the page>",
  "what_to_watch_for": ["<observable next steps that would confirm or deny pillars>"],
  "actian_competitive_impact": "<1 sentence — what this means for Actian specifically>"
}

Rules:
- 3-5 pillars max. Don't list every feature.
- Confidence is "high" for published roadmaps (vendor's own commitment).
- Use straight ASCII quotes. No trailing commas.
"""


def scrape_published(company: str, url: str) -> dict | None:
    """Fetch page, extract roadmap via DeepSeek."""
    print(f"  [PUBLISHED] {company}  ← {url}")
    try:
        r = httpx.get(url, timeout=30, follow_redirects=True,
                      headers={"User-Agent": "Mozilla/5.0 (Actian-CI-Bot)"})
        r.raise_for_status()
    except Exception as e:
        print(f"    [WARN] fetch failed: {e}")
        return None

    text = r.text
    # Strip HTML tags, keep text content
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # Cap to keep prompt size sane
    text = text[:8000]

    user_prompt = f"Vendor: {company}\nSource URL: {url}\n\nPage content:\n{text}"
    raw = _call_deepseek(PUBLISHED_SYSTEM, user_prompt, max_tokens=1500)
    parsed = _parse_json(raw)
    if not parsed:
        print(f"    [WARN] {company} — could not parse roadmap")
        return None

    # Stamp metadata
    parsed["company"] = company
    parsed["source"] = "published"
    parsed["url"] = url
    parsed["overall_confidence"] = "high"
    parsed["last_updated"] = date.today().isoformat()
    return parsed


# ══════════════════════════════════════════════════════════════════════════
# INFERRED ROADMAP — synthesize from existing signals
# ══════════════════════════════════════════════════════════════════════════

INFERRED_SYSTEM = """\
You are Actian's competitive intelligence analyst. The vendor below does NOT
publish a public roadmap. Infer their likely 6-12 month strategic direction
from the signals provided. Use only what the data supports — do not fabricate.

Output ONLY valid JSON, no markdown fences, with this exact shape:
{
  "title": "Inferred direction",
  "summary": "<2-3 sentences — most probable strategic direction based on evidence>",
  "pillars": [
    {
      "name": "<short pillar name>",
      "evidence": "<which specific hires / launches / news support this pillar>",
      "confidence": "high | medium | low"
    }
  ],
  "timeline_estimate": "<Next quarter | 6-12 months | 12+ months>",
  "what_to_watch_for": ["<observable next steps that would confirm or deny>"],
  "actian_competitive_impact": "<1 sentence — what this means for Actian specifically>"
}

Rules:
- 3-5 pillars max. Each must be grounded in concrete signals from the input.
- Per-pillar confidence: high = multiple corroborating signals; medium = one strong;
  low = directional hint only.
- Overall confidence = average of per-pillar confidences.
- If evidence is sparse, say so honestly — fewer high-confidence pillars beats
  five vague ones.
- Use straight ASCII quotes. No trailing commas.
"""


def _build_inferred_user_prompt(company: str, verdict: dict, hiring_signal: dict,
                                 comp_signals: list[dict], news_items: list[dict]) -> str:
    parts = [f"Vendor: {company}"]
    parts.append(f"Primary product area: {verdict.get('product_area', '')}")
    parts.append(f"Multi-area exposure: {verdict.get('product_areas', [])}")
    parts.append(f"Active themes: {verdict.get('themes', [])}")

    parts.append("\n## VERDICT NARRATIVE (analyst-grade synthesis)")
    parts.append(f"What's happening: {verdict.get('what_is_happening', '')}")
    parts.append(f"Primary interpretation: {verdict.get('primary_interpretation', '')}")
    parts.append(f"Why it matters to Actian: {verdict.get('why_it_matters', '')}")

    parts.append("\n## HIRING SIGNAL")
    parts.append(f"Posting count: {hiring_signal.get('posting_count', 0)}")
    parts.append(f"Dominant function: {hiring_signal.get('dominant_function', '')}")
    parts.append(f"Dominant product focus: {hiring_signal.get('dominant_product_focus', '')}")
    parts.append(f"Signal summary: {hiring_signal.get('signal_summary', '')}")
    impls = hiring_signal.get("implications", [])[:5]
    if impls:
        parts.append("Implications:")
        for i in impls:
            parts.append(f"  - {i}")
    watch = hiring_signal.get("watch_for", [])[:3]
    if watch:
        parts.append("Watch for:")
        for w in watch:
            parts.append(f"  - {w}")

    if comp_signals:
        parts.append("\n## RECENT LAUNCHES & EVENTS (last 90 days)")
        for s in comp_signals[:8]:
            parts.append(f"  [{s.get('type')}] {s.get('title', '')[:80]}")
            if s.get("summary"):
                parts.append(f"      → {s['summary'][:160]}")

    if news_items:
        parts.append("\n## RECENT NEWS")
        for n in news_items[:8]:
            parts.append(f"  [{n.get('news_type')}] {n.get('title', '')[:80]}")
            if n.get("summary"):
                parts.append(f"      → {n['summary'][:160]}")

    return "\n".join(parts)


def infer_roadmap(company: str, verdict: dict, hiring_signal: dict,
                  comp_signals: list[dict], news_items: list[dict]) -> dict | None:
    if not verdict and not hiring_signal:
        return None
    print(f"  [INFERRED]  {company}")
    user_prompt = _build_inferred_user_prompt(company, verdict, hiring_signal, comp_signals, news_items)
    raw = _call_deepseek(INFERRED_SYSTEM, user_prompt, max_tokens=1500)
    parsed = _parse_json(raw)
    if not parsed:
        print(f"    [WARN] {company} — inference parse failed")
        return None

    # Compute overall confidence from per-pillar
    pillars = parsed.get("pillars", [])
    if pillars:
        weights = {"high": 3, "medium": 2, "low": 1}
        avg = sum(weights.get((p.get("confidence") or "low").lower(), 1) for p in pillars) / len(pillars)
        if avg >= 2.5:
            overall = "high"
        elif avg >= 1.7:
            overall = "medium"
        else:
            overall = "low"
    else:
        overall = "low"

    parsed["company"] = company
    parsed["source"] = "inferred"
    parsed["url"] = None
    parsed["overall_confidence"] = overall
    parsed["last_updated"] = date.today().isoformat()
    parsed.setdefault("title", "Inferred direction")
    return parsed


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def _load_json(path: Path) -> list:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def main() -> None:
    if not DEEPSEEK_API_KEY:
        print("[roadmaps] DEEPSEEK_API_KEY not set — skipping")
        return

    print(f"[roadmaps] Building strategic roadmaps...")

    verdicts = _load_json(VERDICTS_PATH)
    signals  = _load_json(SIGNALS_PATH)
    comps    = _load_json(COMP_SIGNALS_PATH)
    news     = _load_json(NEWS_PATH)

    verdict_by_co = {v.get("company"): v for v in verdicts}
    signal_by_co  = {s.get("company"): s for s in signals}

    out = []

    # 1. Published roadmaps
    for company, url in PUBLISHED_ROADMAP_URLS.items():
        rm = scrape_published(company, url)
        if rm:
            out.append(rm)

    # 2. Inferred roadmaps for everyone else
    for company in V2_COMPANIES:
        if company in PUBLISHED_ROADMAP_URLS:
            continue  # already covered by published
        verdict = verdict_by_co.get(company, {})
        hiring  = signal_by_co.get(company, {})
        c_comps = [s for s in comps if s.get("company") == company]
        c_news  = [n for n in news if n.get("company") == company]
        rm = infer_roadmap(company, verdict, hiring, c_comps, c_news)
        if rm:
            out.append(rm)

    ROADMAPS_PATH.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    pub = sum(1 for r in out if r.get("source") == "published")
    inf = sum(1 for r in out if r.get("source") == "inferred")
    print(f"[roadmaps] Done — {pub} published + {inf} inferred → {ROADMAPS_PATH.name}")


if __name__ == "__main__":
    main()
