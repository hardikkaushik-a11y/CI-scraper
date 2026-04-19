#!/usr/bin/env python3
"""
weekly_digest.py — Phase 5: Weekly Executive Intelligence Digest
─────────────────────────────────────────────────────────────────
• Runs weekly (Saturday 06:00 UTC via separate scrape.yml schedule job)
• Reads last-7-days slice of intelligence_verdicts.json + signals.json + competitive_signals.json
• Calls Claude Opus to generate markdown summary
• Deliverables:
  - Top threats (CRITICAL + HIGH verdicts from past week)
  - New launches (product + open_source from past week)
  - Upcoming events (next 30 days)
  - Hiring surges (sharp increases in postings)
  - Recommended actions for Actian
• Delivers to #competitive-weekly Slack channel
"""

import json
import os
from datetime import date, timedelta
from pathlib import Path

import httpx

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
OPUS_MODEL = "claude-opus-4-1"

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"

VERDICTS_PATH = DATA_DIR / "intelligence_verdicts.json"
SIGNALS_PATH = DATA_DIR / "signals.json"
COMPETITIVE_SIGNALS_PATH = DATA_DIR / "competitive_signals.json"

# ══════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════

def load_json(path):
    """Load JSON file, return empty list/dict on error."""
    if not path.exists():
        return [] if str(path).endswith(".json") else {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return [] if str(path).endswith(".json") else {}


def get_week_ago():
    """Return ISO date string from 7 days ago."""
    return (date.today() - timedelta(days=7)).isoformat()


def filter_by_date(items, date_key="last_updated"):
    """Filter items from past 7 days."""
    cutoff = get_week_ago()
    return [item for item in items if item.get(date_key, "") >= cutoff]


def filter_events_upcoming(items, days_ahead=30):
    """Filter events with event_date in next N days."""
    today = date.today().isoformat()
    cutoff = (date.today() + timedelta(days=days_ahead)).isoformat()
    return [
        item for item in items
        if item.get("event_date") and today <= item.get("event_date") <= cutoff
    ]


# ══════════════════════════════════════════════════════════════════════════
# DIGEST GENERATION (Claude Opus)
# ══════════════════════════════════════════════════════════════════════════

DIGEST_SYSTEM = """You are Actian's weekly competitive intelligence briefing author.
Generate a concise, leadership-ready summary for Saturday morning.

Structure:
1. **Top Threats This Week** — CRITICAL + HIGH verdicts only, with specifics
2. **New Launches** — What competitors shipped
3. **Upcoming Events** — Conferences, webinars, customer summits in next 30 days
4. **Hiring Signals** — Rapid hiring in key functions (AI/ML, GTM, sales eng)
5. **Recommended Actions** — Concrete next steps for Actian teams

Be specific. No generic phrases. Use actual data (names, role counts, dates, deal risks).
Write for VP / CMO / CPO level — concise, actionable."""


def generate_digest(verdicts, signals, comp_signals):
    """
    Call Claude Opus to generate markdown digest from raw data.
    """
    # Filter to past 7 days
    recent_verdicts = filter_by_date(verdicts, "last_updated")
    recent_signals = filter_by_date(signals, "last_updated")
    recent_launches = [s for s in filter_by_date(comp_signals, "scraped_at")
                       if s.get("type") in ("product_launch", "open_source_release")]
    upcoming_events = filter_events_upcoming(comp_signals)

    # Build human-readable summary for Claude
    summary = f"""
## DATA FOR THIS WEEK

### Recent Verdicts (past 7 days)
{json.dumps(recent_verdicts[:15], indent=2)}

### New Launches (past 7 days)
{json.dumps(recent_launches[:15], indent=2)}

### Upcoming Events (next 30 days)
{json.dumps(upcoming_events[:15], indent=2)}

### Hiring Signals (past 7 days)
Top companies by new postings:
{json.dumps(recent_signals[:15], indent=2)}
"""

    # Call Claude Opus
    if not ANTHROPIC_API_KEY:
        print("[WARN] ANTHROPIC_API_KEY not set — returning placeholder digest")
        return "## Weekly Digest\n\nNo API key configured. Run in CI with ANTHROPIC_API_KEY set."

    client = httpx.Client(timeout=60)
    try:
        r = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": OPUS_MODEL,
                "max_tokens": 2000,
                "system": DIGEST_SYSTEM,
                "messages": [
                    {
                        "role": "user",
                        "content": f"Generate this week's competitive intelligence digest:\n{summary}",
                    }
                ],
            },
        )
        r.raise_for_status()
        result = r.json()

        if result.get("content"):
            return result["content"][0]["text"]
        else:
            return "Error: No response from Claude"
    except Exception as e:
        print(f"[ERROR] Claude Opus call failed: {e}")
        return f"Error generating digest: {e}"
    finally:
        client.close()


# ══════════════════════════════════════════════════════════════════════════
# SLACK DELIVERY
# ══════════════════════════════════════════════════════════════════════════

def send_to_slack(markdown_digest):
    """
    Send markdown digest to #competitive-weekly via webhook.
    """
    if not SLACK_WEBHOOK_URL:
        print("[WARN] SLACK_WEBHOOK_URL not set — digest not sent")
        return False

    payload = {
        "text": "📊 Weekly Competitive Intelligence Digest",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📊 Weekly Competitive Intelligence",
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": markdown_digest,
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Generated {date.today().isoformat()} | [Dashboard](https://actian-ci.vercel.app/dashboard_v3.html)",
                    }
                ],
            },
        ],
    }

    client = httpx.Client(timeout=30)
    try:
        r = client.post(SLACK_WEBHOOK_URL, json=payload)
        r.raise_for_status()
        print(f"✓ Digest sent to Slack")
        return True
    except Exception as e:
        print(f"[ERROR] Slack delivery failed: {e}")
        return False
    finally:
        client.close()


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 70)
    print("WEEKLY DIGEST GENERATOR — Phase 5 (Executive Briefing)")
    print("=" * 70)

    # Load data
    print("\nLoading data...")
    verdicts = load_json(VERDICTS_PATH)
    signals = load_json(SIGNALS_PATH)
    comp_signals = load_json(COMPETITIVE_SIGNALS_PATH)

    print(f"  {len(verdicts)} verdicts")
    print(f"  {len(signals)} signals")
    print(f"  {len(comp_signals)} competitive signals")

    # Generate digest
    print("\nGenerating digest with Claude Opus...")
    digest = generate_digest(verdicts, signals, comp_signals)

    # Save locally
    digest_file = DATA_DIR / "weekly_digest.md"
    with open(digest_file, "w") as f:
        f.write(digest)
    print(f"✓ Digest saved to {digest_file}")

    # Send to Slack
    if SLACK_WEBHOOK_URL:
        print("\nSending to #competitive-weekly...")
        send_to_slack(digest)
    else:
        print("[INFO] SLACK_WEBHOOK_URL not set — skipping Slack delivery")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
