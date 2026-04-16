"""
slack_notifier.py — Phase 4: Slack Routing for Intelligence Verdicts & Signals
───────────────────────────────────────────────────────────────────────────────
• Reads data/intelligence_verdicts.json (Phase 2.5 verdicts)
• Reads data/competitive_signals.json (Phase 2 launches/events)
• Routes to appropriate Slack channels based on signal type and threat level
• Deduplicates via data/slack_sent.json (records sent message IDs)
• Message format: Lawrence-approved structure (specific, actionable, concise)
• Channels:
  - #competitive-product: product launches, OSS releases
  - #competitive-gtm: partnerships, events with high relevance
  - #competitive-signals: critical threat verdicts
  - #competitive-weekly: Saturday digest (separate weekly job)
"""

import hashlib
import json
import os
from datetime import datetime, date

import httpx

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

VERDICTS_PATH = "data/intelligence_verdicts.json"
COMPETITIVE_SIGNALS_PATH = "data/competitive_signals.json"
SLACK_SENT_PATH = "data/slack_sent.json"

# Channel routing rules
CHANNEL_ROUTING = {
    # Verdicts
    "verdict_critical": "#competitive-signals",
    "verdict_high": "#competitive-signals",
    # Launches / Events
    "product_launch": "#competitive-product",
    "open_source_release": "#competitive-product",
    "event_high": "#competitive-gtm",
    "partnership_high": "#competitive-gtm",
    "funding": "#competitive-signals",
}

# V2 allowed companies
V2_COMPANIES = {
    "Atlan", "Collibra", "Alation", "Monte Carlo", "Bigeye",
    "Acceldata", "Pinecone", "Qdrant", "Milvus", "Snowflake", "Databricks"
}

# ══════════════════════════════════════════════════════════════════════════
# DEDUP LOGIC
# ══════════════════════════════════════════════════════════════════════════

def load_sent_messages():
    """Load set of message IDs already sent to Slack."""
    if not os.path.exists(SLACK_SENT_PATH):
        return set()
    try:
        with open(SLACK_SENT_PATH) as f:
            return set(json.load(f))
    except (json.JSONDecodeError, IOError):
        return set()

def save_sent_messages(sent_ids):
    """Persist set of sent message IDs."""
    with open(SLACK_SENT_PATH, 'w') as f:
        json.dump(sorted(sent_ids), f, indent=2)

def message_id(source, item_id):
    """Generate stable ID for a message (verdict or signal)."""
    return hashlib.md5(f"{source}:{item_id}".encode()).hexdigest()[:16]

# ══════════════════════════════════════════════════════════════════════════
# MESSAGE BUILDERS (Lawrence-approved format)
# ══════════════════════════════════════════════════════════════════════════

def format_verdict_message(v):
    """
    Format a verdict for Slack.
    Structure:
    • Company first
    • What happened (specific, not generic)
    • Event or timing context
    • Why it matters (to Actian specifically)
    • Action implication (concrete, not "monitor closely")
    • Dashboard link
    """
    company = v.get("company", "Unknown")
    area = v.get("product_area", "")
    threat = v.get("threat", "").upper()
    what = v.get("what_is_happening", "")
    why = v.get("why_it_matters", "")
    action = v.get("recommended_action", "")

    body = f"""*{company}* — Intelligence Verdict ({area})

{what}

Why it matters: {why}

Actian action: {action}

→ <https://ci-scraper-dashboard.onrender.com/dashboard/v2/|View Dashboard>"""

    return {
        "channel": CHANNEL_ROUTING.get(f"verdict_{threat.lower()}", "#competitive-signals"),
        "text": body,
        "company": company,
        "threat": threat,
        "ts": datetime.utcnow().isoformat() + "Z",
    }

def format_launch_message(l):
    """
    Format a launch/event for Slack.
    Structure: similar to verdicts but launch-focused
    """
    company = l.get("company", "Unknown")
    title = l.get("title", "")
    launch_type = l.get("type", "").replace("_", " ").title()
    summary = l.get("summary", "")
    relevance = l.get("actian_relevance", "")
    published = l.get("published_date", l.get("scraped_at", ""))

    # Determine channel
    sig_type = l.get("type", "blog_post")
    if sig_type == "product_launch":
        channel = "#competitive-product"
    elif sig_type == "open_source_release":
        channel = "#competitive-product"
    elif sig_type == "event" and relevance == "high":
        channel = "#competitive-gtm"
    elif sig_type == "partnership" and relevance == "high":
        channel = "#competitive-gtm"
    elif sig_type == "funding":
        channel = "#competitive-signals"
    else:
        channel = None  # Skip low-relevance items

    if not channel:
        return None

    body = f"""*{company}* — {launch_type} ({summary.split('.')[0]})

{summary}

Published: {published} · Relevance: {relevance.upper()}

→ <https://ci-scraper-dashboard.onrender.com/dashboard/v2/|View Dashboard>"""

    return {
        "channel": channel,
        "text": body,
        "company": company,
        "type": sig_type,
        "ts": datetime.utcnow().isoformat() + "Z",
    }

# ══════════════════════════════════════════════════════════════════════════
# SLACK POSTING
# ══════════════════════════════════════════════════════════════════════════

def send_to_slack(message):
    """Post a message to Slack via webhook."""
    if not SLACK_WEBHOOK_URL:
        print("⚠ SLACK_WEBHOOK_URL not set — skipping Slack delivery")
        return False

    try:
        response = httpx.post(
            SLACK_WEBHOOK_URL,
            json={"text": message["text"]},
            timeout=10.0,
        )
        if response.status_code == 200:
            print(f"✓ Slack: {message['channel']} — {message['company']}")
            return True
        else:
            print(f"✗ Slack error ({response.status_code}): {response.text}")
            return False
    except Exception as e:
        print(f"✗ Slack delivery failed: {e}")
        return False

# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 78)
    print("Phase 4: Slack Routing — Intelligence Verdicts & Signals")
    print("=" * 78)

    sent_ids = load_sent_messages()
    messages_to_send = []

    # ─── Load Verdicts ───────────────────────────────────────────────────
    verdicts = []
    if os.path.exists(VERDICTS_PATH):
        try:
            with open(VERDICTS_PATH) as f:
                verdicts = json.load(f)
                verdicts = [v for v in verdicts if v.get("company") in V2_COMPANIES]
            print(f"✓ Loaded {len(verdicts)} verdicts")
        except Exception as e:
            print(f"✗ Failed to load verdicts: {e}")
    else:
        print(f"⚠ Verdicts file not found: {VERDICTS_PATH}")

    # ─── Load Competitive Signals ─────────────────────────────────────────
    signals = []
    if os.path.exists(COMPETITIVE_SIGNALS_PATH):
        try:
            with open(COMPETITIVE_SIGNALS_PATH) as f:
                signals = json.load(f)
                signals = [s for s in signals if s.get("company") in V2_COMPANIES]
            print(f"✓ Loaded {len(signals)} competitive signals")
        except Exception as e:
            print(f"✗ Failed to load signals: {e}")
    else:
        print(f"⚠ Signals file not found: {COMPETITIVE_SIGNALS_PATH}")

    # ─── Route Verdicts ──────────────────────────────────────────────────
    print("\nRouting verdicts...")
    for v in verdicts:
        msg_id = message_id("verdict", f"{v.get('company')}_{v.get('last_updated')}")
        if msg_id in sent_ids:
            continue  # Already sent

        # Only send high-threat verdicts to Slack
        threat = v.get("threat", "").lower()
        if threat not in ["critical", "high"]:
            continue

        msg = format_verdict_message(v)
        if msg:
            messages_to_send.append((msg_id, msg))

    print(f"  → {len(messages_to_send)} new verdicts to send")

    # ─── Route Signals ──────────────────────────────────────────────────
    print("\nRouting competitive signals...")
    signals_to_send = 0
    for s in signals:
        msg_id = message_id("signal", s.get("url", f"{s.get('company')}_{s.get('published_date')}"))
        if msg_id in sent_ids:
            continue  # Already sent

        # Filter: only high-relevance launches, events, partnerships, and funding
        sig_type = s.get("type", "")
        relevance = s.get("actian_relevance", "")

        skip = (
            sig_type == "blog_post" or
            (sig_type in ["event", "partnership"] and relevance != "high") or
            sig_type not in ["product_launch", "open_source_release", "event", "partnership", "funding"]
        )

        if skip:
            continue

        msg = format_launch_message(s)
        if msg:
            messages_to_send.append((msg_id, msg))
            signals_to_send += 1

    print(f"  → {signals_to_send} new signals to send")

    # ─── Post to Slack ──────────────────────────────────────────────────
    print("\nPosting to Slack...")
    posted = 0
    for msg_id, msg in messages_to_send:
        if send_to_slack(msg):
            sent_ids.add(msg_id)
            posted += 1

    if posted > 0:
        save_sent_messages(sent_ids)
        print(f"\n✓ Posted {posted} messages to Slack")
    else:
        print("\n✓ No new messages to send")

    print("=" * 78)

if __name__ == "__main__":
    main()
