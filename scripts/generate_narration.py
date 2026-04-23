"""
Generate narration.mp3 for the Actian CI video using OpenAI TTS.

Usage:
  export OPENAI_API_KEY=sk-...
  python3 scripts/generate_narration.py

Output: narration.mp3  (placed next to the HTML file, ~100 seconds)

The animation plays at 0.6x speed (default RATE), so 60 animation-seconds
= 100 real seconds. This script generates audio timed to those 100 real
seconds so the narration lands on the right scene.

Scene timing (real seconds at 0.6x):
  0–7     Hero
  7–20    Verdicts dashboard
  20–40   Lens switching
  40–50   Hiring Intelligence
  50–60   Launches & Events
  60–67   News
  67–80   Brief drawer + Sources
  80–92   Ask CI
  92–100  Close
"""

import os
import sys
import pathlib

try:
    from openai import OpenAI
except ImportError:
    sys.exit("Run: pip install openai")

SCRIPT = """
What if your whole team knew exactly what competitors were doing — before it mattered?

Actian CI is a live competitive intelligence platform. It tracks eleven competitors, around the clock — and turns raw data into verdict-level intelligence. Not reports. Decisions.

Five lenses keep every angle covered. Verdicts: the AI-generated brief on each competitor. Launches: product releases and pricing moves. Hiring: where competitors are placing their bets. Events: conference moments before they happen. And News: signal, not noise.

Hiring signals are early indicators. When Atlan posts forty engineering roles, that's not routine growth — it's product acceleration. CI surfaces it before the market does.

Product launches and events get routed automatically. The right intelligence, to the right team, in Slack — before the meeting starts.

News is filtered, not firehosed. Signal. Not noise.

Every verdict is fully traceable. Open any competitor brief and see the exact sources: press releases, job postings, conference announcements — all classified, dated, and linked. Public data governance, built in from day one.

Ask CI anything. Which competitors launched something this week? Where's Databricks investing in hiring? The platform answers from structured intelligence — grounded, traceable, and fast.

Competitive intelligence that moves at the speed of the market. Built on public data. Designed for teams.

Know before it matters.
""".strip()

def main():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        sys.exit("Set OPENAI_API_KEY environment variable first.")

    client = OpenAI(api_key=api_key)

    out_path = pathlib.Path(__file__).parent.parent / "narration.mp3"
    print(f"Generating narration via OpenAI TTS...")
    print(f"Voice: nova  Model: tts-1-hd  Output: {out_path}")

    response = client.audio.speech.create(
        model="tts-1-hd",
        voice="nova",          # clear, professional female voice
        input=SCRIPT,
        speed=0.92,            # slightly slower for gravitas; target ~100s
        response_format="mp3",
    )

    response.stream_to_file(str(out_path))
    print(f"Done. File: {out_path} ({out_path.stat().st_size // 1024} KB)")
    print()
    print("Next step: open the HTML in a browser — click '🔊 Play Narration' to sync audio.")


if __name__ == "__main__":
    main()
