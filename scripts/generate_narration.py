"""
generate_narration.py  —  Actian CI promo narration generator
Video: RATE=0.6667 → 60 virtual s = 90 real s

Usage:  python3 scripts/generate_narration.py [bill|adam|irene]
        (adam/irene require ElevenLabs paid plan for API access)
"""
import os, sys, subprocess, tempfile, json, time
import requests

API_KEY   = "sk_0bdc917733851b0a48e1e06e97d70087324170fba44f96b0"
MODEL_ID  = "eleven_multilingual_v2"
OUT_FILE  = os.path.join(os.path.dirname(__file__), "..", "narration.mp3")
TOTAL_DUR = 90.0

VOICES = {
    "bill":  "pqHfZKP75CvOlQylNhV4",  # Bill  — advertisement, wise, crisp (free tier ✓)
    "adam":  "s3TPKV1kjDlVtZbl4Ksh",  # Adam  — engaging, friendly (paid tier)
    "irene": "w9xM4Spfmuw28ZXAirWK",  # Irene — your pick (paid tier)
}
voice_key = (sys.argv[1].lower() if len(sys.argv) > 1 else "bill")
VOICE_ID  = VOICES.get(voice_key, VOICES["bill"])

def fix(t): return t.replace("Actian", "Ack-tee-an")

# ── Scene windows (real seconds at RATE=0.6667) ───────────────────────────────
# Hero      0–6     Dashboard  6–18    Lens    18–36
# Hiring   36–45    Launches  45–54    News   54–60
# Sources  60–72    AskCI     72–82.5  Close  82.5–90
#
# Narration starts 0.5s after scene and MUST finish before next scene starts.
# Clip durations measured from Bill's first run — trimmed to fit windows.

SEGMENTS = [
    # window 0.4 → 5.8s  (5.4s)
    { "name": "hero",      "start_s": 0.4,
      "text": fix("What if your team always knew what competitors were doing — "
                  "before it mattered?") },

    # window 6.5 → 17.5s  (11s)
    { "name": "dashboard", "start_s": 6.5,
      "text": fix("Eleven competitors. Two critical threats, five high, "
                  "over a hundred signals daily — actionable verdicts.") },

    # window 18.5 → 35.5s  (17s)  — longest scene, most room
    { "name": "lens",      "start_s": 18.5,
      "text": fix("Every verdict adapts to who needs it. "
                  "Product sees the roadmap impact. "
                  "PMM gets the battlecard gap. "
                  "SDRs see which accounts are at risk today. "
                  "Same intelligence — one click changes what matters.") },

    # window 36.5 → 44.5s  (8s)
    { "name": "hiring",    "start_s": 36.5,
      "text": fix("Databricks and Snowflake — four hundred open AI roles, "
                  "velocity up thirty-four percent.") },

    # window 45.5 → 53.5s  (8s)
    { "name": "launches",  "start_s": 45.5,
      "text": fix("Every launch auto-routed to the right Slack channel — "
                  "no manual sorting, every signal finds the right team.") },

    # window 54.5 → 59.5s  (5s)
    { "name": "news",      "start_s": 54.5,
      "text": fix("Funding, acquisitions, leadership moves — "
                  "the changes that matter.") },

    # window 60.5 → 71.5s  (11s)
    { "name": "sources",   "start_s": 61.5,
      "text": fix("Open any brief and see which sources drove it. "
                  "Every claim traced to a source. Governance, built in.") },

    # window 72.5 → 81.5s  (9s)
    { "name": "ask_ci",    "start_s": 72.5,
      "text": fix("Ask CI answers any question in seconds, grounded in live data — "
                  "with synthesised insights and suggested next steps.") },

    # window 83.0 → 89.5s  (6.5s)
    { "name": "close",     "start_s": 83.0,
      "text": fix("Ack-tee-an Competitive Intelligence. Know before it matters.") },
]

def tts(text, out_path):
    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
        headers={"xi-api-key": API_KEY, "Content-Type": "application/json", "Accept": "audio/mpeg"},
        json={"text": text, "model_id": MODEL_ID,
              "voice_settings": {"stability": 0.48, "similarity_boost": 0.76,
                                 "style": 0.18, "use_speaker_boost": True}},
        timeout=60)
    if r.status_code != 200:
        print(f"  ERROR {r.status_code}: {r.text[:300]}"); sys.exit(1)
    open(out_path, "wb").write(r.content)

def get_dur(path):
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", path],
        stderr=subprocess.DEVNULL)
    return float(json.loads(out)["format"]["duration"])

def build_mix(clips):
    inputs = ["-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo:d={TOTAL_DUR}"]
    for _, p in clips: inputs += ["-i", p]
    delays  = [f"[{i+1}]adelay={int(s*1000)}|{int(s*1000)}[d{i}]" for i,(s,_) in enumerate(clips)]
    mix_in  = "".join(f"[d{i}]" for i in range(len(clips)))
    f_cmplx = "; ".join(delays + [f"{mix_in}amix=inputs={len(clips)}:normalize=0[out]"])
    subprocess.run(
        ["ffmpeg", "-y", *inputs, "-filter_complex", f_cmplx,
         "-map", "[out]", "-t", str(TOTAL_DUR), "-ar", "44100", "-ab", "192k",
         os.path.abspath(OUT_FILE)],
        check=True, stderr=subprocess.DEVNULL)

def main():
    print(f"Voice: {voice_key}  |  {len(SEGMENTS)} clips\n")
    clips = []
    with tempfile.TemporaryDirectory() as tmp:
        for s in SEGMENTS:
            path = os.path.join(tmp, f"{s['name']}.mp3")
            print(f"  [{s['name']:12s}] @{s['start_s']:5.1f}s  \"{s['text'][:55]}…\"")
            tts(s["text"], path)
            dur = get_dur(path)
            end = s["start_s"] + dur
            next_start = next((x["start_s"] for x in SEGMENTS if x["start_s"] > s["start_s"]), TOTAL_DUR)
            flag = "  ⚠ OVERLAP" if end > next_start else ""
            print(f"               → {dur:.2f}s  (ends {end:.1f}s, next scene {next_start:.1f}s){flag}")
            clips.append((s["start_s"], path))
            time.sleep(0.25)
        print(f"\nMixing into {TOTAL_DUR}s track…")
        build_mix(clips)
    kb = os.path.getsize(os.path.abspath(OUT_FILE)) // 1024
    print(f"\n✓  narration.mp3 written  ({kb} KB)")

if __name__ == "__main__":
    main()
