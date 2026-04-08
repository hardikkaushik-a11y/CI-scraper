"""
Create jobs_enriched_v2.csv — curated dataset for dashboard v2.

Filters jobs_enriched.csv to allowed companies only and adds product_area column.
Run from repo root: python scripts/create_v2_dataset.py
"""

import csv
import sys
from pathlib import Path

# ── Allowed companies and their product_area ──────────────────────────────────
PRODUCT_AREA_MAP = {
    "Atlan":       "Data Intelligence",
    "Collibra":    "Data Intelligence",
    "Alation":     "Data Intelligence",
    "Monte Carlo": "Data Observability",
    "Bigeye":      "Data Observability",
    "Acceldata":   "Data Observability",
    "Pinecone":    "VectorAI",
    "Qdrant":      "VectorAI",
    "Milvus":      "VectorAI",
    "Snowflake":   "AI Analyst",
    "Databricks":  "AI Analyst",
}

ALLOWED = set(PRODUCT_AREA_MAP.keys())

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
INPUT  = REPO_ROOT / "data" / "jobs_enriched.csv"
OUTPUT = REPO_ROOT / "data" / "jobs_enriched_v2.csv"


def main():
    if not INPUT.exists():
        print(f"ERROR: {INPUT} not found", file=sys.stderr)
        sys.exit(1)

    kept = 0
    skipped = 0

    with INPUT.open(newline="", encoding="utf-8") as fin, \
         OUTPUT.open("w", newline="", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames + ["product_area"]
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            company = row.get("Company", "").strip()
            if company not in ALLOWED:
                skipped += 1
                continue
            row["product_area"] = PRODUCT_AREA_MAP[company]
            writer.writerow(row)
            kept += 1

    print(f"Done. Kept: {kept} rows | Skipped: {skipped} rows")
    print(f"Written to: {OUTPUT}")


if __name__ == "__main__":
    main()
