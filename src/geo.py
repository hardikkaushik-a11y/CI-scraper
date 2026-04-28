"""
geo.py — Shared country normalization
─────────────────────────────────────
Maps freeform Location strings into a canonical country label.
Used by:
  - assistant_backend.py (Ask CI geographic context)
  - scripts/build_dashboard_v3.py (per-company country footprint on dashboard)
"""

import re

_COUNTRY_PATTERNS = [
    # India — cities + region codes
    (r"\b(india|bengaluru|bangalore|gurugram|gurgaon|hyderabad|chennai|mumbai|pune|noida|kolkata|new\s+delhi|delhi|ind[-_])\b", "India"),
    # United States — codes, states, common cities
    (r"\b(united\s+states|usa|^us$|us[-_ ]|amer|new\s+york|san\s+francisco|california|seattle|atlanta|austin|denver|menlo\s+park|oakland|raleigh|north\s+carolina|texas|massachusetts|boston|chicago|washington|virginia|colorado|wa-bellevue)\b", "United States"),
    # UK
    (r"\b(united\s+kingdom|england|^uk$|uk[-_]|london|manchester|edinburgh|scotland)\b", "United Kingdom"),
    # Ireland
    (r"\b(ireland|dublin)\b", "Ireland"),
    # EU
    (r"\b(germany|berlin|munich|hamburg|frankfurt|cologne|deu[-_])\b", "Germany"),
    (r"\b(france|paris|lyon|fra[-_])\b", "France"),
    (r"\b(spain|madrid|barcelona|esp[-_])\b", "Spain"),
    (r"\b(netherlands|amsterdam|nld[-_])\b", "Netherlands"),
    (r"\b(belgium|brussels|antwerp|bel[-_])\b", "Belgium"),
    (r"\b(poland|warsaw|krakow|pl[-_]warsaw|pol[-_])\b", "Poland"),
    (r"\b(italy|milan|rome|ita[-_])\b", "Italy"),
    (r"\b(switzerland|zurich|geneva|che[-_])\b", "Switzerland"),
    (r"\b(sweden|stockholm|swe[-_])\b", "Sweden"),
    (r"\b(czech|prague|cze[-_])\b", "Czechia"),
    # APAC
    (r"\b(japan|tokyo|jpn[-_])\b", "Japan"),
    (r"\b(singapore|sgp[-_])\b", "Singapore"),
    (r"\b(australia|sydney|melbourne|aus[-_])\b", "Australia"),
    (r"\b(china|beijing|shanghai|chn[-_])\b", "China"),
    (r"\b(south\s+korea|seoul|kor[-_])\b", "South Korea"),
    # Americas
    (r"\b(canada|toronto|vancouver|montreal|kitchener|ontario|quebec|can[-_])\b", "Canada"),
    (r"\b(brazil|sao\s+paulo|brazil[-_]|bra[-_])\b", "Brazil"),
    (r"\b(mexico|mexico\s+city|mex[-_])\b", "Mexico"),
    # Middle East
    (r"\b(israel|tel\s+aviv|isr[-_])\b", "Israel"),
    (r"\b(uae|dubai|abu\s+dhabi)\b", "UAE"),
    # Remote (last — only when nothing else matches)
    (r"\b(remote|anywhere|worldwide|global)\b", "Remote"),
]
_COUNTRY_RE = [(re.compile(p, re.I), c) for p, c in _COUNTRY_PATTERNS]


def country_from_location(loc: str) -> str:
    """Normalize freeform location → country label. Returns 'Unknown' if no match."""
    if not loc:
        return "Unknown"
    s = loc.strip().lower()
    for rx, country in _COUNTRY_RE:
        if rx.search(s):
            return country
    return "Unknown"
