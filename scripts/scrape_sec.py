"""
scrape_sec.py
-------------
For each public company in data/orgs.json that has a CIK, fetches the latest
DEF 14A proxy filing from SEC EDGAR and extracts board member names and titles.

Output: data/raw/sec/<ticker>.json  (one file per company)

Usage:
    cd boardgraph
    python scripts/scrape_sec.py                  # all companies with CIKs
    python scripts/scrape_sec.py --ticker MRNA     # single company
    python scripts/scrape_sec.py --dry-run         # print URLs, don't download
"""

import json
import re
import time
import argparse
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parent.parent
ORGS_FILE  = ROOT / "data" / "orgs.json"
RAW_SEC    = ROOT / "data" / "raw" / "sec"
RAW_SEC.mkdir(parents=True, exist_ok=True)

EDGAR_BASE = "https://data.sec.gov"
EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"

# SEC requires a descriptive User-Agent
HEADERS = {
    "User-Agent": "BoardGraph Research contact@example.com",
    "Accept-Encoding": "gzip, deflate",
}

RATE_LIMIT_SECS = 0.12  # EDGAR allows ~10 req/s; stay conservative


# ── EDGAR helpers ─────────────────────────────────────────────────────────────
def get_latest_def14a(cik: str) -> Optional[dict]:
    """Return metadata for the most recent DEF 14A filing for a given CIK."""
    cik_padded = cik.lstrip("0").zfill(10)
    url = f"{EDGAR_BASE}/submissions/CIK{cik_padded}.json"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    time.sleep(RATE_LIMIT_SECS)

    filings = data.get("filings", {}).get("recent", {})
    forms   = filings.get("form", [])
    dates   = filings.get("filingDate", [])
    accnums = filings.get("accessionNumber", [])

    for form, date, acc in zip(forms, dates, accnums):
        if form == "DEF 14A":
            return {
                "cik":        cik_padded,
                "accession":  acc.replace("-", ""),
                "date":       date,
                "accession_fmt": acc,
            }
    return None


def get_filing_index(cik: str, accession: str) -> list[dict]:
    """Return the list of documents in a filing."""
    url = f"{EDGAR_BASE}/Archives/edgar/data/{int(cik)}/{accession}/{accession}-index.json"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT_SECS)
    return resp.json().get("directory", {}).get("item", [])


def get_primary_doc_url(cik: str, accession: str, items: list[dict]) -> Optional[str]:
    """Find the main HTM/HTML document in a filing's index."""
    base = f"{EDGAR_BASE}/Archives/edgar/data/{int(cik)}/{accession}/"
    # Prefer the primary DEF 14A document (usually the largest .htm file)
    htm_items = [i for i in items if i.get("name", "").lower().endswith((".htm", ".html"))
                 and "def14a" not in i.get("name", "").lower()]
    if not htm_items:
        htm_items = [i for i in items if i.get("name", "").lower().endswith((".htm", ".html"))]
    if not htm_items:
        return None
    # Pick largest file (most likely to be the full proxy)
    largest = max(htm_items, key=lambda x: int(x.get("size", 0) or 0))
    return base + largest["name"]


# ── Director extraction ────────────────────────────────────────────────────────
DIRECTOR_SECTION_PATTERNS = [
    re.compile(r"(director|board of director|nominees? for director)", re.I),
]

TITLE_KEYWORDS = [
    "director", "chair", "ceo", "chief executive", "president",
    "founder", "officer", "principal", "trustee",
]

def extract_directors_from_html(html: str, company_name: str) -> list[dict]:
    """
    Parse a DEF 14A HTML document and extract board director names + titles.

    Strategy:
      1. Find the first table that appears after a "Directors" heading.
      2. Extract rows: first cell → name, second cell → title/bio snippet.
      3. Fall back to scanning all <td> text for patterns.
    """
    soup = BeautifulSoup(html, "lxml")
    directors = []
    seen = set()

    # ── Strategy 1: heading → nearest table ──────────────────────────────────
    for heading in soup.find_all(["h1", "h2", "h3", "h4", "p", "div", "td"]):
        text = heading.get_text(" ", strip=True)
        if not any(p.search(text) for p in DIRECTOR_SECTION_PATTERNS):
            continue
        # Walk forward siblings / parents to find the next table
        candidate = heading
        for _ in range(12):
            candidate = candidate.find_next(["table", "h2", "h3"])
            if candidate is None:
                break
            if candidate.name in ("h2", "h3"):
                break
            if candidate.name == "table":
                rows = candidate.find_all("tr")
                for row in rows:
                    cells = [td.get_text(" ", strip=True) for td in row.find_all(["td", "th"])]
                    if len(cells) < 2:
                        continue
                    name_cell  = cells[0].strip()
                    title_cell = cells[1].strip() if len(cells) > 1 else ""
                    if not name_cell or len(name_cell) > 60:
                        continue
                    # Skip header rows
                    if any(kw in name_cell.lower() for kw in ["name", "director", "position", "age"]):
                        continue
                    # Must look like a person name (2+ words, no numbers)
                    words = name_cell.split()
                    if len(words) < 2 or any(c.isdigit() for c in name_cell):
                        continue
                    key = name_cell.lower()
                    if key not in seen:
                        seen.add(key)
                        directors.append({
                            "name":    name_cell,
                            "title":   title_cell[:200],
                            "company": company_name,
                            "source":  "sec_def14a",
                        })
                if directors:
                    return directors

    # ── Strategy 2: scan all bold/strong text for person-like entries ─────────
    for tag in soup.find_all(["b", "strong"]):
        text = tag.get_text(" ", strip=True)
        words = text.split()
        if 2 <= len(words) <= 5 and not any(c.isdigit() for c in text):
            # Check if followed by a title keyword nearby
            sibling_text = ""
            sib = tag.find_next_sibling()
            if sib:
                sibling_text = sib.get_text(" ", strip=True).lower()
            parent_text = tag.parent.get_text(" ", strip=True).lower() if tag.parent else ""
            if any(kw in sibling_text or kw in parent_text for kw in TITLE_KEYWORDS):
                key = text.lower()
                if key not in seen:
                    seen.add(key)
                    directors.append({
                        "name":    text,
                        "title":   sibling_text[:200],
                        "company": company_name,
                        "source":  "sec_def14a_fallback",
                    })

    return directors


# ── Main ──────────────────────────────────────────────────────────────────────
def scrape_company(org: dict, dry_run: bool = False) -> Optional[dict]:
    cik    = org.get("cik")
    ticker = org.get("ticker") or org["id"].replace(" ", "_")
    name   = org["id"]

    if not cik:
        print(f"  [{ticker}] skipped — no CIK")
        return None

    out_path = RAW_SEC / f"{ticker}.json"

    print(f"  [{ticker}] fetching latest DEF 14A …")
    try:
        filing = get_latest_def14a(cik)
    except Exception as e:
        print(f"  [{ticker}] ERROR fetching submissions: {e}")
        return None

    if not filing:
        print(f"  [{ticker}] no DEF 14A found")
        return None

    print(f"  [{ticker}] found filing {filing['accession_fmt']} ({filing['date']})")

    if dry_run:
        return filing

    try:
        items = get_filing_index(filing["cik"], filing["accession"])
        doc_url = get_primary_doc_url(filing["cik"], filing["accession"], items)
    except Exception as e:
        print(f"  [{ticker}] ERROR fetching index: {e}")
        return None

    if not doc_url:
        print(f"  [{ticker}] could not find primary document")
        return None

    print(f"  [{ticker}] downloading {doc_url}")
    try:
        resp = requests.get(doc_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        time.sleep(RATE_LIMIT_SECS)
    except Exception as e:
        print(f"  [{ticker}] ERROR downloading: {e}")
        return None

    directors = extract_directors_from_html(resp.text, name)
    print(f"  [{ticker}] found {len(directors)} director(s)")

    result = {
        "org_id":    name,
        "ticker":    ticker,
        "cik":       cik,
        "filing":    filing,
        "doc_url":   doc_url,
        "directors": directors,
    }

    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  [{ticker}] saved → {out_path.relative_to(ROOT)}")

    return result


def main():
    parser = argparse.ArgumentParser(description="Scrape DEF 14A board data from SEC EDGAR")
    parser.add_argument("--ticker", help="Scrape only this ticker (e.g. MRNA)")
    parser.add_argument("--dry-run", action="store_true", help="Print filing URLs without downloading")
    args = parser.parse_args()

    with open(ORGS_FILE) as f:
        orgs = json.load(f)

    if args.ticker:
        orgs = [o for o in orgs if o.get("ticker", "").upper() == args.ticker.upper()]
        if not orgs:
            print(f"Ticker '{args.ticker}' not found in orgs.json")
            return

    public_orgs = [o for o in orgs if o.get("cik")]
    print(f"Scraping {len(public_orgs)} public companies …\n")

    for org in tqdm(public_orgs, desc="Companies", unit="co"):
        scrape_company(org, dry_run=args.dry_run)
        print()


if __name__ == "__main__":
    main()
