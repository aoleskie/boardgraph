"""
scrape_wikipedia.py
-------------------
Fetches board/leadership sections from Wikipedia pages for organisations and
individuals listed in orgs.json and people.json.

Two modes:
  --orgs    Scrape each org's Wikipedia page for board/leadership sections
  --people  Scrape each person's Wikipedia page for affiliation mentions

Output: data/raw/wikipedia/<slug>.json

Usage:
    cd boardgraph
    python scripts/scrape_wikipedia.py --orgs
    python scripts/scrape_wikipedia.py --people
    python scripts/scrape_wikipedia.py --orgs --people   # both
    python scripts/scrape_wikipedia.py --slug Moderna    # single page
"""

import json
import re
import time
import argparse
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
ORGS_FILE   = ROOT / "data" / "orgs.json"
PEOPLE_FILE = ROOT / "data" / "people.json"
RAW_WIKI    = ROOT / "data" / "raw" / "wikipedia"
RAW_WIKI.mkdir(parents=True, exist_ok=True)

WIKI_API = "https://en.wikipedia.org/w/api.php"
RATE_LIMIT_SECS = 0.5

HEADERS = {
    "User-Agent": "BoardGraph Research contact@example.com",
}

# Sections likely to contain board / affiliation data
BOARD_SECTION_KEYWORDS = [
    "board of directors", "board members", "leadership", "management",
    "governance", "trustees", "directors", "founders", "notable alumni",
    "scientific advisory", "advisory board",
]

PERSON_AFFILIATION_KEYWORDS = [
    "board", "director", "adviser", "advisor", "trustee", "chair",
    "founder", "co-founder", "investor", "partner", "fellow",
]


# ── Wikipedia API helpers ─────────────────────────────────────────────────────
def wiki_fetch_sections(slug: str) -> Optional[dict]:
    """
    Return a dict with:
      - title: canonical page title
      - summary: lead section plain text
      - sections: list of {title, content} for relevant sections
      - categories: list of category names
      - url: canonical URL
    """
    params_parse = {
        "action": "parse",
        "page":   slug,
        "prop":   "sections|text|categories",
        "format": "json",
    }
    resp = requests.get(WIKI_API, params=params_parse, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    time.sleep(RATE_LIMIT_SECS)

    if "error" in data:
        return None

    page_data = data.get("parse", {})
    html      = page_data.get("text", {}).get("*", "")
    soup      = BeautifulSoup(html, "lxml")

    # Remove edit links and footnote markers
    for tag in soup.find_all(class_=["mw-editsection", "reference"]):
        tag.decompose()

    # Extract lead text (everything before first h2)
    lead_parts = []
    for el in soup.children:
        if el.name == "h2":
            break
        if el.name == "p":
            lead_parts.append(el.get_text(" ", strip=True))
    summary = " ".join(lead_parts)[:1000]

    # Extract named sections
    sections     = page_data.get("sections", [])
    raw_sections = []

    for sec in sections:
        title_text = sec.get("line", "").lower()
        if any(kw in title_text for kw in BOARD_SECTION_KEYWORDS):
            # Find the section header in the parsed HTML and grab its content
            anchor  = sec.get("anchor", "")
            heading = soup.find(id=anchor)
            if heading:
                content_parts = []
                el = heading.find_parent().find_next_sibling()
                while el and el.name not in ("h2", "h3", "h4"):
                    content_parts.append(el.get_text(" ", strip=True))
                    el = el.find_next_sibling()
                raw_sections.append({
                    "title":   sec.get("line", ""),
                    "content": " ".join(content_parts)[:3000],
                })

    categories = [c.get("*", "") for c in page_data.get("categories", [])]

    # Also extract any tables that look like board rosters
    board_tables = []
    for tbl in soup.find_all("table", class_=re.compile("wikitable", re.I)):
        headers = [th.get_text(" ", strip=True).lower() for th in tbl.find_all("th")]
        if any(kw in " ".join(headers) for kw in ["name", "director", "member", "title", "position"]):
            rows = []
            for tr in tbl.find_all("tr")[1:]:  # skip header row
                cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
                if cells:
                    rows.append(cells)
            if rows:
                board_tables.append({"headers": headers, "rows": rows[:30]})

    return {
        "slug":         slug,
        "title":        page_data.get("title", slug),
        "url":          f"https://en.wikipedia.org/wiki/{slug}",
        "summary":      summary,
        "sections":     raw_sections,
        "board_tables": board_tables,
        "categories":   categories,
    }


def extract_person_affiliations(person_name: str, slug: str) -> Optional[dict]:
    """
    For a known person, fetch their Wikipedia page and extract:
    - A list of organisations they appear affiliated with
    - The lead paragraph (bio summary)
    """
    result = wiki_fetch_sections(slug)
    if not result:
        return None

    text = result["summary"].lower()

    # Scan for known org names in the bio text
    with open(ORGS_FILE) as f:
        orgs = json.load(f)

    found_orgs = []
    for org in orgs:
        org_id = org["id"]
        # Simple substring match; case-insensitive
        if org_id.lower() in text or org_id.lower().replace("/", " ") in text:
            found_orgs.append(org_id)

    # Also extract links from the lead section (Wikipedia links often point to orgs)
    params_extract = {
        "action":    "query",
        "titles":    slug,
        "prop":      "links",
        "pllimit":   "100",
        "format":    "json",
    }
    resp = requests.get(WIKI_API, params=params_extract, headers=HEADERS, timeout=15)
    time.sleep(RATE_LIMIT_SECS)

    linked_titles = []
    if resp.ok:
        pages = resp.json().get("query", {}).get("pages", {})
        for page in pages.values():
            linked_titles = [l["title"] for l in page.get("links", [])]

    # Match linked Wikipedia titles to our org list
    org_titles = {o.get("wikipedia", "").lower(): o["id"] for o in orgs if o.get("wikipedia")}
    for title in linked_titles:
        match = org_titles.get(title.lower().replace(" ", "_"))
        if match and match not in found_orgs:
            found_orgs.append(match)

    return {
        "person":    person_name,
        "slug":      slug,
        "url":       result["url"],
        "summary":   result["summary"],
        "found_orgs": found_orgs,
    }


# ── Slug resolution ───────────────────────────────────────────────────────────
def resolve_person_slug(name: str) -> Optional[str]:
    """Search Wikipedia for a person's page slug."""
    params = {
        "action":     "query",
        "list":       "search",
        "srsearch":   name,
        "srlimit":    3,
        "format":     "json",
    }
    resp = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT_SECS)

    results = resp.json().get("query", {}).get("search", [])
    if results:
        # Use first result's title as slug
        return results[0]["title"].replace(" ", "_")
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def scrape_org(org: dict) -> Optional[dict]:
    slug = org.get("wikipedia")
    if not slug:
        print(f"  [{org['id']}] skipped — no Wikipedia slug")
        return None

    out_path = RAW_WIKI / f"{slug}.json"
    print(f"  Fetching Wikipedia: {slug} …")
    try:
        data = wiki_fetch_sections(slug)
    except Exception as e:
        print(f"  ERROR: {e}")
        return None

    if not data:
        print(f"  Not found: {slug}")
        return None

    data["org_id"] = org["id"]
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    tables = len(data.get("board_tables", []))
    sections = len(data.get("sections", []))
    print(f"  → saved ({sections} sections, {tables} tables)")
    return data


def scrape_person(person: dict) -> Optional[dict]:
    name = person["id"]
    slug = resolve_person_slug(name)
    if not slug:
        print(f"  [{name}] Wikipedia page not found")
        return None

    out_path = RAW_WIKI / f"person_{slug}.json"
    print(f"  [{name}] → {slug}")
    try:
        data = extract_person_affiliations(name, slug)
    except Exception as e:
        print(f"  [{name}] ERROR: {e}")
        return None

    if not data:
        return None

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  → found orgs: {data['found_orgs']}")
    return data


def main():
    parser = argparse.ArgumentParser(description="Scrape Wikipedia for board and affiliation data")
    parser.add_argument("--orgs",   action="store_true", help="Scrape org pages")
    parser.add_argument("--people", action="store_true", help="Scrape people pages")
    parser.add_argument("--slug",   help="Scrape a single Wikipedia slug")
    args = parser.parse_args()

    if args.slug:
        data = wiki_fetch_sections(args.slug)
        print(json.dumps(data, indent=2))
        return

    if not args.orgs and not args.people:
        parser.print_help()
        return

    if args.orgs:
        with open(ORGS_FILE) as f:
            orgs = json.load(f)
        wiki_orgs = [o for o in orgs if o.get("wikipedia")]
        print(f"\nScraping {len(wiki_orgs)} org Wikipedia pages …\n")
        for org in wiki_orgs:
            scrape_org(org)
            print()

    if args.people:
        with open(PEOPLE_FILE) as f:
            people = json.load(f)
        print(f"\nScraping {len(people)} people Wikipedia pages …\n")
        for person in people:
            scrape_person(person)
            print()


if __name__ == "__main__":
    main()
