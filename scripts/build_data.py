"""
build_data.py
-------------
Reads data/people.json and data/orgs.json and writes site/data.js —
a plain JS file that defines window.PEOPLE_DATA and window.ORGS_DATA.

Optionally merges new names discovered in raw SEC and Wikipedia scrape outputs
into people.json as draft entries for manual review.

Usage:
    cd boardgraph
    python scripts/build_data.py                  # just rebuild site/data.js
    python scripts/build_data.py --merge-raw      # also fold in raw scraped names
    python scripts/build_data.py --validate       # check for unknown org refs
"""

import json
import re
import argparse
from pathlib import Path
from datetime import date

ROOT        = Path(__file__).resolve().parent.parent
PEOPLE_FILE = ROOT / "data" / "people.json"
ORGS_FILE   = ROOT / "data" / "orgs.json"
RAW_SEC     = ROOT / "data" / "raw" / "sec"
RAW_WIKI    = ROOT / "data" / "raw" / "wikipedia"
OUT_JS      = ROOT / "site" / "data.js"
OUT_JS.parent.mkdir(parents=True, exist_ok=True)


# ── Validation ────────────────────────────────────────────────────────────────
def validate(people: list[dict], orgs: list[dict]) -> list[str]:
    """Return a list of warning strings for missing or mismatched references."""
    org_ids = {o["id"] for o in orgs}
    warnings = []
    for person in people:
        for org_ref in person.get("orgs", []):
            if org_ref not in org_ids:
                warnings.append(
                    f"  ⚠ '{person['id']}' references unknown org '{org_ref}' — "
                    f"add it to orgs.json or fix the typo"
                )
        if not person.get("initials"):
            warnings.append(f"  ⚠ '{person['id']}' has no initials field")
        if person.get("category") not in ("academic", "executive", "vc", "regulatory"):
            warnings.append(
                f"  ⚠ '{person['id']}' has unexpected category '{person.get('category')}'"
            )
    return warnings


# ── Raw merge ─────────────────────────────────────────────────────────────────
def derive_initials(name: str) -> str:
    words = name.split()
    return "".join(w[0].upper() for w in words if w)[:3]


def merge_raw_into_people(people: list[dict], orgs: list[dict]) -> tuple[list[dict], int]:
    """
    Scan raw SEC + Wikipedia outputs and add newly discovered people as draft
    entries (marked with "review": true).  Never overwrites existing entries.
    Returns (updated_people, count_added).
    """
    existing_names = {p["id"].lower() for p in people}
    org_ids        = {o["id"] for o in orgs}
    new_entries    = []

    # ── From SEC raw files ───────────────────────────────────────────────────
    for sec_file in sorted(RAW_SEC.glob("*.json")):
        try:
            data = json.loads(sec_file.read_text())
        except Exception:
            continue

        org_id     = data.get("org_id", "")
        directors  = data.get("directors", [])

        for d in directors:
            name = d.get("name", "").strip()
            if not name or name.lower() in existing_names:
                continue
            # Basic sanity: must look like a real name
            words = name.split()
            if len(words) < 2 or len(words) > 5:
                continue
            if any(c.isdigit() for c in name):
                continue

            existing_names.add(name.lower())
            new_entries.append({
                "id":          name,
                "initials":    derive_initials(name),
                "role":        d.get("title", "")[:100],
                "category":    "executive",  # default; adjust manually
                "orgs":        [org_id] if org_id in org_ids else [],
                "notes":       "",
                "last_updated": str(date.today()),
                "sources":     [data.get("doc_url", "")],
                "review":      True,  # flag for manual review
            })

    # ── From Wikipedia raw files ─────────────────────────────────────────────
    for wiki_file in sorted(RAW_WIKI.glob("*.json")):
        try:
            data = json.loads(wiki_file.read_text())
        except Exception:
            continue

        # Only process person files (not org files)
        if "person" not in wiki_file.name:
            continue

        person_name = data.get("person", "")
        if not person_name or person_name.lower() in existing_names:
            continue

        found_orgs = [o for o in data.get("found_orgs", []) if o in org_ids]
        existing_names.add(person_name.lower())
        new_entries.append({
            "id":          person_name,
            "initials":    derive_initials(person_name),
            "role":        "",
            "category":    "academic",
            "orgs":        found_orgs,
            "notes":       data.get("summary", "")[:200],
            "last_updated": str(date.today()),
            "sources":     [data.get("url", "")],
            "review":      True,
        })

    people.extend(new_entries)
    return people, len(new_entries)


# ── JS generation ─────────────────────────────────────────────────────────────
JS_TEMPLATE = """\
// site/data.js — GENERATED by scripts/build_data.py on {date}
// DO NOT EDIT DIRECTLY. Edit data/people.json and data/orgs.json, then re-run:
//   python scripts/build_data.py

/* eslint-disable */
window.PEOPLE_DATA = {people_json};

window.ORGS_DATA = {orgs_json};
"""


def build_js(people: list[dict], orgs: list[dict]) -> str:
    # Strip internal-only fields before shipping to the browser
    browser_people = []
    for p in people:
        if p.get("review"):
            continue  # skip unreviewed draft entries
        browser_people.append({
            "id":       p["id"],
            "initials": p.get("initials", derive_initials(p["id"])),
            "role":     p.get("role", ""),
            "category": p.get("category", "executive"),
            "orgs":     p.get("orgs", []),
            "notes":    p.get("notes", ""),
        })

    browser_orgs = [
        {
            "id":   o["id"],
            "type": o.get("type", "biotech"),
        }
        for o in orgs
    ]

    return JS_TEMPLATE.format(
        date=str(date.today()),
        people_json=json.dumps(browser_people, indent=2),
        orgs_json=json.dumps(browser_orgs, indent=2),
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Build site/data.js from data files")
    parser.add_argument("--merge-raw", action="store_true",
                        help="Merge raw scraped names into people.json as review drafts")
    parser.add_argument("--validate", action="store_true",
                        help="Validate org references and print warnings")
    args = parser.parse_args()

    with open(PEOPLE_FILE) as f:
        people = json.load(f)
    with open(ORGS_FILE) as f:
        orgs = json.load(f)

    print(f"Loaded {len(people)} people, {len(orgs)} orgs")

    if args.validate:
        warnings = validate(people, orgs)
        if warnings:
            print(f"\n{len(warnings)} validation warning(s):")
            for w in warnings:
                print(w)
        else:
            print("✓ All org references are valid")

    if args.merge_raw:
        people, n_added = merge_raw_into_people(people, orgs)
        if n_added:
            with open(PEOPLE_FILE, "w") as f:
                json.dump(people, f, indent=2)
            print(f"Added {n_added} draft entries to people.json (marked 'review: true')")
            print("→ Open data/people.json, review entries, remove 'review' flag when done")
        else:
            print("No new people found in raw data")

    js = build_js(people, orgs)
    OUT_JS.write_text(js)

    # Stats
    shown = [p for p in people if not p.get("review")]
    print(f"\n✓ Wrote site/data.js")
    print(f"  {len(shown)} people exported")
    print(f"  {len(orgs)} orgs exported")
    edges = sum(
        1
        for i in range(len(shown))
        for j in range(i + 1, len(shown))
        if set(shown[i]["orgs"]) & set(shown[j]["orgs"])
    )
    print(f"  {edges} co-affiliation edges computed")


if __name__ == "__main__":
    main()
