"""
build_data.py
-------------
Reads data/people.json and data/orgs.json and writes site/data.js.

Usage:
    python scripts/build_data.py                  # rebuild site/data.js
    python scripts/build_data.py --merge-raw      # merge scraped data as review drafts
    python scripts/build_data.py --auto-approve   # approve entries that pass quality filter
    python scripts/build_data.py --validate       # check for unknown org refs
    python scripts/build_data.py --stats          # show breakdown of people by source/status
"""

import json
import re
import argparse
from pathlib import Path
from datetime import date

ROOT         = Path(__file__).resolve().parent.parent
PEOPLE_FILE  = ROOT / "data" / "people.json"
ORGS_FILE    = ROOT / "data" / "orgs.json"
RAW_SEC      = ROOT / "data" / "raw" / "sec"
RAW_WIKI     = ROOT / "data" / "raw" / "wikipedia"
RAW_WIKIDATA = ROOT / "data" / "raw" / "wikidata"
OUT_JS       = ROOT / "site" / "data.js"
OUT_JS.parent.mkdir(parents=True, exist_ok=True)


# ── Name quality filter ───────────────────────────────────────────────────────

JUNK_WORDS = re.compile(
    r"\b(committee|compensation|proxy|statement|proposal|shareholder|fiscal|"
    r"annual|report|section|table|contents|information|plan|policy|pursuant|"
    r"exchange|securities|total|shares|common|performance|equity|salary|"
    r"corporation|company|foundation|incorporated|university|institute|"
    r"association|council|authority|department|highlights|act|rule|"
    r"management|governance|audit|finance|nominating|approve|independent|"
    r"registered|accounting|firm|vote|required|matters|guidelines|"
    r"group|street|avenue|road|boulevard|drive|lane|place)\b",
    re.IGNORECASE,
)

HONORIFIC = re.compile(
    r"^(?:Mr\.?|Ms\.?|Mrs\.?|Dr\.?|Prof\.?|Sir)\s+",
    re.IGNORECASE,
)


def clean_name(raw: str) -> str:
    name = raw.strip()
    if "\n" in name:
        name = name.rsplit("\n", 1)[-1].strip()
    name = HONORIFIC.sub("", name).strip()
    name = re.sub(r"[\s\u00a0]+", " ", name).strip()
    return name


def name_looks_valid(name: str) -> tuple[bool, str]:
    if not name:
        return False, "empty"
    if "\n" in name or "\r" in name or "\t" in name:
        return False, "newline in name"
    if "\u00a0" in name:
        return False, "non-breaking space"
    if JUNK_WORDS.search(name):
        return False, "junk keyword"
    words = name.split()
    if not (2 <= len(words) <= 5):
        return False, f"word count {len(words)}"
    if any(c.isdigit() for c in name):
        return False, "digit in name"
    if len(name) > 50:
        return False, "name too long"
    if not name[0].isupper():
        return False, "not title case"
    if not all(w[0].isupper() for w in words if len(w) > 2):
        return False, "word not capitalised"
    if sum(1 for w in words if w.isupper() and len(w) > 2) >= 2:
        return False, "all-caps words"
    return True, "ok"


def derive_initials(name: str) -> str:
    words = name.split()
    return "".join(w[0].upper() for w in words if w)[:3]


# ── Validation ────────────────────────────────────────────────────────────────
def validate(people: list, orgs: list) -> list:
    org_ids  = {o["id"] for o in orgs}
    warnings = []
    for person in people:
        if person.get("review"):
            continue
        for org_ref in person.get("orgs", []):
            if org_ref not in org_ids:
                warnings.append(f"  ⚠  '{person['id']}' -> unknown org '{org_ref}'")
        if not person.get("initials"):
            warnings.append(f"  ⚠  '{person['id']}' has no initials")
        if person.get("category") not in ("academic", "executive", "vc", "regulatory"):
            warnings.append(
                f"  ⚠  '{person['id']}' unexpected category '{person.get('category')}'"
            )
    return warnings


# ── Merge raw data ────────────────────────────────────────────────────────────
def merge_raw_into_people(people: list, orgs: list) -> tuple:
    existing = {p["id"].lower() for p in people}
    org_ids  = {o["id"] for o in orgs}
    added    = []

    def add_entry(entry):
        key = entry["id"].lower()
        if key not in existing:
            existing.add(key)
            added.append(entry)

    # ── Wikidata (structured, highest trust) ─────────────────────────────────
    for wd_file in sorted(RAW_WIKIDATA.glob("*.json")):
        try:
            data = json.loads(wd_file.read_text())
        except Exception:
            continue
        org_id = data.get("org_id", "")
        for d in data.get("directors", []):
            name = clean_name(d.get("name", ""))
            ok, _ = name_looks_valid(name)
            if not ok:
                continue
            add_entry({
                "id":             name,
                "initials":       derive_initials(name),
                "role":           d.get("role", ""),
                "category":       "executive",
                "orgs":           [org_id] if org_id in org_ids else [],
                "notes":          "",
                "last_updated":   str(date.today()),
                "sources":        [f"wikidata:{d.get('qid', '')}"],
                "director_since": d.get("director_since", ""),
                "director_until": d.get("director_until", ""),
                "review":         True,
                "_source_type":   "wikidata",
            })

    # ── SEC EDGAR ────────────────────────────────────────────────────────────
    for sec_file in sorted(RAW_SEC.glob("*.json")):
        try:
            data = json.loads(sec_file.read_text())
        except Exception:
            continue
        org_id = data.get("org_id", "")
        for d in data.get("directors", []):
            name = clean_name(d.get("name", ""))
            ok, _ = name_looks_valid(name)
            if not ok:
                continue
            add_entry({
                "id":             name,
                "initials":       derive_initials(name),
                "role":           d.get("title", "")[:100],
                "category":       "executive",
                "orgs":           [org_id] if org_id in org_ids else [],
                "notes":          "",
                "last_updated":   str(date.today()),
                "sources":        [data.get("doc_url", "")],
                "director_since": d.get("director_since", ""),
                "review":         True,
                "_source_type":   "sec",
                "_extraction":    d.get("source", ""),
            })

    # ── Wikipedia ────────────────────────────────────────────────────────────
    for wiki_file in sorted(RAW_WIKI.glob("*.json")):
        try:
            data = json.loads(wiki_file.read_text())
        except Exception:
            continue
        if "person" not in wiki_file.name:
            continue
        person_name = clean_name(data.get("person", ""))
        ok, _ = name_looks_valid(person_name)
        if not ok:
            continue
        found_orgs = [o for o in data.get("found_orgs", []) if o in org_ids]
        add_entry({
            "id":           person_name,
            "initials":     derive_initials(person_name),
            "role":         "",
            "category":     "academic",
            "orgs":         found_orgs,
            "notes":        data.get("summary", "")[:200],
            "last_updated": str(date.today()),
            "sources":      [data.get("url", "")],
            "review":       True,
            "_source_type": "wikipedia",
        })

    people.extend(added)
    return people, len(added)


# ── Auto-approve ──────────────────────────────────────────────────────────────
#
# Wikidata  — approve if name is valid. Structured data, no parsing ambiguity.
#
# SEC       — approve if name is valid AND either:
#               (a) director_since is present (confirms it's a real board entry,
#                   not a heading or page number that slipped through), OR
#               (b) extraction used the reliable table strategy, not a fallback.
#
# Wikipedia — always keep for manual review (too noisy).

RELIABLE_SEC_SOURCES = {"sec_def14a_director_table", "sec_def14a_inline_age"}


def should_auto_approve(entry: dict) -> tuple:
    name = entry["id"]
    ok, reason = name_looks_valid(name)
    if not ok:
        return False, reason

    src = entry.get("_source_type", "")

    if src == "wikidata":
        return True, "wikidata structured data"

    if src == "sec":
        has_since    = bool(entry.get("director_since", "").strip())
        reliable_ext = entry.get("_extraction", "") in RELIABLE_SEC_SOURCES
        if has_since:
            return True, f"sec + director_since={entry['director_since']}"
        if reliable_ext:
            return True, f"sec + reliable extraction ({entry['_extraction']})"
        return False, "sec entry lacks director_since and used fallback extraction"

    return False, f"source '{src}' requires manual review"


def auto_approve(people: list) -> tuple:
    n_approved = 0
    for p in people:
        if not p.get("review"):
            continue
        ok, _ = should_auto_approve(p)
        if ok:
            del p["review"]
            p.pop("_source_type", None)
            p.pop("_extraction",  None)
            n_approved += 1
    n_remaining = sum(1 for p in people if p.get("review"))
    return people, n_approved, n_remaining


# ── Stats ─────────────────────────────────────────────────────────────────────
def print_stats(people: list):
    approved = [p for p in people if not p.get("review")]
    pending  = [p for p in people if p.get("review")]

    by_cat: dict = {}
    for p in approved:
        c = p.get("category", "unknown")
        by_cat[c] = by_cat.get(c, 0) + 1

    by_src: dict = {}
    for p in pending:
        s = p.get("_source_type", "unknown")
        by_src[s] = by_src.get(s, 0) + 1

    print(f"\nApproved: {len(approved)}")
    for cat, n in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"  {cat:<14} {n}")
    print(f"\nPending review: {len(pending)}")
    for src, n in sorted(by_src.items(), key=lambda x: -x[1]):
        print(f"  {src:<14} {n}")

    would = sum(1 for p in pending if should_auto_approve(p)[0])
    print(f"\n  {would} of {len(pending)} pending would pass --auto-approve")


# ── JS generation ─────────────────────────────────────────────────────────────
JS_TEMPLATE = """\
// site/data.js — GENERATED by scripts/build_data.py on {date}
// DO NOT EDIT DIRECTLY. Edit data/people.json and data/orgs.json, then re-run:
//   python scripts/build_data.py

/* eslint-disable */
window.PEOPLE_DATA = {people_json};

window.ORGS_DATA = {orgs_json};
"""


def build_js(people: list, orgs: list) -> str:
    browser_people = []
    for p in people:
        if p.get("review"):
            continue
        browser_people.append({
            "id":       p["id"],
            "initials": p.get("initials") or derive_initials(p["id"]),
            "role":     p.get("role", ""),
            "category": p.get("category", "executive"),
            "orgs":     p.get("orgs", []),
            "notes":    p.get("notes", ""),
        })
    browser_orgs = [{"id": o["id"], "type": o.get("type", "biotech")} for o in orgs]
    return JS_TEMPLATE.format(
        date=str(date.today()),
        people_json=json.dumps(browser_people, indent=2),
        orgs_json=json.dumps(browser_orgs, indent=2),
    )


def dedup_people(people: list) -> tuple:
    """
    Remove duplicates caused by middle-initial mismatches or nicknames
    (e.g. 'Rick Waddell' and 'Frederick H. Waddell').
    Keeps the richer entry (longer name, more orgs, non-empty role).
    """
    NICKNAMES = {
        "becky": "rebecca", "becca": "rebecca",
        "rick": "richard",  "rich": "richard",  "dick": "richard",
        "bob":  "robert",   "rob":  "robert",
        "bill": "william",  "will": "william",
        "jim":  "james",    "jamie": "james",
        "tom":  "thomas",   "tommy": "thomas",
        "mike": "michael",  "mick": "michael",
        "dave": "david",
        "steve": "stephen", "steven": "stephen",
        "kate": "katherine","kathy": "katherine",
        "liz":  "elizabeth","beth": "elizabeth",
        "sue":  "susan",
        "joe":  "joseph",
        "tony": "anthony",
        "chris": "christopher",
        "dan":  "daniel",
        "ed":   "edward",   "ted": "edward",
        "fred": "frederick",
        "ken":  "kenneth",
        "larry": "lawrence",
        "matt": "matthew",
        "ron":  "ronald",
        "sam":  "samuel",
    }

    TITLE_RE = re.compile(
        r"^(?:prof\.?\s+dr\.?|prof\.?|dr\.?|mr\.?|ms\.?|mrs\.?|"
        r"sir|dame|gen\.?|rev\.?|hon\.?)\s+",
        re.IGNORECASE,
    )

    def nkey(n):
        import unicodedata
        for _ in range(3):
            n2 = TITLE_RE.sub("", n).strip()
            if n2 == n:
                break
            n = n2
        n = "".join(c for c in unicodedata.normalize("NFD", n)
                    if unicodedata.category(c) != "Mn")
        n = re.sub(r"[.,]", "", n).strip().lower()
        parts = [p for p in n.split() if len(p) > 1]
        if parts:
            parts[0] = NICKNAMES.get(parts[0], parts[0])
        return " ".join(parts)

    seen: dict[str, dict] = {}
    for p in people:
        key = nkey(p["id"])
        if key not in seen:
            seen[key] = p
        else:
            # Merge: keep longer name, union orgs, prefer richer fields
            existing = seen[key]
            if len(p["id"]) > len(existing["id"]):
                existing["id"]       = p["id"]
                existing["initials"] = derive_initials(p["id"])
            existing["orgs"] = list(set(existing.get("orgs", [])) | set(p.get("orgs", [])))
            if p.get("role") and not existing.get("role"):
                existing["role"] = p["role"]
            if p.get("director_since") and not existing.get("director_since"):
                existing["director_since"] = p["director_since"]

    deduped  = list(seen.values())
    n_removed = len(people) - len(deduped)
    return deduped, n_removed


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Build site/data.js from data files")
    parser.add_argument("--merge-raw",    action="store_true",
                        help="Merge raw scraped data into people.json as review drafts")
    parser.add_argument("--auto-approve", action="store_true",
                        help="Approve review entries that pass the quality filter")
    parser.add_argument("--dedup",        action="store_true",
                        help="Remove duplicate people caused by middle-initial mismatches")
    parser.add_argument("--validate",     action="store_true",
                        help="Validate org references and field values")
    parser.add_argument("--stats",        action="store_true",
                        help="Show breakdown of people by source and status")
    args = parser.parse_args()

    with open(PEOPLE_FILE) as f:
        people = json.load(f)
    with open(ORGS_FILE) as f:
        orgs = json.load(f)

    print(f"Loaded {len(people)} people, {len(orgs)} orgs")

    if args.stats:
        print_stats(people)

    if args.validate:
        warnings = validate(people, orgs)
        if warnings:
            print(f"\n{len(warnings)} validation warning(s):")
            for w in warnings:
                print(w)
        else:
            print("✓ All references valid")

    if args.dedup:
        people, n_removed = dedup_people(people)
        print(f"\nRemoved {n_removed} duplicate(s)")
        with open(PEOPLE_FILE, "w") as f:
            json.dump(people, f, indent=2)

    if args.merge_raw:
        people, n_added = merge_raw_into_people(people, orgs)
        print(f"\nMerged {n_added} new draft entries")
        with open(PEOPLE_FILE, "w") as f:
            json.dump(people, f, indent=2)

    if args.auto_approve:
        people, n_approved, n_remaining = auto_approve(people)
        print(f"\nAuto-approved {n_approved}  ({n_remaining} still need manual review)")
        with open(PEOPLE_FILE, "w") as f:
            json.dump(people, f, indent=2)

    js = build_js(people, orgs)
    OUT_JS.write_text(js)

    shown = [p for p in people if not p.get("review")]
    edges = sum(
        1
        for i in range(len(shown))
        for j in range(i + 1, len(shown))
        if set(shown[i]["orgs"]) & set(shown[j]["orgs"])
    )
    print(f"\n✓ Wrote site/data.js")
    print(f"  {len(shown)} people  ·  {len(orgs)} orgs  ·  {edges} edges")


if __name__ == "__main__":
    main()
