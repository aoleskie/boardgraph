"""
scrape_wikidata.py
------------------
Fetches board members for each org in data/orgs.json using the Wikidata
SPARQL endpoint. Wikidata stores board membership as structured triples,
so results are clean with no HTML parsing required.

Output: data/raw/wikidata/<org_id>.json  (one file per org)

Usage:
    cd boardgraph
    python scripts/scrape_wikidata.py               # all orgs with wikipedia slugs
    python scripts/scrape_wikidata.py --org Moderna  # single org by id
    python scripts/scrape_wikidata.py --dry-run      # print QIDs without querying
"""

import json
import time
import argparse
from pathlib import Path
from typing import Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
ORGS_FILE   = ROOT / "data" / "orgs.json"
RAW_WIKIDATA = ROOT / "data" / "raw" / "wikidata"
RAW_WIKIDATA.mkdir(parents=True, exist_ok=True)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_API    = "https://www.wikidata.org/w/api.php"

HEADERS = {
    "User-Agent": "BoardGraph Research contact@example.com",
    "Accept": "application/sparql-results+json",
}

RATE_LIMIT_SECS = 1.0  # Wikidata asks for politeness


# ── QID lookup ────────────────────────────────────────────────────────────────
def wikipedia_slug_to_qid(slug: str) -> Optional[str]:
    """
    Convert a Wikipedia article slug to a Wikidata QID using the sitelinks API.
    e.g. "Moderna" → "Q84870533"
    """
    params = {
        "action":   "wbgetentities",
        "sites":    "enwiki",
        "titles":   slug.replace("_", " "),
        "props":    "info",
        "format":   "json",
    }
    resp = requests.get(WIKIDATA_API, params=params,
                        headers={"User-Agent": HEADERS["User-Agent"]}, timeout=15)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT_SECS)

    entities = resp.json().get("entities", {})
    for qid, entity in entities.items():
        if qid != "-1" and not entity.get("missing"):
            return qid
    return None


# ── SPARQL queries ────────────────────────────────────────────────────────────
def query_board_members(qid: str) -> list[dict]:
    """
    Query Wikidata for all board members of an organisation (P3320),
    including start/end dates and other roles (CEO, chair, etc.).

    P3320  = board member
    P580   = start time (qualifier)
    P582   = end time (qualifier)
    P39    = position held (for CEO/chair cross-check)
    P21    = sex or gender
    P106   = occupation
    """
    query = f"""
    SELECT DISTINCT
      ?person ?personLabel
      ?startDate ?endDate
      ?occupationLabel
    WHERE {{
      wd:{qid} p:P3320 ?boardStmt .
      ?boardStmt ps:P3320 ?person .
      OPTIONAL {{ ?boardStmt pq:P580 ?startDate }}
      OPTIONAL {{ ?boardStmt pq:P582 ?endDate   }}
      OPTIONAL {{ ?person wdt:P106 ?occupation  }}
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en" .
      }}
    }}
    ORDER BY ?personLabel
    """
    return _run_sparql(query)


def query_executives(qid: str) -> list[dict]:
    """
    Query for people who held executive positions (CEO, chair, president, CFO)
    at this organisation via P488 (chairperson) and P169 (chief executive).
    Supplements board member data for organisations that store execs separately.
    """
    query = f"""
    SELECT DISTINCT
      ?person ?personLabel ?roleLabel ?startDate ?endDate
    WHERE {{
      {{
        wd:{qid} p:P488 ?stmt .        # chairperson
        ?stmt ps:P488 ?person .
        BIND("Chair" AS ?role)
        OPTIONAL {{ ?stmt pq:P580 ?startDate }}
        OPTIONAL {{ ?stmt pq:P582 ?endDate   }}
      }} UNION {{
        wd:{qid} p:P169 ?stmt .        # chief executive officer
        ?stmt ps:P169 ?person .
        BIND("CEO" AS ?role)
        OPTIONAL {{ ?stmt pq:P580 ?startDate }}
        OPTIONAL {{ ?stmt pq:P582 ?endDate   }}
      }} UNION {{
        wd:{qid} p:P1308 ?stmt .       # officeholder (general)
        ?stmt ps:P1308 ?person .
        OPTIONAL {{ ?stmt pq:P580 ?startDate }}
        OPTIONAL {{ ?stmt pq:P582 ?endDate   }}
        OPTIONAL {{ ?stmt pq:P768 ?role      }}
      }}
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en" .
      }}
    }}
    ORDER BY ?personLabel
    """
    return _run_sparql(query)


def query_person_details(person_qid: str) -> dict:
    """
    Fetch additional details for a person QID:
    employer history, education, positions held.
    Used to enrich the orgs list for a person.
    """
    query = f"""
    SELECT DISTINCT ?employerLabel ?eduLabel ?posLabel WHERE {{
      OPTIONAL {{ wd:{person_qid} wdt:P108 ?employer }}
      OPTIONAL {{ wd:{person_qid} wdt:P69  ?edu      }}
      OPTIONAL {{ wd:{person_qid} p:P39 ?posStmt .
                  ?posStmt ps:P39 ?pos }}
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en" .
      }}
    }}
    """
    rows = _run_sparql(query)
    employers  = list({r["employerLabel"] for r in rows if r.get("employerLabel")})
    educations = list({r["eduLabel"]      for r in rows if r.get("eduLabel")})
    positions  = list({r["posLabel"]      for r in rows if r.get("posLabel")})
    return {"employers": employers, "education": educations, "positions": positions}


def _run_sparql(query: str) -> list[dict]:
    """Execute a SPARQL query and return a flat list of row dicts."""
    resp = requests.get(
        SPARQL_ENDPOINT,
        params={"query": query, "format": "json"},
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    time.sleep(RATE_LIMIT_SECS)

    bindings = resp.json().get("results", {}).get("bindings", [])
    rows = []
    for b in bindings:
        row = {}
        for key, val in b.items():
            # Strip Wikidata URI prefix from QID values
            v = val.get("value", "")
            if val.get("type") == "uri" and "entity/Q" in v:
                v = v.split("/entity/")[-1]
            row[key] = v
        rows.append(row)
    return rows


# ── Result shaping ────────────────────────────────────────────────────────────
def _format_date(iso: str) -> str:
    """Trim Wikidata ISO dates like '2015-01-01T00:00:00Z' → '2015'."""
    return iso[:4] if iso else ""


def shape_results(
    org_id: str,
    qid: str,
    board_rows: list[dict],
    exec_rows:  list[dict],
) -> dict:
    """
    Merge board + exec results into a clean list of director dicts.
    Each entry has: name, qid, director_since, director_until, role, source.
    """
    seen: dict[str, dict] = {}

    def add(name: str, person_qid: str, since: str, until: str, role: str, src: str):
        if not name or name.lower() in ("unknown", ""):
            return
        # Skip if name looks like a QID or URL
        if name.startswith("Q") and name[1:].isdigit():
            return
        key = name.lower()
        if key not in seen:
            seen[key] = {
                "name":            name,
                "qid":             person_qid,
                "director_since":  since,
                "director_until":  until,
                "role":            role,
                "company":         org_id,
                "source":          src,
            }
        else:
            # Enrich existing entry with any new date info
            if since and not seen[key]["director_since"]:
                seen[key]["director_since"] = since
            if until and not seen[key]["director_until"]:
                seen[key]["director_until"] = until

    for r in board_rows:
        add(
            name=r.get("personLabel", ""),
            person_qid=r.get("person", ""),
            since=_format_date(r.get("startDate", "")),
            until=_format_date(r.get("endDate", "")),
            role="Board member",
            src="wikidata_P3320",
        )

    for r in exec_rows:
        add(
            name=r.get("personLabel", ""),
            person_qid=r.get("person", ""),
            since=_format_date(r.get("startDate", "")),
            until=_format_date(r.get("endDate", "")),
            role=r.get("roleLabel", "Executive"),
            src="wikidata_exec",
        )

    return {
        "org_id":    org_id,
        "qid":       qid,
        "directors": list(seen.values()),
    }


# ── Per-org scrape ────────────────────────────────────────────────────────────
def scrape_org(org: dict, dry_run: bool = False) -> Optional[dict]:
    org_id = org["id"]
    slug   = org.get("wikipedia")

    if not slug:
        print(f"  [{org_id}] skipped — no wikipedia slug")
        return None

    out_path = RAW_WIKIDATA / f"{org_id.replace('/', '_').replace(' ', '_')}.json"

    # Resolve Wikipedia slug → Wikidata QID
    print(f"  [{org_id}] resolving QID from slug '{slug}' …")
    try:
        qid = wikipedia_slug_to_qid(slug)
    except Exception as e:
        print(f"  [{org_id}] ERROR resolving QID: {e}")
        return None

    if not qid:
        print(f"  [{org_id}] QID not found for slug '{slug}'")
        return None

    print(f"  [{org_id}] QID = {qid}")

    if dry_run:
        return {"org_id": org_id, "qid": qid}

    # Query board members
    try:
        board_rows = query_board_members(qid)
        print(f"  [{org_id}] P3320 board members: {len(board_rows)}")
    except Exception as e:
        print(f"  [{org_id}] ERROR querying board members: {e}")
        board_rows = []

    # Query executives (CEO, chair) — supplements sparse board data
    try:
        exec_rows = query_executives(qid)
        print(f"  [{org_id}] exec roles: {len(exec_rows)}")
    except Exception as e:
        print(f"  [{org_id}] ERROR querying executives: {e}")
        exec_rows = []

    result = shape_results(org_id, qid, board_rows, exec_rows)
    n = len(result["directors"])

    if n == 0:
        print(f"  [{org_id}] no directors found")
    else:
        print(f"  [{org_id}] {n} director(s) total")
        for d in result["directors"][:5]:
            since = f" (since {d['director_since']})" if d["director_since"] else ""
            print(f"    · {d['name']}{since}")
        if n > 5:
            print(f"    … and {n - 5} more")

    out_path.write_text(json.dumps(result, indent=2))
    print(f"  [{org_id}] saved → {out_path.relative_to(ROOT)}")
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Scrape Wikidata board member data for orgs in orgs.json"
    )
    parser.add_argument("--org",      help="Scrape only this org id (e.g. 'Moderna')")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Resolve QIDs only, no SPARQL queries")
    parser.add_argument("--type",     help="Only scrape orgs of this type (e.g. biotech)")
    args = parser.parse_args()

    with open(ORGS_FILE) as f:
        orgs = json.load(f)

    if args.org:
        orgs = [o for o in orgs if o["id"].lower() == args.org.lower()]
        if not orgs:
            print(f"Org '{args.org}' not found in orgs.json")
            return

    if args.type:
        orgs = [o for o in orgs if o.get("type") == args.type]

    orgs = [o for o in orgs if o.get("wikipedia")]
    print(f"Scraping {len(orgs)} orgs from Wikidata …\n")

    total_directors = 0
    for org in orgs:
        result = scrape_org(org, dry_run=args.dry_run)
        if result:
            total_directors += len(result.get("directors", []))
        print()

    print(f"Done. {total_directors} director entries collected across {len(orgs)} orgs.")


if __name__ == "__main__":
    main()
