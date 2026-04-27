"""
pipeline.py
-----------
Works through data/companies_market_cap.csv company by company.
For each company:
  1. Finds board members via Wikidata, SEC filing, and/or Claude API extraction
  2. Shows a terminal approval UI — review, edit, or skip each person
  3. Expands each approved person's other affiliations
  4. Writes approved data to people.json and orgs.json
  5. Records progress so you can stop and resume at any time

Usage:
    cd boardgraph
    python scripts/pipeline.py                    # process next company in CSV
    python scripts/pipeline.py --company "Merck"  # jump to a specific company
    python scripts/pipeline.py --auto             # skip TUI, auto-approve all
    python scripts/pipeline.py --status           # show progress summary
    python scripts/pipeline.py --reset "Merck"    # mark a company as unprocessed

Requirements:
    pip install anthropic requests beautifulsoup4 lxml python-dotenv tqdm

    Set your API key in a .env file in the project root:
        ANTHROPIC_API_KEY=sk-ant-...

    Or set it for the current PowerShell session:
        $env:ANTHROPIC_API_KEY = "sk-ant-..."

    Or set it permanently on Windows:
        [System.Environment]::SetEnvironmentVariable("ANTHROPIC_API_KEY", "sk-ant-...", "User")
"""

import csv
import json
import os
import re
import sys
import time
import textwrap
from pathlib import Path
from typing import Optional

# Load .env file from the project root if present.
# Supports both ANTHROPIC_API_KEY and ANTHROPIC_API as the key name.
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_env_path)
    # Normalise: if .env uses ANTHROPIC_API, copy it to the expected name
    if not os.environ.get("ANTHROPIC_API_KEY") and os.environ.get("ANTHROPIC_API"):
        os.environ["ANTHROPIC_API_KEY"] = os.environ["ANTHROPIC_API"]
except ImportError:
    pass  # dotenv not installed — env var must be set manually

import requests
from bs4 import BeautifulSoup

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parent.parent
CSV_FILE      = ROOT / "data" / "companies_market_cap.csv"
PEOPLE_FILE   = ROOT / "data" / "people.json"
ORGS_FILE     = ROOT / "data" / "orgs.json"
PROGRESS_FILE = ROOT / "data" / "pipeline_progress.json"
RAW_WIKIDATA  = ROOT / "data" / "raw" / "wikidata"
RAW_WIKIDATA.mkdir(parents=True, exist_ok=True)

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
EDGAR_API     = "https://data.sec.gov"
EDGAR_ARCH    = "https://www.sec.gov"
SPARQL        = "https://query.wikidata.org/sparql"
WIKIDATA_API  = "https://www.wikidata.org/w/api.php"

HEADERS_SEC  = {"User-Agent": "BoardGraph Research contact@example.com"}
HEADERS_WIKI = {"User-Agent": "BoardGraph Research contact@example.com",
                "Accept": "application/sparql-results+json"}

RATE_SEC  = 0.15
RATE_WIKI = 1.0

# ── Colour helpers (no dependencies) ─────────────────────────────────────────
BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[32m"
CYAN  = "\033[36m"
YELLOW= "\033[33m"
RED   = "\033[31m"
RESET = "\033[0m"

def c(text, *codes): return "".join(codes) + str(text) + RESET
def hr(char="─", width=60): print(c(char * width, DIM))


# ── Progress tracking ─────────────────────────────────────────────────────────
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"done": [], "skipped": []}

def save_progress(prog: dict):
    PROGRESS_FILE.write_text(json.dumps(prog, indent=2))

def mark_done(prog: dict, company_name: str):
    if company_name not in prog["done"]:
        prog["done"].append(company_name)
    save_progress(prog)

def mark_skipped(prog: dict, company_name: str):
    if company_name not in prog["skipped"]:
        prog["skipped"].append(company_name)
    save_progress(prog)


# ── CSV loading ───────────────────────────────────────────────────────────────
def load_companies() -> list[dict]:
    companies = []
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            companies.append({
                "rank":   int(row["Rank"]),
                "name":   row["Name"].strip(),
                "ticker": row["Symbol"].strip(),
                "mcap":   int(float(row["marketcap"])),
                "country":row["country"].strip(),
            })
    return companies

def next_company(companies: list[dict], prog: dict) -> Optional[dict]:
    done = set(prog["done"]) | set(prog["skipped"])
    for co in companies:
        if co["name"] not in done:
            return co
    return None


# ── Data file helpers ─────────────────────────────────────────────────────────
def load_people() -> list[dict]:
    return json.loads(PEOPLE_FILE.read_text()) if PEOPLE_FILE.exists() else []

def load_orgs() -> list[dict]:
    return json.loads(ORGS_FILE.read_text()) if ORGS_FILE.exists() else []

def save_people(people: list[dict]):
    PEOPLE_FILE.write_text(json.dumps(people, indent=2))

def save_orgs(orgs: list[dict]):
    ORGS_FILE.write_text(json.dumps(orgs, indent=2))

def derive_initials(name: str) -> str:
    return "".join(w[0].upper() for w in name.split() if w)[:3]

def person_exists(people: list[dict], name: str) -> bool:
    return any(p["id"].lower() == name.lower() for p in people)

def org_exists(orgs: list[dict], name: str) -> bool:
    return any(o["id"].lower() == name.lower() for o in orgs)


# ── Claude API extractor ──────────────────────────────────────────────────────
EXTRACT_PROMPT = """\
Extract the board of directors from the document below.

Return ONLY a JSON object — no explanation, no preamble, no markdown.
Start your response with { and end with }

For each CURRENT board member return:
  name            (string, full name — strip all titles/honorifics such as
                   Dr., Prof., Prof. Dr., Mr., Ms., Gen., Sir, etc.)
  role            (string, e.g. "Independent Director", "Chair", "Vice-Chairman")
  age             (integer or null)
  director_since  (4-digit year string or null)
  other_boards    (list of other companies/orgs this person is affiliated with,
                   excluding the company being scraped)

Example:
{"directors":[{"name":"Akiko Iwasaki","role":"Independent Director","age":55,"director_since":"2023","other_boards":["Yale University","Howard Hughes Medical Institute"]}]}

If no board members are found return {"directors":[]}

COMPANY: {company}

DOCUMENT:
"""

def claude_extract(text: str, company_name: str) -> list[dict]:
    """
    Send page text to Claude API and ask it to extract board members as JSON.
    Returns a list of director dicts.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(c("  ⚠ ANTHROPIC_API_KEY not set — skipping Claude extraction", YELLOW))
        return []

    prompt = EXTRACT_PROMPT.replace("{company}", company_name) + text[:60000]

    payload = {
        "model":      "claude-sonnet-4-6",
        "max_tokens": 4096,
        "messages":   [{"role": "user", "content": prompt}],
    }
    headers = {
        "x-api-key":         api_key,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }

    try:
        resp = requests.post(ANTHROPIC_API, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()

        if not raw:
            print(c("  Claude returned empty response", YELLOW))
            return []

        # Extract the JSON object from wherever it appears in the response.
        # Handles cases where Claude adds a preamble before the JSON.
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not match:
            print(c(f"  No JSON found in response: {raw[:120]}", YELLOW))
            return []

        data = json.loads(match.group())
        directors = data.get("directors", [])
        return directors

    except json.JSONDecodeError as e:
        preview = raw[:200] if "raw" in dir() else "(no response)"
        print(c(f"  Claude JSON parse error: {e}", RED))
        print(c(f"  Response preview: {preview}", DIM))
        return []
    except Exception as e:
        print(c(f"  Claude API error: {e}", RED))
        return []


# ── Source: Wikidata ──────────────────────────────────────────────────────────
def wikidata_slug_to_qid(slug: str) -> Optional[str]:
    params = {"action": "wbgetentities", "sites": "enwiki",
              "titles": slug.replace("_", " "), "props": "info", "format": "json"}
    r = requests.get(WIKIDATA_API, params=params, headers=HEADERS_SEC, timeout=15)
    r.raise_for_status()
    time.sleep(RATE_WIKI)
    for qid, ent in r.json().get("entities", {}).items():
        if qid != "-1" and not ent.get("missing"):
            return qid
    return None

def wikidata_board_members(qid: str) -> list[dict]:
    query = f"""
    SELECT DISTINCT ?person ?personLabel ?startDate ?endDate WHERE {{
      wd:{qid} p:P3320 ?s .
      ?s ps:P3320 ?person .
      OPTIONAL {{ ?s pq:P580 ?startDate }}
      OPTIONAL {{ ?s pq:P582 ?endDate   }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
    }} ORDER BY ?personLabel
    """
    r = requests.get(SPARQL, params={"query": query, "format": "json"},
                     headers=HEADERS_WIKI, timeout=30)
    r.raise_for_status()
    time.sleep(RATE_WIKI)
    results = []
    for b in r.json().get("results", {}).get("bindings", []):
        name = b.get("personLabel", {}).get("value", "")
        qid2 = b.get("person",      {}).get("value", "").split("/")[-1]
        since= b.get("startDate",   {}).get("value", "")[:4]
        until= b.get("endDate",     {}).get("value", "")[:4]
        if name and not name.startswith("Q"):
            results.append({"name": name, "qid": qid2,
                            "director_since": since, "director_until": until,
                            "role": "Board member", "other_boards": []})
    return results

def wikidata_person_affiliations(person_qid: str) -> list[str]:
    """Return org names this person is affiliated with via employer/board/education."""
    query = f"""
    SELECT DISTINCT ?orgLabel WHERE {{
      {{
        wd:{person_qid} p:P3320 ?s . ?s ps:P3320 ?org .
      }} UNION {{
        wd:{person_qid} wdt:P108 ?org .
      }} UNION {{
        wd:{person_qid} wdt:P69  ?org .
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
    }}
    """
    try:
        r = requests.get(SPARQL, params={"query": query, "format": "json"},
                         headers=HEADERS_WIKI, timeout=20)
        r.raise_for_status()
        time.sleep(RATE_WIKI)
        return [b["orgLabel"]["value"] for b in
                r.json().get("results", {}).get("bindings", [])
                if b.get("orgLabel", {}).get("value", "")]
    except Exception:
        return []


# ── Source: SEC DEF 14A via Claude ────────────────────────────────────────────
def sec_fetch_def14a_text(ticker: str, cik: str) -> Optional[str]:
    """Fetch the latest DEF 14A filing and return plain text."""
    cik_padded = cik.lstrip("0").zfill(10)
    url  = f"{EDGAR_API}/submissions/CIK{cik_padded}.json"
    try:
        r = requests.get(url, headers=HEADERS_SEC, timeout=15)
        r.raise_for_status()
        time.sleep(RATE_SEC)
    except Exception as e:
        print(c(f"  SEC submissions error: {e}", RED))
        return None

    recent       = r.json().get("filings", {}).get("recent", {})
    forms        = recent.get("form", [])
    accnums      = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    for form, acc, doc in zip(forms, accnums, primary_docs):
        if form == "DEF 14A":
            acc_nd  = acc.replace("-", "")
            base    = f"{EDGAR_ARCH}/Archives/edgar/data/{int(cik_padded)}/{acc_nd}"
            doc_url = f"{base}/{doc}"
            try:
                r2 = requests.get(doc_url, headers=HEADERS_SEC, timeout=40)
                r2.raise_for_status()
                time.sleep(RATE_SEC)
                text = _filing_to_text(r2.text, doc)

                # iXBRL primary docs often parse poorly — try the filing index
                # for a plain .htm companion file if we got very little text
                if len(text.strip()) < 2000:
                    text = _try_plain_htm_from_index(base, text) or text

                # Proxy statements lead with shareholder/meeting boilerplate.
                # Seek forward to the director biography section so we don't
                # waste the context window before the names appear.
                text = _seek_director_section(text)

                print(c(f"  Filing text: {len(text)} chars", DIM))
                return text
            except Exception as e:
                print(c(f"  DEF 14A download error: {e}", RED))
                return None
    return None


def _filing_to_text(html: str, filename: str) -> str:
    """
    Convert a filing HTML/iXBRL document to clean plain text.
    Strips iXBRL namespace tags, scripts, styles, and boilerplate.
    """
    import warnings
    from bs4 import XMLParsedAsHTMLWarning
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

    soup = BeautifulSoup(html, "lxml")

    # Remove iXBRL hidden sections (contain raw XBRL facts, not readable text)
    for tag in soup.find_all(["ix:header", "ix:hidden"]):
        tag.decompose()

    # Unwrap iXBRL inline tags so their text content is preserved
    for tag in soup.find_all(re.compile(r"^ix:", re.I)):
        tag.unwrap()

    # Remove noise
    for tag in soup(["script", "style", "head"]):
        tag.decompose()

    text = soup.get_text(separator="\n")

    # Collapse runs of blank lines and leading whitespace per line
    lines = [l.strip() for l in text.splitlines()]
    lines = [l for l in lines if l]
    # Remove lines that are just numbers or single characters (XBRL artefacts)
    lines = [l for l in lines if len(l) > 3 or l.isalpha()]
    # No truncation here — _seek_director_section will take the right window
    return "\n".join(lines)


def _seek_director_section(text: str, window: int = 60000) -> str:
    """
    Find the start of the director biography section and return a window
    of text from that point. Falls back to the beginning if not found.

    Most proxy statements follow the pattern:
      [cover / ToC / proposals]  →  ELECTION OF DIRECTORS  →  [bios]
    """
    ANCHORS = [
        r"election of directors",
        r"nominees? for (?:election as )?directors?",
        r"information (?:about|regarding) (?:the )?(?:board|directors?|nominees?)",
        r"director nominees?",
        r"board of directors",
        r"proposal\s+(?:no\.?\s*)?1",          # Proposal 1 is almost always director election
    ]
    pattern = re.compile("|".join(ANCHORS), re.IGNORECASE)

    # Walk through matches and pick the first one that has actual bio content
    # nearby (indicated by "age" or "since" appearing within 3000 chars after).
    best = None
    for m in pattern.finditer(text):
        lookahead = text[m.start():m.start() + 3000].lower()
        if re.search(r"\bage\b|\bdirector since\b|\bsince \d{4}\b", lookahead):
            best = m.start()
            break

    if best is None:
        # No strong signal — fall back to start of document
        return text[:window]

    # Back up a little so we don't clip the heading itself
    start = max(0, best - 200)
    return text[start:start + window]


def _try_plain_htm_from_index(base_url: str, fallback: str) -> Optional[str]:
    """
    Fetch the filing index page and look for a companion .htm file
    that may be a more readable plain-HTML version of the proxy statement.
    """
    try:
        r = requests.get(base_url + "/", headers=HEADERS_SEC, timeout=15)
        r.raise_for_status()
        time.sleep(RATE_SEC)
        soup = BeautifulSoup(r.text, "lxml")
        links = [a["href"] for a in soup.find_all("a", href=True)
                 if a["href"].lower().endswith(".htm")]
        for href in links:
            full = href if href.startswith("http") else f"{EDGAR_ARCH}{href}"
            r2 = requests.get(full, headers=HEADERS_SEC, timeout=40)
            r2.raise_for_status()
            time.sleep(RATE_SEC)
            text = _filing_to_text(r2.text, href)
            if len(text.strip()) > 3000:
                print(c(f"  Using companion document: {href.split('/')[-1]}", DIM))
                return text
    except Exception:
        pass
    return None


# ── Source: IR page + Wikipedia fallback ─────────────────────────────────────
def fetch_ir_page(org_rec: Optional[dict]) -> Optional[str]:
    """Fetch a company's IR/leadership page if an ir_url is set in orgs.json."""
    if not org_rec:
        return None
    ir_url = org_rec.get("ir_url", "").strip()
    if not ir_url:
        return None
    try:
        print(f"  Trying IR page ({ir_url}) …")
        r = requests.get(ir_url, headers={**HEADERS_SEC,
            "User-Agent": "Mozilla/5.0 (compatible; BoardGraph/1.0)"
        }, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        return soup.get_text(separator="\n")[:20000]
    except Exception as e:
        print(c(f"  IR page error: {e}", DIM))
        return None


def web_search_board(company_name: str, org_rec: Optional[dict] = None) -> Optional[str]:
    """
    Try multiple Wikipedia slug variants to find board/governance content.
    Common patterns: 'Roche_Holding', 'F._Hoffmann-La_Roche', 'Roche_Holding_AG'
    """
    base = company_name.replace(" ", "_")
    slugs = [
        base,
        base + "_Holding",
        base + "_Holding_AG",
        base + "_AG",
        base + "_Inc.",
        base + ",_Inc.",
        base + "_plc",
        "F._Hoffmann-La_" + base,   # Roche edge case
    ]
    # Also try the wikipedia slug from orgs.json if it differs from company name
    if org_rec and org_rec.get("wikipedia"):
        wiki = org_rec["wikipedia"]
        if wiki not in slugs:
            slugs.insert(1, wiki)

    for slug in slugs:
        url = "https://en.wikipedia.org/wiki/" + slug
        try:
            r = requests.get(url, headers=HEADERS_SEC, timeout=15)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "lxml")
                for tag in soup(["script", "style", "sup"]):
                    tag.decompose()
                text = soup.get_text(separator="\n")
                # Only use if page has governance-relevant content
                if re.search(r"board of directors|chairman|supervisory board", text, re.I):
                    print(c(f"  Wikipedia: using slug '{slug}'", DIM))
                    return text[:20000]
        except Exception:
            continue
    return None


# ── Board finder: tries all sources ──────────────────────────────────────────
def find_board_members(company: dict, orgs: list[dict]) -> list[dict]:
    """
    Try Wikidata, then SEC+Claude, then Wikipedia+Claude.
    Returns a merged deduplicated list of director dicts.
    """
    name   = company["name"]
    ticker = company["ticker"]
    found  = []
    seen   = {}   # name_key → index in found

    # Common nickname → canonical first name mappings
    NICKNAMES = {
        "becky": "rebecca", "becca": "rebecca", "beck": "rebecca",
        "rick": "richard",  "rich": "richard",  "dick": "richard",
        "bob":  "robert",   "rob":  "robert",   "bobby": "robert",
        "bill": "william",  "will": "william",  "billy": "william",
        "jim":  "james",    "jimmy": "james",   "jamie": "james",
        "tom":  "thomas",   "tommy": "thomas",
        "mike": "michael",  "mick": "michael",
        "dave": "david",    "davy": "david",
        "steve": "stephen", "steven": "stephen",
        "kate": "katherine","kathy": "katherine","katie": "katherine",
        "liz":  "elizabeth","beth": "elizabeth", "betty": "elizabeth",
        "sue":  "susan",    "susie": "susan",
        "joe":  "joseph",   "joey": "joseph",
        "tony": "anthony",  "ant":  "anthony",
        "chris": "christopher",
        "dan":  "daniel",   "danny": "daniel",
        "ed":   "edward",   "eddie": "edward",  "ted": "edward",
        "fred": "frederick","freddy": "frederick",
        "ken":  "kenneth",  "kenny": "kenneth",
        "larry": "lawrence","lou": "louis",     "louie": "louis",
        "matt": "matthew",  "pat": "patricia",  "patty": "patricia",
        "ron":  "ronald",   "ronnie": "ronald",
        "sam":  "samuel",   "sandy": "sandra",
    }

    # Compound titles must come before single-word titles (longest match first)
    TITLE_RE = re.compile(
        r"^(?:prof\.?\s+dr\.?|prof\.?|dr\.?|mr\.?|ms\.?|mrs\.?|"
        r"sir|dame|gen\.?|lt\.?\s*gen\.?|rev\.?|hon\.?)\s+",
        re.IGNORECASE,
    )

    def normalize_unicode(s: str) -> str:
        import unicodedata
        return "".join(
            c for c in unicodedata.normalize("NFD", s)
            if unicodedata.category(c) != "Mn"
        )

    def name_key(raw: str) -> str:
        """
        Normalise a name for dedup:
        - strip honorifics/titles (Dr., Prof. Dr., Gen., etc.)
        - fold diacritics (Jörg → jorg, Schwan → schwan)
        - lowercase, strip punctuation
        - drop single-character middle initials
        - resolve nicknames to canonical first names
        """
        n = raw.strip()
        for _ in range(3):          # handle "Dr. Dr." edge cases
            n2 = TITLE_RE.sub("", n).strip()
            if n2 == n:
                break
            n = n2
        n = normalize_unicode(n)
        n = re.sub(r"[.,]", "", n).lower()
        parts = [p for p in n.split() if len(p) > 1]
        if parts:
            parts[0] = NICKNAMES.get(parts[0], parts[0])
        return " ".join(parts)

    def add(d: dict):
        raw = d.get("name", "").strip()
        if not raw:
            return
        key = name_key(raw)
        if key in seen:
            existing = found[seen[key]]
            if len(d.get("other_boards", [])) > len(existing.get("other_boards", [])):
                existing["other_boards"] = d["other_boards"]
            if d.get("role") and not existing.get("role"):
                existing["role"] = d["role"]
            if d.get("age") and not existing.get("age"):
                existing["age"] = d["age"]
            if len(raw) > len(existing["name"]):
                existing["name"] = raw
        else:
            seen[key] = len(found)
            found.append(dict(d))

    # ── Try matching org in orgs.json for CIK / Wikipedia slug ───────────────
    org_rec = next((o for o in orgs if o["id"].lower() == name.lower()), None)

    # ── 1. Wikidata ───────────────────────────────────────────────────────────
    wiki_slug = org_rec.get("wikipedia") if org_rec else name.replace(" ", "_")
    if wiki_slug:
        print(f"  Trying Wikidata ({wiki_slug}) …")
        try:
            qid = wikidata_slug_to_qid(wiki_slug)
            if qid:
                members = wikidata_board_members(qid)
                # Filter out former members — those with a director_until year set
                current_year = int(time.strftime("%Y"))
                members = [
                    m for m in members
                    if not m.get("director_until") or
                    int(m["director_until"]) >= current_year
                ]
                if members:
                    print(c(f"  ✓ Wikidata: {len(members)} current members", GREEN))
                    for m in members:
                        add(m)
                else:
                    print(c("  Wikidata: no current P3320 board data", DIM))
            else:
                print(c("  Wikidata: QID not found", DIM))
        except Exception as e:
            print(c(f"  Wikidata error: {e}", RED))

    # ── 2. SEC DEF 14A → Claude ───────────────────────────────────────────────
    # Always run SEC — Wikidata lags on recent appointments so the two sources
    # complement each other. New names from SEC are merged in if not already seen.
    cik = org_rec.get("cik") if org_rec else None
    if cik:
        print(f"  Trying SEC DEF 14A …")
        text = sec_fetch_def14a_text(ticker, cik)
        if text:
            members = claude_extract(text, name)
            if members:
                before = len(found)
                for m in members:
                    if m.get("name"):
                        add({**m, "source": "sec_claude"})
                added = len(found) - before
                print(c(f"  ✓ SEC+Claude: {len(members)} members ({added} new vs Wikidata)", GREEN))
            else:
                print(c("  SEC+Claude: no members found", DIM))
    else:
        print(c("  SEC: no CIK — skipping", DIM))

    # ── 3. IR page → Claude ───────────────────────────────────────────────────
    ir_text = fetch_ir_page(org_rec)
    if ir_text:
        members = claude_extract(ir_text, name)
        if members:
            before = len(found)
            for m in members:
                if m.get("name"):
                    add({**m, "source": "ir_claude"})
            print(c(f"  ✓ IR page+Claude: {len(members)} members ({len(found)-before} new)", GREEN))
        else:
            print(c("  IR page+Claude: no members found", DIM))

    # ── 4. Wikipedia → Claude fallback ───────────────────────────────────────
    if not found:
        print(f"  Trying Wikipedia …")
        text = web_search_board(name, org_rec)
        if text:
            members = claude_extract(text, name)
            if members:
                print(c(f"  ✓ Wikipedia+Claude: {len(members)} members", GREEN))
                for m in members:
                    if m.get("name"):
                        add({**m, "source": "wikipedia_claude"})
            else:
                print(c("  Wikipedia+Claude: no members found", DIM))

    return found


# ── Manual entry fallback ─────────────────────────────────────────────────────
def tui_manual_entry(company: dict) -> list[dict]:
    """
    When automated sources find nothing, offer the user a chance to paste
    board members in manually. One person per line:
      Name | Role | OtherOrg1, OtherOrg2
    Blank line or 'done' to finish. 'skip' to skip the company entirely.
    """
    hr()
    print(c("  Manual entry mode", BOLD))
    print(c("  Format: Name  |  Role  |  Org1, Org2  (role and orgs optional)", DIM))
    print(c("  Examples:", DIM))
    print(c("    Severin Schwan | Chair | Roche Holding", DIM))
    print(c("    Akiko Iwasaki | Independent Director | Yale University", DIM))
    print(c("  Type 'done' or leave blank to finish. 'skip' to skip company.", DIM))
    hr()

    entries = []
    while True:
        try:
            line = input(c("  > ", YELLOW)).strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line or line.lower() == "done":
            break
        if line.lower() == "skip":
            return []

        parts = [p.strip() for p in line.split("|")]
        name  = parts[0] if len(parts) > 0 else ""
        role  = parts[1] if len(parts) > 1 else "Board member"
        orgs  = [o.strip() for o in parts[2].split(",")] if len(parts) > 2 else []

        if not name:
            continue
        entries.append({
            "name":        name,
            "role":        role,
            "other_boards": orgs,
            "source":      "manual",
        })
        print(c(f"  + {name}", GREEN))

    return entries


# ── Terminal approval UI ──────────────────────────────────────────────────────
def format_mcap(n: int) -> str:
    if n >= 1e12: return f"${n/1e12:.1f}T"
    if n >= 1e9:  return f"${n/1e9:.1f}B"
    return f"${n/1e6:.0f}M"

def tui_approve(company: dict, candidates: list[dict], auto: bool = False) -> list[dict]:
    """
    Show each candidate director and ask the user to approve, skip, or edit.
    Returns the approved list.
    """
    if not candidates:
        print(c("  No candidates to review.", YELLOW))
        return []

    hr()
    print(c(f"  {len(candidates)} people found for {company['name']}", BOLD))
    print(c("  Commands: y=approve  n=skip  e=edit  a=approve all  q=quit company", DIM))
    hr()

    approved = []

    for i, person in enumerate(candidates):
        name   = person.get("name", "").strip()
        role   = person.get("role", "")
        since  = person.get("director_since", "")
        others = person.get("other_boards", [])
        age    = person.get("age")

        meta = []
        if age:    meta.append(f"age {age}")
        if since:  meta.append(f"since {since}")
        meta_str = "  " + "  ·  ".join(meta) if meta else ""

        print(f"\n  {c(i+1, BOLD)}/{len(candidates)}  {c(name, CYAN, BOLD)}")
        print(f"  {c(role or '—', DIM)}{meta_str}")
        if others:
            wrapped = textwrap.fill(", ".join(others[:8]), width=56,
                                    initial_indent="  Other: ",
                                    subsequent_indent="         ")
            print(c(wrapped, DIM))

        if auto:
            approved.append(person)
            print(c("  → auto-approved", GREEN))
            continue

        while True:
            try:
                cmd = input(c("  [y/n/e/a/q] ", YELLOW)).strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                return approved

            if cmd == "y":
                approved.append(person)
                print(c("  ✓ approved", GREEN))
                break
            elif cmd == "n":
                print(c("  ✗ skipped", DIM))
                break
            elif cmd == "a":
                approved.append(person)
                approved.extend(candidates[i+1:])
                print(c(f"  ✓ approved all remaining ({len(candidates)-i} people)", GREEN))
                return approved
            elif cmd == "q":
                print(c("  Stopping review for this company.", YELLOW))
                return approved
            elif cmd == "e":
                try:
                    new_name = input(f"  Name [{name}]: ").strip() or name
                    new_role = input(f"  Role [{role}]: ").strip() or role
                    raw_orgs = input(f"  Other orgs (comma-separated) [{', '.join(others)}]: ").strip()
                    new_orgs = [o.strip() for o in raw_orgs.split(",")] if raw_orgs else others
                    person = {**person, "name": new_name, "role": new_role, "other_boards": new_orgs}
                    approved.append(person)
                    print(c("  ✓ edited & approved", GREEN))
                    break
                except (EOFError, KeyboardInterrupt):
                    print()
                    return approved
            else:
                print(c("  Type y, n, e, a, or q", DIM))

    return approved


# ── Affiliation expansion ─────────────────────────────────────────────────────
def expand_affiliations(approved: list[dict], orgs: list[dict]) -> list[str]:
    """
    Collect all unique org names mentioned in approved people's other_boards,
    plus optionally query Wikidata for more.
    Returns new org IDs to consider adding.
    """
    org_ids    = {o["id"].lower() for o in orgs}
    candidates = set()

    for person in approved:
        for org_name in person.get("other_boards", []):
            if org_name and org_name.lower() not in org_ids:
                candidates.add(org_name)

    return sorted(candidates)


def tui_add_orgs(new_org_names: list[str], orgs: list[dict], auto: bool) -> list[dict]:
    """Ask user which new orgs to add to orgs.json."""
    if not new_org_names:
        return orgs

    print(f"\n  {c(len(new_org_names), BOLD)} new organisations found in affiliations:")
    for name in new_org_names[:30]:
        print(f"    · {name}")
    if len(new_org_names) > 30:
        print(f"    … and {len(new_org_names)-30} more")

    if auto:
        add_all = True
    else:
        try:
            ans = input(c("\n  Add all as new orgs? [y/n/s=select] ", YELLOW)).strip().lower()
        except (EOFError, KeyboardInterrupt):
            return orgs
        add_all = ans == "y"
        select  = ans == "s"

    existing_ids = {o["id"].lower() for o in orgs}
    added = 0
    for name in new_org_names:
        if name.lower() in existing_ids:
            continue
        if auto or add_all:
            do_add = True
        elif select:
            try:
                ans = input(f"  Add '{name}'? [y/n] ").strip().lower()
                do_add = ans == "y"
            except (EOFError, KeyboardInterrupt):
                break
        else:
            continue

        if do_add:
            org_type = guess_org_type(name)
            orgs.append({
                "id":        name,
                "type":      org_type,
                "ticker":    None,
                "cik":       None,
                "wikipedia": None,
                "notes":     "auto-added by pipeline",
            })
            existing_ids.add(name.lower())
            added += 1

    if added:
        print(c(f"  Added {added} new orgs", GREEN))
    return orgs


def guess_org_type(name: str) -> str:
    n = name.lower()
    if any(w in n for w in ["university", "college", "institute", "school",
                             "mit", "harvard", "stanford", "academy", "hhmi"]):
        return "academia"
    if any(w in n for w in ["capital", "ventures", "partners", "fund",
                             "equity", "investment", "asset", "holdings"]):
        return "vc"
    if any(w in n for w in ["pharma", "therapeutics", "medicines", "biosciences",
                             "biotech", "genomics", "oncology", "biologics"]):
        return "biotech"
    if any(w in n for w in ["lilly", "pfizer", "merck", "novartis", "roche",
                             "sanofi", "abbvie", "bristol", "astrazeneca", "gsk",
                             "bayer", "novo nordisk", "johnson", "boehringer",
                             "takeda", "eisai", "chugai", "genentech"]):
        return "pharma"
    if any(w in n for w in ["google", "microsoft", "apple", "amazon", "meta",
                             "nvidia", "intel", "ibm", "oracle", "salesforce"]):
        return "tech"
    return "other"


# ── Write approved people ─────────────────────────────────────────────────────
def write_approved_people(
    approved: list[dict],
    company_name: str,
    people: list[dict],
    orgs: list[dict],
) -> tuple[list[dict], int]:
    """
    Add approved directors to people.json.
    Returns (updated_people, count_added).
    """
    org_ids  = {o["id"] for o in orgs}
    # Build a normalised lookup of existing people
    def _nkey(n):
        n = re.sub(r"[.,]", "", n).strip().lower()
        parts = [p for p in n.split() if len(p) > 1]
        return " ".join(parts)

    existing = {_nkey(p["id"]): p for p in people}
    added    = 0

    for d in approved:
        name = d.get("name", "").strip()
        if not name:
            continue
        key = _nkey(name)

        if key in existing:
            # Person already in people.json — enrich rather than duplicate
            p = existing[key]
            if company_name not in p.get("orgs", []):
                p.setdefault("orgs", []).append(company_name)
            # Upgrade to longer name if new one has middle initial
            if len(name) > len(p["id"]):
                p["id"] = name
                p["initials"] = derive_initials(name)
            continue

        other_boards = [o for o in d.get("other_boards", []) if o in org_ids]
        orgs_list    = list({company_name} | set(other_boards))

        people.append({
            "id":             name,
            "initials":       derive_initials(name),
            "role":           d.get("role", ""),
            "category":       "executive",
            "orgs":           orgs_list,
            "notes":          "",
            "last_updated":   time.strftime("%Y-%m-%d"),
            "sources":        [d.get("source", "pipeline")],
            "director_since": d.get("director_since") or "",
            "director_until": d.get("director_until", "") or "",
        })
        existing[key] = people[-1]
        added += 1

    return people, added


# ── Status display ────────────────────────────────────────────────────────────
def show_status(companies: list[dict], prog: dict):
    done    = set(prog["done"])
    skipped = set(prog["skipped"])
    total   = len(companies)
    n_done  = len(done)
    n_skip  = len(skipped)
    n_left  = total - n_done - n_skip

    print(f"\n{c('Pipeline status', BOLD)}")
    hr()
    print(f"  Total companies : {total}")
    print(f"  Processed       : {c(n_done, GREEN)}")
    print(f"  Skipped         : {c(n_skip, YELLOW)}")
    print(f"  Remaining       : {c(n_left, CYAN)}")

    nxt = next_company(companies, prog)
    if nxt:
        print(f"\n  Next: {c(nxt['name'], BOLD)} ({nxt['ticker']})  "
              f"{format_mcap(nxt['mcap'])}  {nxt['country']}")

    # Show last 5 done
    if done:
        recent = [c for c in companies if c["name"] in done][-5:]
        print(f"\n  Recently processed:")
        for co in recent:
            print(f"    {c('✓', GREEN)} {co['name']}")
    print()


# ── CIK lookup ───────────────────────────────────────────────────────────────
# EDGAR publishes a full ticker→CIK map. We cache it in memory for the session.
_CIK_CACHE: dict[str, str] = {}

def lookup_cik(ticker: str, company_name: str) -> Optional[str]:
    """
    Look up a company's SEC CIK by ticker symbol using EDGAR's company
    tickers endpoint. Falls back to a name search if the ticker isn't found.
    """
    global _CIK_CACHE

    # Populate cache on first call
    if not _CIK_CACHE:
        try:
            r = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers=HEADERS_SEC, timeout=15,
            )
            r.raise_for_status()
            for entry in r.json().values():
                t = entry.get("ticker", "").upper()
                c_ = str(entry.get("cik_str", "")).zfill(10)
                if t:
                    _CIK_CACHE[t] = c_
            time.sleep(RATE_SEC)
        except Exception as e:
            print(c(f"  CIK cache load error: {e}", RED))

    # Try ticker lookup first
    cik = _CIK_CACHE.get(ticker.upper())
    if cik:
        return cik

    # Fall back to EDGAR company name search
    try:
        r = requests.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={"q": f'"{company_name}"', "forms": "DEF+14A", "dateRange": "custom",
                    "startdt": "2023-01-01"},
            headers=HEADERS_SEC, timeout=15,
        )
        r.raise_for_status()
        time.sleep(RATE_SEC)
        hits = r.json().get("hits", {}).get("hits", [])
        if hits:
            entity = hits[0].get("_source", {}).get("entity_id", "")
            if entity:
                return str(entity).zfill(10)
    except Exception:
        pass

    return None


# ── Ensure company in orgs.json ───────────────────────────────────────────────
def ensure_org(company: dict, orgs: list[dict]) -> list[dict]:
    if not org_exists(orgs, company["name"]):
        org_type = guess_org_type(company["name"])
        # Try to guess type from sector — most in this CSV are pharma/biotech
        if any(w in company["name"].lower() for w in [
            "pharma", "novartis", "roche", "lilly", "pfizer", "merck", "sanofi",
            "abbvie", "bristol", "astrazeneca", "gsk", "bayer", "novo", "johnson",
            "boehringer", "takeda", "eisai", "otsuka", "daiichi", "chugai",
            "biogen", "genentech", "servier", "ipsen", "ucb", "almirall",
        ]):
            org_type = "pharma"
        elif any(w in company["name"].lower() for w in
                 ["biotech","therapeutics","biosciences","genomics","amgen",
                  "regeneron","gilead","vertex","biogen","moderna","alnylam"]):
            org_type = "biotech"
        # Auto-lookup the CIK from EDGAR so SEC scraping works immediately
        cik = lookup_cik(company["ticker"], company["name"])
        if cik:
            print(c(f"  Resolved CIK: {cik}", CYAN))
        orgs.append({
            "id":        company["name"],
            "type":      org_type,
            "ticker":    company["ticker"],
            "cik":       cik,
            "wikipedia": company["name"].replace(" ", "_"),
            "notes":     f"market cap {format_mcap(company['mcap'])}",
        })
        print(c(f"  Added '{company['name']}' to orgs.json", CYAN))
    return orgs


# ── Main ──────────────────────────────────────────────────────────────────────
def process_company(company: dict, auto: bool = False):
    people = load_people()
    orgs   = load_orgs()
    prog   = load_progress()

    hr("═")
    rank   = company["rank"]
    name   = company["name"]
    ticker = company["ticker"]
    mcap   = format_mcap(company["mcap"])
    print(c(f"  #{rank}  {name}  ({ticker})  {mcap}  {company['country']}", BOLD))
    hr("═")

    # Make sure the company is in orgs.json
    orgs = ensure_org(company, orgs)
    save_orgs(orgs)

    # Find board members
    print(f"\n{c('Finding board members …', BOLD)}\n")
    candidates = find_board_members(company, orgs)

    if not candidates:
        print(c("\n  No board members found from any source.", RED))
        candidates = tui_manual_entry(company)
        if not candidates:
            try:
                ans = input(c("  Mark as skipped? [y/n] ", YELLOW)).strip().lower()
            except (EOFError, KeyboardInterrupt):
                ans = "n"
            if ans == "y":
                mark_skipped(prog, name)
            return

    # TUI approval
    print(f"\n{c('Review board members', BOLD)}\n")
    approved = tui_approve(company, candidates, auto=auto)

    if not approved:
        print(c("\n  No people approved.", YELLOW))
        mark_skipped(prog, name)
        return

    # Collect and optionally add new orgs from affiliations
    new_orgs = expand_affiliations(approved, orgs)
    if new_orgs:
        orgs = tui_add_orgs(new_orgs, orgs, auto=auto)
        save_orgs(orgs)

    # Write approved people
    people, n_added = write_approved_people(approved, name, people, orgs)
    save_people(people)
    mark_done(prog, name)

    print(f"\n{c('Done', GREEN, BOLD)} — {n_added} new people added for {name}")
    print(c(f"  {len(people)} total people  ·  {len(orgs)} total orgs", DIM))


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline: process companies from CSV")
    parser.add_argument("--company", help="Jump to a specific company by name")
    parser.add_argument("--auto",    action="store_true",
                        help="Auto-approve all, skip TUI")
    parser.add_argument("--status",  action="store_true",
                        help="Show progress and exit")
    parser.add_argument("--reset",   metavar="COMPANY",
                        help="Mark a company as unprocessed")
    parser.add_argument("--loop",    action="store_true",
                        help="Keep processing companies until interrupted")
    args = parser.parse_args()

    companies = load_companies()
    prog      = load_progress()

    if args.status:
        show_status(companies, prog)
        return

    if args.reset:
        prog["done"]    = [d for d in prog["done"]    if d != args.reset]
        prog["skipped"] = [s for s in prog["skipped"] if s != args.reset]
        save_progress(prog)
        print(c(f"Reset '{args.reset}'", GREEN))
        return

    if args.company:
        company = next((c for c in companies if c["name"].lower() == args.company.lower()), None)
        if not company:
            print(c(f"Company '{args.company}' not found in CSV", RED))
            return
        process_company(company, auto=args.auto)
        return

    if args.loop:
        while True:
            company = next_company(companies, prog)
            if not company:
                print(c("All companies processed!", GREEN))
                break
            process_company(company, auto=args.auto)
            prog = load_progress()
            try:
                input(c("\nPress Enter for next company, Ctrl+C to stop …", DIM))
            except (EOFError, KeyboardInterrupt):
                print()
                break
        return

    # Default: process one company
    company = next_company(companies, prog)
    if not company:
        print(c("All companies processed!", GREEN))
        return
    process_company(company, auto=args.auto)


if __name__ == "__main__":
    main()
