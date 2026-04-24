# BoardGraph

An interactive network visualisation of biotech board interlocks — who sits on
whose boards, and how academic institutions, VC firms, and tech companies tie
the industry together.

---

## Folder structure

```
boardgraph/
├── data/
│   ├── people.json          ← EDIT THIS to add/update people & affiliations
│   ├── orgs.json            ← EDIT THIS to add/update organisations
│   └── raw/                 ← auto-generated; never edit manually
│       ├── sec/             ← DEF 14A proxy filing dumps per ticker
│       └── wikipedia/       ← scraped board/bio pages
├── scripts/
│   ├── requirements.txt
│   ├── scrape_sec.py        ← pulls board members from SEC EDGAR
│   ├── scrape_wikipedia.py  ← pulls affiliation data from Wikipedia
│   └── build_data.py        ← merges everything → site/data.js
└── site/
    ├── index.html           ← the website (never edit data in here)
    └── data.js              ← GENERATED — do not edit directly
```

---

## Quick start

```bash
# 1. Install dependencies
pip install -r scripts/requirements.txt

# 2. (Optional) scrape fresh data
python scripts/scrape_sec.py          # public companies via SEC EDGAR
python scripts/scrape_wikipedia.py --orgs --people

# 3. (Optional) fold scraped names into people.json as draft entries
python scripts/build_data.py --merge-raw
# → open data/people.json, review entries with "review": true, edit and remove the flag

# 4. Rebuild the website data file
python scripts/build_data.py

# 5. Serve the site locally
cd site && python -m http.server 8080
# → open http://localhost:8080
```

> **Why a local server?** `index.html` loads `data.js` via a `<script>` tag.
> Modern browsers block this on `file://` URLs. Python's built-in server works
> fine; alternatively use VS Code Live Server or any static host.

---

## Editing the data

### Adding a person

Open `data/people.json` and add an entry. All fields:

```json
{
  "id":          "Ada Lovelace",          // display name — must be unique
  "initials":    "AL",                    // shown inside the node circle
  "role":        "Mathematician, Analyst Engines Ltd",
  "category":    "academic",             // academic | executive | vc | regulatory
  "orgs":        ["Harvard", "OpenAI"],  // must match ids in orgs.json exactly
  "notes":       "Optional free text.",
  "last_updated": "2025-01-15",
  "sources":     ["https://..."]         // URL to proxy filing, press release, etc.
}
```

Then run `python scripts/build_data.py` to regenerate `site/data.js`.

### Adding an organisation

Open `data/orgs.json` and add an entry:

```json
{
  "id":        "Acme Therapeutics",     // display name — must be unique
  "type":      "biotech",              // biotech | pharma | tech | academia | vc
  "ticker":    "ACME",                 // null if private
  "cik":       "0001234567",           // SEC CIK — used by scrape_sec.py; null if N/A
  "wikipedia": "Acme_Therapeutics",    // Wikipedia page slug; null if none
  "notes":     ""
}
```

### Updating a board change

Find the person in `people.json`, edit their `"orgs"` array, update
`"last_updated"`, and add the source URL. Re-run `build_data.py`.

---

## Scraping workflow

### SEC EDGAR (`scrape_sec.py`)

Fetches the latest DEF 14A proxy filing for each public company in `orgs.json`
that has a `cik` field. Parses the HTML to extract director names and titles.

```bash
python scripts/scrape_sec.py                   # all public companies
python scripts/scrape_sec.py --ticker MRNA      # single company
python scripts/scrape_sec.py --dry-run          # list filing URLs without downloading
```

Output goes to `data/raw/sec/<ticker>.json`. These files are input for
`build_data.py --merge-raw`.

**Rate limits:** EDGAR allows ~10 requests/second. The script sleeps 120 ms
between requests by default.

### Wikipedia (`scrape_wikipedia.py`)

Fetches board/leadership sections from Wikipedia pages. Two modes:

```bash
python scripts/scrape_wikipedia.py --orgs    # scrape each org's Wikipedia page
python scripts/scrape_wikipedia.py --people  # search & scrape each person's page
```

Output goes to `data/raw/wikipedia/`. Useful for private companies and academic
institutions that don't file with the SEC.

### Merging scraped data

```bash
python scripts/build_data.py --merge-raw
```

This adds newly discovered names from `data/raw/` to `people.json` with
`"review": true`. Open the file and for each entry:

1. Verify the name is correct (scraped names can be noisy)
2. Set `"category"` appropriately
3. Add or correct `"orgs"`, `"role"`, `"notes"`
4. Remove the `"review": true` line

People with `"review": true` are **not** exported to `data.js` or shown in
the website until the flag is removed.

### Validating

```bash
python scripts/build_data.py --validate
```

Checks that every org referenced in `people.json` exists in `orgs.json`, and
that all required fields are present.

---

## Keeping data fresh

Proxy filings (DEF 14A) are filed annually, typically 3–4 months after fiscal
year end. A reasonable cadence:

| Frequency | Action |
|-----------|--------|
| Quarterly | Run `scrape_sec.py`, review `--merge-raw` output |
| As needed | Update `people.json` manually for notable board changes |
| Annually  | Run `scrape_wikipedia.py` to catch private company changes |

---

## Tech notes

- `site/index.html` is a self-contained static page. It loads `data.js` via
  `<script src="data.js">` which defines `window.PEOPLE_DATA` and
  `window.ORGS_DATA`.
- The graph layout uses D3 force simulation. Edges connect two people who share
  at least one org in their `orgs` array.
- Node size scales with connection count; node colour encodes role category.
