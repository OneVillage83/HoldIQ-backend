#!/usr/bin/env python3
"""
HoldIQ Unified Ingestion
------------------------
Loads one or more EDGAR CSV exports (raw or enriched) into a single normalized
SQLite table: filings

Features
- Idempotent upsert using a robust uniq key:
    * If accessionNo is present -> uniq = "ACC:" + accessionNo (normalized nodash)
    * Else uniq = "ROW:" + sha1(company|formType|filedAt|filingUrl)
- Adds/derives year (from filedAt) if not present
- Accepts arbitrary headers; maps common fields; stores extras in a JSON column
- Builds helpful indexes

Usage
  py holdiq_ingest.py --db .\data\holdiq.db --csv ".\data\holdiq_2024.csv"
  py holdiq_ingest.py --db .\data\holdiq.db --csv ".\data\holdiq_2024_tiers_enriched.csv"
  py holdiq_ingest.py --db .\data\holdiq.db --csv ".\data\holdiq_2023_tiers_enriched.csv" ".\data\holdiq_2024_tiers_enriched.csv"

After ingest:
  - SELECT COUNT(*) FROM filings;
  - SELECT year, formType, COUNT(*) FROM filings GROUP BY year, formType ORDER BY year, COUNT(*) DESC LIMIT 50;
"""

import csv
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

RE_ACC = re.compile(r"^0*(\d{10})(\d{2})(\d{6})$")  # normalize nodash form if needed

# columns we normalize into the master table
CORE_COLS = [
    "cik", "ticker", "company", "formType", "filedAt", "reportPeriod",
    "accessionNo", "primaryDocument", "filingUrl", "size"
]

DDL = """
CREATE TABLE IF NOT EXISTS filings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uniq TEXT NOT NULL UNIQUE,            -- dedupe key (ACC:... or ROW:...)
  year INTEGER,                         -- derived from filedAt if possible
  cik TEXT,
  ticker TEXT,
  company TEXT,
  formType TEXT,
  filedAt TEXT,                         -- ISO date or datetime string as present
  reportPeriod TEXT,
  accessionNo TEXT,
  primaryDocument TEXT,
  filingUrl TEXT,
  size TEXT,
  extras_json TEXT                      -- JSON for any extra columns we didn't explicitly map
);
"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_filings_year      ON filings(year)",
    "CREATE INDEX IF NOT EXISTS idx_filings_formType  ON filings(formType)",
    "CREATE INDEX IF NOT EXISTS idx_filings_filedAt   ON filings(filedAt)",
    "CREATE INDEX IF NOT EXISTS idx_filings_company   ON filings(company)",
    "CREATE INDEX IF NOT EXISTS idx_filings_cik       ON filings(cik)"
]

def parse_year_from_filed_at(val: str) -> int:
    if not val:
        return None
    # common formats: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ
    try:
        if len(val) >= 10 and val[4] == "-" and val[7] == "-":
            return int(val[:4])
    except Exception:
        pass
    # try general parsing with datetime (best-effort)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y/%m/%d"):
        try:
            return datetime.strptime(val[:len(fmt)], fmt).year
        except Exception:
            continue
    return None

def normalize_accession_nodash(acc: str) -> str:
    if not acc:
        return ""
    acc = acc.strip()
    if "-" in acc:
        return acc.replace("-", "")
    # try to validate nodash structure (10+2+6)
    m = RE_ACC.match(acc)
    return m.group(0) if m else acc

def build_uniq(row: Dict[str, Any]) -> str:
    acc = normalize_accession_nodash(row.get("accessionNo") or "")
    if acc:
        return "ACC:" + acc
    # fallback: stable hash over key fields
    key = "|".join([
        str(row.get("company") or ""),
        str(row.get("formType") or ""),
        str(row.get("filedAt") or ""),
        str(row.get("filingUrl") or ""),
    ])
    return "ROW:" + hashlib.sha1(key.encode("utf-8")).hexdigest()

def open_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path))
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.executescript(DDL)
    for ix in INDEXES:
        con.execute(ix)
    con.commit()
    return con

def map_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    # flexible mapping: pull what we can, stash the rest in extras_json
    out = {k: (raw.get(k) or "") for k in CORE_COLS}
    # filedAt fallback: some master.idx pipelines use 'Date Filed' or similar
    if not out["filedAt"]:
        out["filedAt"] = raw.get("dateFiled") or raw.get("filed_at") or raw.get("filed_at_date") or ""

    # compute year if possible
    year = parse_year_from_filed_at(out["filedAt"])

    # uniq key
    uniq = build_uniq(out)

    # extras: anything not in CORE_COLS
    extras = {k: v for k, v in raw.items() if k not in CORE_COLS}
    return {
        "uniq": uniq,
        "year": year,
        **out,
        "extras_json": json.dumps(extras, ensure_ascii=False) if extras else None
    }

def upsert_rows(con: sqlite3.Connection, mapped_rows: List[Dict[str, Any]]) -> int:
    # UPSERT by uniq
    cols = [
        "uniq", "year", "cik", "ticker", "company", "formType", "filedAt", "reportPeriod",
        "accessionNo", "primaryDocument", "filingUrl", "size", "extras_json"
    ]
    placeholders = ",".join(["?"] * len(cols))
    update_assign = ",".join([f"{c}=excluded.{c}" for c in cols if c != "uniq"])
    sql = f"""
    INSERT INTO filings ({",".join(cols)})
    VALUES ({placeholders})
    ON CONFLICT(uniq) DO UPDATE SET {update_assign};
    """
    data = [
        [row.get(c) for c in cols]
        for row in mapped_rows
    ]
    cur = con.cursor()
    cur.executemany(sql, data)
    con.commit()
    return cur.rowcount  # affected rows (insert+update)

def ingest_csv(db: Path, csv_paths: List[Path]) -> None:
    con = open_db(db)
    total = 0
    for p in csv_paths:
        if not p.exists():
            print(f"[warn] CSV not found, skipping: {p}")
            continue
        print(f"[info] loading: {p}")
        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                print(f"[warn] empty or headerless CSV: {p}")
                continue
            batch = []
            BATCH = 2000
            for raw in reader:
                batch.append(map_row(raw))
                if len(batch) >= BATCH:
                    total += upsert_rows(con, batch)
                    batch.clear()
            if batch:
                total += upsert_rows(con, batch)
    con.close()
    print(f"[done] upserted rows (insert+update count): {total}")

def main(argv: List[str]):
    if "--help" in argv or len(argv) < 3 or argv[0] != "--db":
        print("Usage:\n  py holdiq_ingest.py --db <path_to_db> --csv <csv1> [<csv2> ...]")
        sys.exit(2)

    try:
        db_idx = argv.index("--db")
    except ValueError:
        print("Missing --db")
        sys.exit(2)

    db_path = Path(argv[db_idx + 1])
    if "--csv" not in argv:
        print("Missing --csv")
        sys.exit(2)
    csv_idx = argv.index("--csv")
    csv_args = argv[csv_idx + 1:]

    if not csv_args:
        print("Provide at least one CSV after --csv")
        sys.exit(2)

    ingest_csv(db_path, [Path(x) for x in csv_args])

if __name__ == "__main__":
    main(sys.argv[1:])
