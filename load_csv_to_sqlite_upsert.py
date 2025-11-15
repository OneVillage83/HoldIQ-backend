#!/usr/bin/env python3
"""
Load a CSV into SQLite with de-duplication (UPSERT on accessionNo).

- Creates the table if it doesn't exist, with columns taken from the CSV header.
- Ensures a UNIQUE index on accessionNo (adds it if missing).
- Performs INSERT ... ON CONFLICT(accessionNo) DO UPDATE for idempotent loads.

USAGE:
  py load_csv_to_sqlite_upsert.py .\data\master\edgar_2024.csv .\data\holdiq.db filings
"""
import csv
import sqlite3
import sys
from pathlib import Path

def ensure_table(con: sqlite3.Connection, table: str, headers):
    # Create table if not exists using a generic TEXT schema for all columns.
    cols_sql = ", ".join([f'"{h}" TEXT' for h in headers])
    con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
    # Ensure UNIQUE constraint via unique index (works even if table already existed without constraint)
    con.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_accessionNo_unique ON "{table}"(accessionNo)')

def upsert_rows(con: sqlite3.Connection, table: str, headers, rows):
    placeholders = ",".join(["?"] * len(headers))
    cols = ",".join([f'"{h}"' for h in headers])
    # Build ON CONFLICT update set-list dynamically (skip accessionNo itself)
    set_list = ",".join([f'"{h}"=excluded."{h}"' for h in headers if h != "accessionNo"])
    sql = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders}) ON CONFLICT(accessionNo) DO UPDATE SET {set_list}'
    con.executemany(sql, rows)

def main():
    if len(sys.argv) < 4:
        print("usage: py load_csv_to_sqlite_upsert.py <csv_path> <db_path> <table>", file=sys.stderr)
        sys.exit(2)

    csv_path = Path(sys.argv[1])
    db_path  = Path(sys.argv[2])
    table    = sys.argv[3]

    if not csv_path.exists():
        print(f"[error] CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        headers = r.fieldnames or []
        if "accessionNo" not in headers:
            print("[error] This loader requires an 'accessionNo' column in the CSV.", file=sys.stderr)
            sys.exit(1)

        ensure_table(con, table, headers)

        batch = []
        n_ins = n = 0
        for row in r:
            batch.append(tuple(row.get(h) for h in headers))
            if len(batch) >= 10_000:
                upsert_rows(con, table, headers, batch)
                con.commit()
                n_ins += len(batch)
                batch.clear()
        if batch:
            upsert_rows(con, table, headers, batch)
            con.commit()
            n_ins += len(batch)

    con.close()
    print(f"[done] upserted rows into {db_path} (table: {table}): {n_ins}")

if __name__ == "__main__":
    main()
