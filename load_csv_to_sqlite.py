#!/usr/bin/env python3
"""
Load a CSV of filings into SQLite efficiently (streaming insert).
Usage:
  py load_csv_to_sqlite.py .\data\holdiq_2024_tiers_enriched.csv .\data\holdiq.db filings
If the table doesn't exist, it's created. If it exists, rows are appended.
"""

import csv
import sqlite3
import sys
from pathlib import Path

def main():
    if len(sys.argv) < 4:
        print("Usage: py load_csv_to_sqlite.py <input.csv> <output.db> <table_name>")
        sys.exit(2)

    csv_path = Path(sys.argv[1])
    db_path  = Path(sys.argv[2])
    table    = sys.argv[3]

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        sys.exit(2)

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()

    # Read header from CSV to define columns dynamically
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        columns = r.fieldnames
        if not columns:
            print("CSV has no header/columns.")
            sys.exit(2)

        # Create table if not exists with all columns as TEXT for simplicity
        cols_sql = ", ".join(f'"{c}" TEXT' for c in columns)
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')

        # Speed up bulk insert (safe for local, single-writer use)
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute("PRAGMA temp_store=MEMORY;")

        # Prepare insert statement
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f'INSERT INTO "{table}" ({", ".join(columns)}) VALUES ({placeholders})'

        # Stream rows
        batch = []
        BATCH_SIZE = 2000
        total = 0
        for row in r:
            batch.append([row.get(c) for c in columns])
            if len(batch) >= BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                con.commit()
                total += len(batch)
                batch.clear()
        if batch:
            cur.executemany(insert_sql, batch)
            con.commit()
            total += len(batch)

    # Optional helpful indexes (speeds up common queries)
    try:
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_formType ON "{table}" (formType)')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_filedAt  ON "{table}" (filedAt)')
        cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_cik      ON "{table}" (cik)')
        con.commit()
    except Exception:
        pass

    con.close()
    print(f"Loaded {total} rows into {db_path} (table: {table}).")

if __name__ == "__main__":
    main()
