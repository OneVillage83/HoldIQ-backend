#!/usr/bin/env python3
import csv, sqlite3, sys
from pathlib import Path

def ensure_table(con: sqlite3.Connection, table: str, headers):
    cols_sql = ", ".join([f'"{h}" TEXT' for h in headers])
    con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
    con.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_accessionNo_unique ON "{table}"(accessionNo)')

def derive_accession(row):
    acc = (row.get("accessionNo") or "").strip()
    if acc:
        return acc
    pd = (row.get("primaryDocument") or "").strip()
    # Expect something like "0000950170-25-021128.txt"
    if pd:
        # only file name part
        pd = pd.split("/")[-1].split("\\")[-1]
        # drop extension(s)
        if "." in pd:
            pd = pd.split(".")[0]
        return pd
    return ""

def upsert_rows(con, table, headers, rows):
    placeholders = ",".join(["?"] * len(headers))
    cols = ",".join([f'"{h}"' for h in headers])
    set_list = ",".join([f'"{h}"=excluded."{h}"' for h in headers if h != "accessionNo"])
    sql = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders}) ON CONFLICT(accessionNo) DO UPDATE SET {set_list}'
    con.executemany(sql, rows)

def main():
    if len(sys.argv) < 4:
        print("usage: py load_csv_to_sqlite_upsert_fixacc.py <csv_path> <db_path> <table>", file=sys.stderr)
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
        # Ensure expected columns exist
        for need in ("accessionNo", "primaryDocument"):
            if need not in headers:
                print(f"[error] CSV missing required column: {need}", file=sys.stderr)
                sys.exit(1)

        ensure_table(con, table, headers)

        batch, n = [], 0
        for row in r:
            # derive accession if missing
            acc = derive_accession(row)
            row["accessionNo"] = acc
            if not acc:
                # skip rows that still have no accession (extremely rare)
                continue
            batch.append(tuple(row.get(h) for h in headers))
            if len(batch) >= 10_000:
                upsert_rows(con, table, headers, batch)
                con.commit()
                n += len(batch)
                batch.clear()
        if batch:
            upsert_rows(con, table, headers, batch)
            con.commit()
            n += len(batch)

    con.close()
    print(f"[done] upserted rows into {db_path} (table: {table}): {n}")

if __name__ == "__main__":
    main()
