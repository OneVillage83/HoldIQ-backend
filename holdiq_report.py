#!/usr/bin/env python3
import argparse, csv, sqlite3
from pathlib import Path

# ----- same YEAR expr used in create_views_all.py -----
YEAR_EXPR = """CAST(
  CASE
    WHEN filedAt IS NOT NULL AND length(filedAt) >= 4 AND substr(filedAt,1,4) GLOB '[0-9][0-9][0-9][0-9]'
      THEN substr(filedAt,1,4)
    WHEN accessionNo IS NOT NULL AND instr(accessionNo,'-')>0
      THEN CASE
        WHEN length(substr(accessionNo, instr(accessionNo,'-')+1, 2))=2
          THEN (2000 + CAST(substr(accessionNo, instr(accessionNo,'-')+1, 2) AS INT))
        ELSE NULL
      END
    ELSE NULL
  END AS INT
)"""

VIEWS_SQL = f"""
CREATE VIEW IF NOT EXISTS v_filings_norm AS
SELECT
  {YEAR_EXPR} AS year,
  filedAt, ticker, company, formType, accessionNo, primaryDocument, filingUrl, size
FROM filings;

CREATE VIEW IF NOT EXISTS v_top_forms AS
SELECT year, formType, COUNT(*) AS n
FROM v_filings_norm
GROUP BY year, formType
ORDER BY year, n DESC;

CREATE VIEW IF NOT EXISTS v_daily AS
SELECT substr(filedAt,1,10) AS day, COUNT(*) AS n
FROM filings
GROUP BY day
ORDER BY day DESC;

CREATE VIEW IF NOT EXISTS v_recent_13f AS
SELECT filedAt, company, ticker, filingUrl
FROM filings
WHERE formType='13F-HR'
ORDER BY filedAt DESC;

CREATE VIEW IF NOT EXISTS v_recent_24h AS
SELECT filedAt, company, formType, filingUrl
FROM filings
WHERE filedAt >= (SELECT MAX(filedAt) FROM filings)
ORDER BY filedAt DESC;

CREATE VIEW IF NOT EXISTS v_recent_7d_topforms AS
WITH days AS (
  SELECT substr(filedAt,1,10) AS day
  FROM filings
  WHERE filedAt IS NOT NULL
  GROUP BY day
  ORDER BY day DESC
  LIMIT 7
)
SELECT f.formType, COUNT(*) AS n
FROM filings f
JOIN days d ON substr(f.filedAt,1,10) = d.day
GROUP BY f.formType
ORDER BY n DESC;
"""

def ensure_views(con: sqlite3.Connection):
    have = dict(con.execute(
        "SELECT name, 1 FROM sqlite_master WHERE type='view'"
    ).fetchall())
    if "v_top_forms" not in have:
        con.executescript(VIEWS_SQL)
        con.commit()

def q(con, sql, params=()):
    return con.execute(sql, params).fetchall()

def write_csv(path, headers, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(headers); w.writerows(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    ensure_views(con)

    total = q(con, "SELECT COUNT(*) FROM filings")[0][0]
    max_date = q(con, "SELECT MAX(filedAt) FROM filings")[0][0]
    top_forms = q(con, "SELECT formType, COUNT(*) as n FROM filings GROUP BY formType ORDER BY n DESC LIMIT 20")
    top_forms_by_year = q(con, "SELECT year, formType, n FROM v_top_forms LIMIT 5000")
    recent7 = q(con, "SELECT * FROM v_recent_7d_topforms LIMIT 200")
    recent_stream = q(con, "SELECT filedAt, company, formType, filingUrl FROM v_recent_24h LIMIT 200")

    out = Path(args.out_dir)
    write_csv(out / "summary_totals.csv", ["metric","value"], [
        ("total_rows", total), ("max_filedAt", max_date),
    ])
    write_csv(out / "top_forms_overall.csv", ["formType","count"], top_forms)
    write_csv(out / "top_forms_by_year.csv", ["year","formType","count"], top_forms_by_year)
    write_csv(out / "recent_7d_topforms.csv", ["formType","count"], recent7)
    write_csv(out / "recent_stream.csv", ["filedAt","company","formType","filingUrl"], recent_stream)

    con.close()
    print("✅ Report written to", str(out))

if __name__ == "__main__":
    main()
