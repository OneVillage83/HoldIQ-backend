#!/usr/bin/env python3
import argparse, sqlite3

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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()
    con = sqlite3.connect(args.db)
    con.executescript(VIEWS_SQL)
    con.commit()
    names = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='view' AND name LIKE 'v_%'")]
    con.close()
    print("Views present:", ", ".join(sorted(names)))

if __name__ == "__main__":
    main()
