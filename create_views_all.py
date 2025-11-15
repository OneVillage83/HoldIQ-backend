#!/usr/bin/env python3
import sqlite3, sys, pathlib

DB = r".\data\holdiq.db"

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
-- Canonical view that computes 'year' on the fly
CREATE VIEW IF NOT EXISTS v_filings_norm AS
SELECT
  {YEAR_EXPR} AS year,
  filedAt,
  ticker,
  company,
  formType,
  accessionNo,
  primaryDocument,
  filingUrl,
  size
FROM filings;

-- Top forms by year (doesn't require a physical 'year' column)
CREATE VIEW IF NOT EXISTS v_top_forms AS
SELECT year, formType, COUNT(*) AS n
FROM v_filings_norm
GROUP BY year, formType
ORDER BY year, n DESC;

-- Daily totals
CREATE VIEW IF NOT EXISTS v_daily AS
SELECT substr(filedAt,1,10) AS day, COUNT(*) AS n
FROM filings
GROUP BY day
ORDER BY day DESC;

-- 13F recents
CREATE VIEW IF NOT EXISTS v_recent_13f AS
SELECT filedAt, company, ticker, filingUrl
FROM filings
WHERE formType='13F-HR'
ORDER BY filedAt DESC;

-- Recent 24h (based on filedAt date granularity)
CREATE VIEW IF NOT EXISTS v_recent_24h AS
SELECT filedAt, company, formType, filingUrl
FROM filings
WHERE filedAt >= (SELECT MAX(filedAt) FROM filings, (SELECT 1) tmp)
-- this is a loose approx if filedAt lacks time; feel free to refine once EFTS JSON is available
ORDER BY filedAt DESC;

-- Last 7 days top forms
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

INDEXES_SQL = """
-- helpful indexes; ignore if already exist
CREATE INDEX IF NOT EXISTS idx_filings_formType ON filings(formType);
CREATE INDEX IF NOT EXISTS idx_filings_filedAt  ON filings(filedAt);
CREATE INDEX IF NOT EXISTS idx_filings_company  ON filings(company);
CREATE INDEX IF NOT EXISTS idx_filings_accNo    ON filings(accessionNo);
"""

def main():
    con = sqlite3.connect(DB)
    con.executescript(INDEXES_SQL)
    con.executescript(VIEWS_SQL)
    con.commit()
    con.close()
    print("✅ Views created/updated (no physical year needed).")

if __name__ == "__main__":
    main()
