import sqlite3

DB = r".\data\holdiq.db"

VIEWS_SQL = r"""
-- ========= Helpers =========

-- Best ticker available: prefer enriched JSON if present, else base ticker
CREATE VIEW IF NOT EXISTS v_ticker_mapped AS
SELECT
  rowid AS _rowid,
  COALESCE(json_extract(extras_json, '$.ticker_enriched'), ticker) AS ticker_best,
  *
FROM filings;

-- Canonical day/month/week/quarter breakdowns from filedAt
CREATE VIEW IF NOT EXISTS v_filed_calendar AS
SELECT
  f.rowid AS _rowid,
  substr(f.filedAt,1,10)                           AS day,
  strftime('%Y', f.filedAt)                         AS year_str,
  CAST(strftime('%Y', f.filedAt) AS INTEGER)        AS year,
  strftime('%Y-%m', f.filedAt)                      AS month,
  strftime('%Y', f.filedAt) || '-W' || strftime('%W', f.filedAt) AS week,
  CASE strftime('%m', f.filedAt)
      WHEN '01' THEN 'Q1' WHEN '02' THEN 'Q1' WHEN '03' THEN 'Q1'
      WHEN '04' THEN 'Q2' WHEN '05' THEN 'Q2' WHEN '06' THEN 'Q2'
      WHEN '07' THEN 'Q3' WHEN '08' THEN 'Q3' WHEN '09' THEN 'Q3'
      ELSE 'Q4'
  END AS quarter
FROM filings f;

-- ========= Volumetrics =========

-- Daily volume for all filings
CREATE VIEW IF NOT EXISTS v_daily_all AS
SELECT day, COUNT(*) AS n
FROM v_filed_calendar
GROUP BY day
ORDER BY day DESC;

-- Weekly volume for all filings
CREATE VIEW IF NOT EXISTS v_weekly_all AS
SELECT week, COUNT(*) AS n
FROM v_filed_calendar
GROUP BY week
ORDER BY week DESC;

-- Monthly volume for all filings
CREATE VIEW IF NOT EXISTS v_monthly_all AS
SELECT month, COUNT(*) AS n
FROM v_filed_calendar
GROUP BY month
ORDER BY month DESC;

-- Volume by formType per day (pivot-friendly)
CREATE VIEW IF NOT EXISTS v_daily_by_form AS
SELECT c.day, f.formType, COUNT(*) AS n
FROM filings f
JOIN v_filed_calendar c ON c._rowid = f.rowid
GROUP BY c.day, f.formType
ORDER BY c.day DESC, n DESC;

-- Volume by formType per month
CREATE VIEW IF NOT EXISTS v_monthly_by_form AS
SELECT c.month, f.formType, COUNT(*) AS n
FROM filings f
JOIN v_filed_calendar c ON c._rowid = f.rowid
GROUP BY c.month, f.formType
ORDER BY c.month DESC, n DESC;

-- Top N forms overall (all time)
CREATE VIEW IF NOT EXISTS v_top_forms_all AS
SELECT formType, COUNT(*) AS n
FROM filings
GROUP BY formType
ORDER BY n DESC;

-- ========= Snapshots / Recency =========

-- Latest filing per company (any form)
CREATE VIEW IF NOT EXISTS v_latest_per_company AS
SELECT company, MAX(filedAt) AS last_filed_at
FROM filings
GROUP BY company
ORDER BY last_filed_at DESC;

-- Latest of specific key forms
CREATE VIEW IF NOT EXISTS v_latest_10k AS
SELECT company, MAX(filedAt) AS last_10k
FROM filings
WHERE formType='10-K'
GROUP BY company
ORDER BY last_10k DESC;

CREATE VIEW IF NOT EXISTS v_latest_10q AS
SELECT company, MAX(filedAt) AS last_10q
FROM filings
WHERE formType='10-Q'
GROUP BY company
ORDER BY last_10q DESC;

CREATE VIEW IF NOT EXISTS v_latest_8k AS
SELECT company, MAX(filedAt) AS last_8k
FROM filings
WHERE formType='8-K'
GROUP BY company
ORDER BY last_8k DESC;

-- ========= Insiders / 13F / Funds =========

-- Insider trades (Form 4) last 7 days
CREATE VIEW IF NOT EXISTS v_form4_last7 AS
SELECT f.filedAt, f.company, COALESCE(json_extract(f.extras_json,'$.ticker_enriched'), f.ticker) AS ticker_best, f.filingUrl
FROM filings f
WHERE f.formType='4' AND f.filedAt >= date('now','-7 day')
ORDER BY f.filedAt DESC;

-- 13F-HR latest 60 days (institutions)
CREATE VIEW IF NOT EXISTS v_13f_last60 AS
SELECT f.filedAt, f.company, COALESCE(json_extract(f.extras_json,'$.ticker_enriched'), f.ticker) AS ticker_best, f.filingUrl
FROM filings f
WHERE f.formType='13F-HR' AND f.filedAt >= date('now','-60 day')
ORDER BY f.filedAt DESC;

-- Fund reporting NPORT-P last 30 days
CREATE VIEW IF NOT EXISTS v_nportp_last30 AS
SELECT f.filedAt, f.company, f.filingUrl
FROM filings f
WHERE f.formType='NPORT-P' AND f.filedAt >= date('now','-30 day')
ORDER BY f.filedAt DESC;

-- ========= Company Heatmaps / Activity =========

-- Most active companies (all forms), last 30 days
CREATE VIEW IF NOT EXISTS v_most_active_30d AS
SELECT f.company, COUNT(*) AS n
FROM filings f
WHERE f.filedAt >= date('now','-30 day')
GROUP BY f.company
ORDER BY n DESC, f.company;

-- Most active companies by form, last 30 days
CREATE VIEW IF NOT EXISTS v_most_active_30d_by_form AS
SELECT f.formType, f.company, COUNT(*) AS n
FROM filings f
WHERE f.filedAt >= date('now','-30 day')
GROUP BY f.formType, f.company
ORDER BY f.formType, n DESC;

-- ========= Velocity / Recency =========

-- Filing velocity per company (avg days between filings in last 180d)
-- (approximate via count / span)
CREATE VIEW IF NOT EXISTS v_company_velocity_180d AS
WITH w AS (
  SELECT company, MIN(filedAt) AS first_dt, MAX(filedAt) AS last_dt, COUNT(*) AS cnt
  FROM filings
  WHERE filedAt >= date('now','-180 day')
  GROUP BY company
)
SELECT
  company,
  cnt,
  CAST((julianday(last_dt) - julianday(first_dt)) AS REAL) AS span_days,
  CASE WHEN cnt > 1 THEN (julianday(last_dt) - julianday(first_dt)) / (cnt - 1)
       ELSE NULL END AS avg_days_between
FROM w
ORDER BY cnt DESC, avg_days_between ASC;

-- ========= Company Timelines / Drilldowns =========

-- All filings for a given company (parameterize in app layer), newest first
-- Example query:
--   SELECT filedAt, formType, filingUrl FROM v_company_timeline WHERE company = 'APPLE INC' ORDER BY filedAt DESC;
CREATE VIEW IF NOT EXISTS v_company_timeline AS
SELECT company, filedAt, formType, filingUrl
FROM filings
ORDER BY company, filedAt DESC;

-- ========= Convenience “Recent Highlights” =========

-- Last 24h highlights (what just happened)
CREATE VIEW IF NOT EXISTS v_recent_24h AS
SELECT filedAt, company, formType, filingUrl
FROM filings
WHERE filedAt >= datetime('now','-1 day')
ORDER BY filedAt DESC;

-- Last 7d highlights (top forms)
CREATE VIEW IF NOT EXISTS v_recent_7d_topforms AS
SELECT formType, COUNT(*) AS n
FROM filings
WHERE filedAt >= date('now','-7 day')
GROUP BY formType
ORDER BY n DESC;

-- ========= End =========
"""

INDEXES_SQL = r"""
-- Extra composite indexes to speed common filters
CREATE INDEX IF NOT EXISTS idx_filings_form_filed ON filings(formType, filedAt);
CREATE INDEX IF NOT EXISTS idx_filings_co_filed   ON filings(company, filedAt);
"""

def main():
    con = sqlite3.connect(DB)
    # ensure JSON1 ext is available; if not, views will still work but ticker_best falls back to base
    try:
        con.execute("SELECT json('[]')")
    except Exception:
        pass  # JSON1 missing; views with json_extract still compile but return NULLs

    con.executescript(INDEXES_SQL)
    con.executescript(VIEWS_SQL)
    con.commit()
    con.close()
    print("✅ Extended views + indexes created.")

if __name__ == "__main__":
    main()
