# create_indexes.py
import sqlite3

con = sqlite3.connect(r'.\data\holdiq.db')
con.executescript("""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

/* Clean up any old yearly indexes */
DROP INDEX IF EXISTS idx_f2024_formType;
DROP INDEX IF EXISTS idx_f2024_filedAt;
DROP INDEX IF EXISTS idx_f2024_company;
DROP INDEX IF EXISTS idx_filings_year_form;

/* Single-column indexes on existing columns */
CREATE INDEX IF NOT EXISTS idx_filings_formType ON filings(formType);
CREATE INDEX IF NOT EXISTS idx_filings_filedAt  ON filings(filedAt);
CREATE INDEX IF NOT EXISTS idx_filings_company  ON filings(company);
CREATE INDEX IF NOT EXISTS idx_filings_ticker   ON filings(ticker);
CREATE INDEX IF NOT EXISTS idx_filings_cik      ON filings(cik);

/* Expression index: year derived from filedAt */
CREATE INDEX IF NOT EXISTS idx_filings_year_form_expr
ON filings( substr(filedAt,1,4), formType );

/* Helpful for “last day” queries */
CREATE INDEX IF NOT EXISTS idx_filings_form_date
ON filings(formType, filedAt DESC);
""")
con.close()
print("Indexes created on table 'filings'.")
