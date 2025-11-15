import sqlite3
con = sqlite3.connect(r".\data\holdiq.db")
con.executescript("""
DROP TABLE IF EXISTS filings;
CREATE TABLE filings (
    cik TEXT,
    ticker TEXT,
    company TEXT,
    formType TEXT,
    filedAt TEXT,
    reportPeriod TEXT,
    accessionNo TEXT NOT NULL PRIMARY KEY,
    primaryDocument TEXT,
    filingUrl TEXT,
    size TEXT
);
CREATE INDEX IF NOT EXISTS idx_filings_formType ON filings(formType);
CREATE INDEX IF NOT EXISTS idx_filings_filedAt  ON filings(filedAt);
CREATE INDEX IF NOT EXISTS idx_filings_company  ON filings(company);
CREATE INDEX IF NOT EXISTS idx_filings_cik      ON filings(cik);
""")
con.commit(); con.close()
print("✅ filings table rebuilt with PRIMARY KEY(accessionNo)")
