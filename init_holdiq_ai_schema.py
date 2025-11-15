# init_holdiq_ai_schema.py
import sqlite3

db = r".\data\holdiq.db"
con = sqlite3.connect(db)
c = con.cursor()

schema = """
-- parsed filings & AI-layer schema

CREATE TABLE IF NOT EXISTS filings_parsed (
  accessionNo TEXT PRIMARY KEY,
  formType    TEXT,
  parsed_at   TEXT,
  succeeded   INTEGER,
  err         TEXT
);

CREATE TABLE IF NOT EXISTS positions_13f (
  manager_cik     TEXT,
  report_period   TEXT,
  cusip           TEXT,
  issuer          TEXT,
  class           TEXT,
  shares          REAL,
  value_usd       REAL,
  put_call        TEXT,
  discretion      TEXT,
  voting_sole     INTEGER,
  voting_shared   INTEGER,
  voting_none     INTEGER,
  PRIMARY KEY (manager_cik, report_period, cusip)
);

CREATE TABLE IF NOT EXISTS positions_13f_delta (
  manager_cik     TEXT,
  report_period   TEXT,
  cusip           TEXT,
  issuer          TEXT,
  action          TEXT,
  shares_prev     REAL,
  shares_now      REAL,
  value_prev      REAL,
  value_now       REAL,
  shares_delta    REAL,
  value_delta     REAL,
  pct_of_port     REAL,
  rank_in_port    INTEGER,
  PRIMARY KEY (manager_cik, report_period, cusip)
);

CREATE TABLE IF NOT EXISTS insider_tx (
  issuer_cik      TEXT,
  issuer_ticker   TEXT,
  issuer_name     TEXT,
  reporting_owner TEXT,
  owner_title     TEXT,
  trans_date      TEXT,
  trans_code      TEXT,
  side            TEXT,
  shares          REAL,
  price           REAL,
  direct_indirect TEXT,
  accessionNo     TEXT
);

cur.executescript("""
CREATE TABLE IF NOT EXISTS parse_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    accessionNo TEXT NOT NULL UNIQUE,
    formType TEXT NOT NULL,
    filingUrl TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_parse_queue_status
    ON parse_queue(status);
CREATE INDEX IF NOT EXISTS idx_parse_queue_formType
    ON parse_queue(formType);
""")

CREATE INDEX IF NOT EXISTS idx_itx_issuer_date ON insider_tx(issuer_cik, trans_date);
"""

c.executescript(schema)
con.commit()
con.close()

print("✅ HoldIQ AI/ML schema initialized.")
