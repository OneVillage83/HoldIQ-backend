import sqlite3

con = sqlite3.connect(r".\data\holdiq.db")
con.executescript("""
CREATE VIEW IF NOT EXISTS v_top_forms AS
  SELECT year, formType, COUNT(*) AS n
  FROM filings
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
""")
con.commit()
con.close()
print("✅ Views created")
