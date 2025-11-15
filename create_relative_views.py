import sqlite3

DB = r".\data\holdiq.db"

SQL = """
CREATE VIEW IF NOT EXISTS v_recent_7d_topforms_rel AS
WITH mx AS (SELECT MAX(filedAt) m FROM filings)
SELECT formType, COUNT(*) AS n
FROM filings, mx
WHERE filedAt >= date(mx.m,'-7 day')
GROUP BY formType
ORDER BY n DESC;

CREATE VIEW IF NOT EXISTS v_recent_24h_rel AS
WITH mx AS (SELECT MAX(filedAt) m FROM filings)
SELECT filedAt, company, formType, filingUrl
FROM filings, mx
WHERE filedAt >= datetime(mx.m,'-1 day')
ORDER BY filedAt DESC;
"""

con = sqlite3.connect(DB)
con.executescript(SQL)
con.commit()
con.close()
print("✅ Relative views created")
