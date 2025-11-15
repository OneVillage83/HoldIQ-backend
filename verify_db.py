import sqlite3
con = sqlite3.connect(r".\data\holdiq.db")
print("Rows:", con.execute("SELECT COUNT(*) FROM filings").fetchone()[0])
print("Max filedAt:", con.execute("SELECT MAX(filedAt) FROM filings").fetchone()[0])
print("Years (first 5):", con.execute("SELECT substr(filedAt,1,4) AS y, COUNT(*) FROM filings GROUP BY y ORDER BY y LIMIT 5").fetchall())
print("Years (last 5):",  con.execute("SELECT substr(filedAt,1,4) AS y, COUNT(*) FROM filings GROUP BY y ORDER BY y DESC LIMIT 5").fetchall())
print("Top 10 form types:", con.execute("SELECT formType, COUNT(*) AS n FROM filings GROUP BY formType ORDER BY n DESC LIMIT 10").fetchall())
con.close()
