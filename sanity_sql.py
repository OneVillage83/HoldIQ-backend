# sanity_sql.py
import sqlite3

con = sqlite3.connect(r".\data\holdiq.db")
rows     = con.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
max_date = con.execute("SELECT MAX(filedAt) FROM filings").fetchone()[0]

by_year = con.execute("""
  SELECT substr(filedAt,1,4) AS year, COUNT(*)
  FROM filings
  GROUP BY year
  ORDER BY year
""").fetchall()

top_forms = con.execute("""
  SELECT formType, COUNT(*) AS n
  FROM filings
  GROUP BY formType
  ORDER BY n DESC
  LIMIT 10
""").fetchall()

con.close()

print("Rows:", rows)
print("Max filedAt:", max_date)
print("Rows by year (first & last 3):", (by_year[:3], by_year[-3:]))
print("Top 10 forms:", top_forms)
