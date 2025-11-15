import sqlite3

con = sqlite3.connect(r".\data\holdiq.db")
cur = con.cursor()

print("\nTop 10 companies by filing count:")
for row in cur.execute("SELECT company, COUNT(*) AS n FROM filings_2024 GROUP BY company ORDER BY n DESC LIMIT 10"):
    print(row)

print("\nRecent 13F-HR filings:")
for row in cur.execute("SELECT filedAt, company, filingUrl FROM filings_2024 WHERE formType='13F-HR' ORDER BY filedAt DESC LIMIT 10"):
    print(row)

print("\nDaily filing volume (last 30 days):")
for row in cur.execute("SELECT filedAt, COUNT(*) FROM filings_2024 WHERE filedAt >= date('now','-30 day') GROUP BY filedAt ORDER BY filedAt DESC"):
    print(row)

con.close()
