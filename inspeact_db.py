import sqlite3
con = sqlite3.connect(r'.\data\holdiq.db')
print(list(con.execute("SELECT type,name,sql FROM sqlite_master WHERE name='filings'")))
print(list(con.execute("PRAGMA table_info(filings)")))
print(list(con.execute("PRAGMA index_list(filings)")))
print("rows =", con.execute("SELECT COUNT(*) FROM filings").fetchone()[0])
print("distinct accessionNo =", con.execute("SELECT COUNT(DISTINCT accessionNo) FROM filings").fetchone()[0])
con.close()
