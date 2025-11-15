import sqlite3, json
con = sqlite3.connect(r".\data\holdiq.db")
print("sqlite_master entry for filings:")
print(list(con.execute("SELECT type,name,sql FROM sqlite_master WHERE name='filings'")))
print("\nPRAGMA table_info(filings):")
print(list(con.execute("PRAGMA table_info(filings)")))
print("\nIndexes on filings:")
print(list(con.execute("PRAGMA index_list(filings)")))
try:
    rows = con.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    print("\nRow count:", rows)
    dy = list(con.execute("SELECT substr(filedAt,1,4) AS y, COUNT(*) FROM filings GROUP BY y ORDER BY y"))
    print("Rows by year:", dy[:3], "...", dy[-3:])
except Exception as e:
    print("\nQuery error:", e)
con.close()
