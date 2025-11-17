# migrate_positions_delta.py

import sqlite3

DB_PATH = r".\data\holdiq.db"

def column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # If table doesn't exist at all, create it fresh
    cur.execute("""
        CREATE TABLE IF NOT EXISTS positions_13f_delta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT NOT NULL,
            reportPeriod TEXT NOT NULL,
            ticker TEXT NOT NULL,
            companyName TEXT,
            delta_type TEXT NOT NULL, -- 'new','increase','decrease','closed'
            old_shares REAL,
            new_shares REAL,
            delta_shares REAL,
            old_value_usd REAL,
            new_value_usd REAL,
            delta_value_usd REAL,
            old_weight_pct REAL,
            new_weight_pct REAL,
            delta_weight_pct REAL
        )
    """)
    con.commit()

    # Ensure all columns exist (for older versions of the table)
    required_cols = {
        "delta_type": "TEXT NOT NULL DEFAULT 'increase'",
        "old_shares": "REAL",
        "new_shares": "REAL",
        "delta_shares": "REAL",
        "old_value_usd": "REAL",
        "new_value_usd": "REAL",
        "delta_value_usd": "REAL",
        "old_weight_pct": "REAL",
        "new_weight_pct": "REAL",
        "delta_weight_pct": "REAL",
    }

    for col, coltype in required_cols.items():
        if not column_exists(cur, "positions_13f_delta", col):
            print(f"Adding column {col} to positions_13f_delta")
            cur.execute(f"ALTER TABLE positions_13f_delta ADD COLUMN {col} {coltype}")

    con.commit()
    con.close()
    print("✅ Migration complete for positions_13f_delta")

if __name__ == "__main__":
    main()
