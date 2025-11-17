# init_subscribers_table.py

import sqlite3

DB_PATH = r".\data\holdiq.db"


def ensure_subscribers_table() -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Base definition (for brand-new DBs)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            cik TEXT NOT NULL,
            tier TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            notes TEXT,
            is_comped INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    # Introspect existing columns (for migration on older DBs)
    existing_cols = {
        row[1] for row in cur.execute("PRAGMA table_info(subscribers)").fetchall()
    }

    # Add missing columns in a safe, incremental way
    if "email" not in existing_cols:
        cur.execute(
            "ALTER TABLE subscribers "
            "ADD COLUMN email TEXT NOT NULL DEFAULT ''"
        )

    if "cik" not in existing_cols:
        cur.execute(
            "ALTER TABLE subscribers "
            "ADD COLUMN cik TEXT NOT NULL DEFAULT ''"
        )

    if "tier" not in existing_cols:
        cur.execute(
            "ALTER TABLE subscribers "
            "ADD COLUMN tier TEXT NOT NULL DEFAULT 'nano'"
        )

    if "status" not in existing_cols:
        cur.execute(
            "ALTER TABLE subscribers "
            "ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"
        )

    if "created_at" not in existing_cols:
        cur.execute(
            "ALTER TABLE subscribers "
            "ADD COLUMN created_at TEXT NOT NULL "
            "DEFAULT (datetime('now'))"
        )

    if "updated_at" not in existing_cols:
        cur.execute(
            "ALTER TABLE subscribers "
            "ADD COLUMN updated_at TEXT NOT NULL "
            "DEFAULT (datetime('now'))"
        )

    if "notes" not in existing_cols:
        cur.execute("ALTER TABLE subscribers ADD COLUMN notes TEXT")

    if "is_comped" not in existing_cols:
        cur.execute(
            "ALTER TABLE subscribers "
            "ADD COLUMN is_comped INTEGER NOT NULL DEFAULT 0"
        )

    con.commit()
    con.close()
    print("✅ subscribers table ensured / aligned.")


if __name__ == "__main__":
    ensure_subscribers_table()
