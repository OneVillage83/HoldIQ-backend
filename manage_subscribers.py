# manage_subscribers.py
import sqlite3
import sys
from datetime import datetime
from typing import Optional, List, Tuple
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "holdiq.db"


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def ensure_table(con: sqlite3.Connection) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS subscribers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        cik TEXT NOT NULL,
        tier TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    """
    con.execute(ddl)
    con.commit()


def add_or_update_subscriber(
    email: str, cik: str, tier: str, active: int = 1
) -> int:
    """
    Upsert a subscriber row: (email, cik) is treated as unique logical key.

    If a row exists, we update its tier/active/updated_at.
    If not, we insert a new row.
    Returns the row id.
    """
    tier = tier.lower()
    if tier not in ("nano", "mini", "premium"):
        raise ValueError("tier must be one of: nano, mini, premium")

    con = get_conn()
    try:
        ensure_table(con)
        cur = con.cursor()

        row = cur.execute(
            """
            SELECT id FROM subscribers
            WHERE email = ? AND cik = ?
            LIMIT 1
            """,
            (email, cik),
        ).fetchone()

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if row:
            sub_id = row[0]
            cur.execute(
                """
                UPDATE subscribers
                SET tier = ?, active = ?, updated_at = ?
                WHERE id = ?
                """,
                (tier, active, now, sub_id),
            )
            print(f"✅ Updated subscriber id={sub_id}: {email} | CIK={cik} | tier={tier} | active={active}")
        else:
            cur.execute(
                """
                INSERT INTO subscribers (email, cik, tier, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (email, cik, tier, active, now, now),
            )
            sub_id = cur.lastrowid
            print(f"✅ Inserted subscriber id={sub_id}: {email} | CIK={cik} | tier={tier} | active={active}")

        con.commit()
        return sub_id
    finally:
        con.close()


def list_subscribers() -> None:
    con = get_conn()
    try:
        ensure_table(con)
        rows = con.execute(
            """
            SELECT id, email, cik, tier, active, created_at, updated_at
            FROM subscribers
            ORDER BY email, cik
            """
        ).fetchall()

        if not rows:
            print("No subscribers found.")
            return

        for r in rows:
            _id, email, cik, tier, active, created_at, updated_at = r
            status = "ACTIVE" if active else "INACTIVE"
            print(f"[{_id}] {email} | CIK={cik} | tier={tier} | {status} | created={created_at} | updated={updated_at}")
    finally:
        con.close()


def set_active(sub_id: int, active: int) -> None:
    con = get_conn()
    try:
        ensure_table(con)
        cur = con.cursor()
        cur.execute(
            "UPDATE subscribers SET active = ?, updated_at = datetime('now') WHERE id = ?",
            (active, sub_id),
        )
        con.commit()
        print(f"✅ Updated subscriber id={sub_id} active={active}")
    finally:
        con.close()


def change_tier(sub_id: int, tier: str) -> None:
    tier = tier.lower()
    if tier not in ("nano", "mini", "premium"):
        raise ValueError("tier must be one of: nano, mini, premium")

    con = get_conn()
    try:
        ensure_table(con)
        cur = con.cursor()
        cur.execute(
            "UPDATE subscribers SET tier = ?, updated_at = datetime('now') WHERE id = ?",
            (tier, sub_id),
        )
        con.commit()
        print(f"✅ Updated subscriber id={sub_id} tier={tier}")
    finally:
        con.close()


def print_usage() -> None:
    print(
        "Usage:\n"
        "  py manage_subscribers.py list\n"
        "  py manage_subscribers.py add <email> <cik> <tier>\n"
        "  py manage_subscribers.py deactivate <id>\n"
        "  py manage_subscribers.py activate <id>\n"
        "  py manage_subscribers.py change-tier <id> <tier>\n"
        "\n"
        "Examples:\n"
        "  py manage_subscribers.py add alice@example.com 1558481 nano\n"
        "  py manage_subscribers.py add bob@example.com 1558481 premium\n"
        "  py manage_subscribers.py list\n"
        "  py manage_subscribers.py deactivate 3\n"
        "  py manage_subscribers.py change-tier 5 mini\n"
    )


def main(argv: list[str]) -> None:
    if len(argv) < 2:
        print_usage()
        return

    cmd = argv[1].lower()

    if cmd == "list":
        list_subscribers()
    elif cmd == "add":
        if len(argv) != 5:
            print("Usage: py manage_subscribers.py add <email> <cik> <tier>")
            return
        email, cik, tier = argv[2], argv[3], argv[4]
        add_or_update_subscriber(email, cik, tier)
    elif cmd in ("deactivate", "activate"):
        if len(argv) != 3:
            print(f"Usage: py manage_subscribers.py {cmd} <id>")
            return
        sub_id = int(argv[2])
        active = 0 if cmd == "deactivate" else 1
        set_active(sub_id, active)
    elif cmd == "change-tier":
        if len(argv) != 4:
            print("Usage: py manage_subscribers.py change-tier <id> <tier>")
            return
        sub_id = int(argv[2])
        tier = argv[3]
        change_tier(sub_id, tier)
    else:
        print(f"Unknown command: {cmd}")
        print()
        print_usage()


if __name__ == "__main__":
    main(sys.argv)
