# reset_13f_parse_queue.py
#
# Clears existing 13F items from parse_queue, then repopulates from filings
# with more sane filters:
#   - Only 13F-HR / 13F-HR/A
#   - Exclude legacy '9999999997-%' accessions
#   - Use filedAt to focus on modern filings (2013+)
#   - Limit to a reasonable queue size

import sqlite3

DB_PATH = r".\data\holdiq.db"


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Show how many 13F items are currently in the queue
    cur.execute("SELECT COUNT(*) FROM parse_queue WHERE formType IN ('13F-HR', '13F-HR/A')")
    before = cur.fetchone()[0]
    print(f"Existing 13F items in parse_queue: {before}")

    # Remove ONLY 13F-related items from parse_queue
    cur.execute("DELETE FROM parse_queue WHERE formType IN ('13F-HR', '13F-HR/A')")
    con.commit()

    # Insert fresh, MODERN 13F filings from 'filings' table.
    # Columns in parse_queue are:
    #   id (PK), accessionNo, formType, filingUrl, enqueued_at, status, last_error
    #
    # We fill: accessionNo, formType, filingUrl, enqueued_at, status, last_error
    # and let 'id' autoincrement.
    cur.execute("""
        INSERT OR IGNORE INTO parse_queue (
            accessionNo,
            formType,
            filingUrl,
            enqueued_at,
            status,
            last_error
        )
        SELECT
            accessionNo,
            formType,
            filingUrl,
            datetime('now')      AS enqueued_at,
            'pending'            AS status,
            NULL                 AS last_error
        FROM filings
        WHERE formType IN ('13F-HR', '13F-HR/A')
          AND accessionNo NOT LIKE '9999999997-%'
          AND filedAt IS NOT NULL
          AND filedAt >= '2013-01-01'
        ORDER BY filedAt DESC
        LIMIT 20000
    """)
    con.commit()

    # How many did we add?
    cur.execute("SELECT COUNT(*) FROM parse_queue WHERE formType IN ('13F-HR', '13F-HR/A')")
    after = cur.fetchone()[0]
    print(f"13F items in parse_queue after reset: {after}")

    con.close()
    print("✅ 13F parse_queue reset (modern 13Fs by filedAt) complete.")


if __name__ == "__main__":
    main()
