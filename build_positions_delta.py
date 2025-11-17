# build_positions_delta.py
#
# Compute quarter-over-quarter position deltas for each manager
# using positions_13f, which has:
#   manager_cik, report_period, cusip, issuer, class,
#   shares, value_usd, put_call, discretion, voting_*
#
# We write into positions_13f_delta with richer numeric fields that
# the AI layer will consume later.

import sqlite3
from collections import defaultdict

DB_PATH = r".\data\holdiq.db"


def fetch_quarter_positions(cur, manager_cik, report_period):
    """
    Load all positions for a given manager and report_period from positions_13f,
    and compute each position's weight_pct within that quarter.
    """
    rows = cur.execute("""
        SELECT
            cusip,
            issuer,
            shares,
            value_usd
        FROM positions_13f
        WHERE manager_cik = ? AND report_period = ?
    """, (manager_cik, report_period)).fetchall()

    # Compute total value for weight calculation
    total_value = sum((r[3] or 0.0) for r in rows)
    if total_value <= 0:
        total_value = 0.0

    positions = {}
    for cusip, issuer, shares, value_usd in rows:
        value_usd = value_usd or 0.0
        shares = shares or 0.0
        weight_pct = (value_usd / total_value * 100.0) if total_value > 0 else 0.0

        positions[cusip] = {
            "cusip": cusip,
            "companyName": issuer,
            "shares": shares,
            "value_usd": value_usd,
            "weight_pct": weight_pct,
        }

    return positions


def compute_delta_row(manager_cik, report_period, cusip, prev_pos, curr_pos):
    """
    prev_pos or curr_pos can be None (for new/closed).
    Returns a dict matching the positions_13f_delta schema we want.
    """

    if prev_pos is None and curr_pos is None:
        raise ValueError("Both prev_pos and curr_pos are None – shouldn't happen")

    # Use whichever side is present for the name
    companyName = (curr_pos or prev_pos)["companyName"]

    old_shares = prev_pos["shares"] if prev_pos else 0.0
    new_shares = curr_pos["shares"] if curr_pos else 0.0
    delta_shares = new_shares - old_shares

    old_value = prev_pos["value_usd"] if prev_pos else 0.0
    new_value = curr_pos["value_usd"] if curr_pos else 0.0
    delta_value = new_value - old_value

    old_weight = prev_pos["weight_pct"] if prev_pos else 0.0
    new_weight = curr_pos["weight_pct"] if curr_pos else 0.0
    delta_weight = new_weight - old_weight

    # Classify delta_type
    if prev_pos is None and curr_pos is not None:
        delta_type = "new"
    elif prev_pos is not None and curr_pos is None:
        delta_type = "closed"
    else:
        # Both sides exist
        if delta_shares > 0 and delta_value >= 0:
            delta_type = "increase"
        elif delta_shares < 0 and delta_value <= 0:
            delta_type = "decrease"
        else:
            # Mixed (shares vs value); classify by value direction
            delta_type = "increase" if delta_value > 0 else "decrease"

    # NOTE: we store cusip in the "ticker" field for now.
    # Later we can map cusip -> actual ticker via another table/view.
    return {
        "cik": manager_cik,
        "reportPeriod": report_period,
        "ticker": cusip,
        "companyName": companyName,
        "delta_type": delta_type,
        "old_shares": old_shares,
        "new_shares": new_shares,
        "delta_shares": delta_shares,
        "old_value_usd": old_value,
        "new_value_usd": new_value,
        "delta_value_usd": delta_value,
        "old_weight_pct": old_weight,
        "new_weight_pct": new_weight,
        "delta_weight_pct": delta_weight,
    }


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Make sure positions_13f_delta exists with the rich schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS positions_13f_delta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT NOT NULL,
            reportPeriod TEXT NOT NULL,
            ticker TEXT NOT NULL,
            companyName TEXT,
            delta_type TEXT NOT NULL,
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

    print("Clearing existing rows in positions_13f_delta...")
    cur.execute("DELETE FROM positions_13f_delta")
    con.commit()

    # Collect all distinct (manager_cik, report_period), grouped by manager
    rows = cur.execute("""
        SELECT DISTINCT manager_cik, report_period
        FROM positions_13f
        ORDER BY manager_cik, report_period
    """).fetchall()

    by_manager = defaultdict(list)
    for manager_cik, report_period in rows:
        by_manager[manager_cik].append(report_period)

    total_inserted = 0

    for manager_cik, periods in by_manager.items():
        periods_sorted = sorted(periods)  # 'YYYY-MM-DD' should sort lexicographically
        if len(periods_sorted) < 2:
            continue  # need at least two quarters to compute deltas

        print(f"Processing manager_cik {manager_cik}, {len(periods_sorted)} quarters...")

        for i in range(1, len(periods_sorted)):
            prev_rp = periods_sorted[i - 1]
            curr_rp = periods_sorted[i]

            prev_positions = fetch_quarter_positions(cur, manager_cik, prev_rp)
            curr_positions = fetch_quarter_positions(cur, manager_cik, curr_rp)

            all_cusips = set(prev_positions.keys()) | set(curr_positions.keys())
            delta_rows = []

            for cusip in all_cusips:
                prev_pos = prev_positions.get(cusip)
                curr_pos = curr_positions.get(cusip)
                delta_row = compute_delta_row(manager_cik, curr_rp, cusip, prev_pos, curr_pos)
                delta_rows.append(delta_row)

            cur.executemany("""
                INSERT INTO positions_13f_delta (
                    cik, reportPeriod, ticker, companyName,
                    delta_type,
                    old_shares, new_shares, delta_shares,
                    old_value_usd, new_value_usd, delta_value_usd,
                    old_weight_pct, new_weight_pct, delta_weight_pct
                )
                VALUES (
                    :cik, :reportPeriod, :ticker, :companyName,
                    :delta_type,
                    :old_shares, :new_shares, :delta_shares,
                    :old_value_usd, :new_value_usd, :delta_value_usd,
                    :old_weight_pct, :new_weight_pct, :delta_weight_pct
                )
            """, delta_rows)
            con.commit()
            total_inserted += len(delta_rows)

    con.close()
    print(f"✅ Done. Inserted {total_inserted} rows into positions_13f_delta.")


if __name__ == "__main__":
    main()
