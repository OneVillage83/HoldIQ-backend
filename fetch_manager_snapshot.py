# fetch_manager_snapshot.py
"""
Builds a structured snapshot for a single 13F manager from positions_13f.

This is the "fuel" for HoldIQ's AI layer:
- Total portfolio value
- Number of positions
- Top holdings (issuer, cusip, shares, value, weight)
- Concentration metrics, etc.

Usage:
    py fetch_manager_snapshot.py <manager_cik> [report_period]

If report_period is omitted, we use the latest non-empty report_period
found in positions_13f for that manager.
"""

import json
import os
import sqlite3
import sys
from typing import Any, Dict, Optional

DB_PATH = r".\data\holdiq.db"
OUT_DIR = r".\out"


def get_latest_period_for_manager(con: sqlite3.Connection, cik: str) -> Optional[str]:
    row = con.execute(
        """
        SELECT MAX(report_period)
        FROM positions_13f
        WHERE manager_cik = ?
          AND report_period IS NOT NULL
          AND report_period <> ''
        """,
        (cik,),
    ).fetchone()
    return row[0] if row and row[0] else None


def build_manager_snapshot(
    manager_cik: str,
    report_period: Optional[str] = None,
    top_n: int = 50,
) -> Dict[str, Any]:
    """
    Returns a dictionary with:
      {
        "manager": {...},
        "holdings": [...],
      }
    """
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Determine period if not provided
    if report_period is None:
        report_period = get_latest_period_for_manager(con, manager_cik)
        if not report_period:
            con.close()
            raise ValueError(
                f"No non-empty report_period found in positions_13f "
                f"for manager_cik={manager_cik}"
            )

    # Get manager metadata from filings
    meta = cur.execute(
        """
        SELECT company, ticker, filedAt, accessionNo
        FROM filings
        WHERE cik = ?
          AND formType IN ('13F-HR', '13F-HR/A')
        ORDER BY filedAt DESC
        LIMIT 1
        """,
        (manager_cik,),
    ).fetchone()

    manager_name = meta["company"] if meta else None
    manager_ticker = meta["ticker"] if meta else None
    latest_filed_at = meta["filedAt"] if meta else None
    latest_accession = meta["accessionNo"] if meta else None

    # Totals for this period
    total_row = cur.execute(
        """
        SELECT
            SUM(value_usd) AS total_value_usd,
            COUNT(*) AS num_positions
        FROM positions_13f
        WHERE manager_cik = ?
          AND report_period = ?
        """,
        (manager_cik, report_period),
    ).fetchone()

    total_value_usd = float(total_row["total_value_usd"] or 0.0)
    num_positions = int(total_row["num_positions"] or 0)

    # Detailed holdings (top N by value)
    rows = cur.execute(
        """
        SELECT issuer, cusip, shares, value_usd
        FROM positions_13f
        WHERE manager_cik = ?
          AND report_period = ?
        ORDER BY value_usd DESC
        LIMIT ?
        """,
        (manager_cik, report_period, top_n),
    ).fetchall()

    holdings = []
    running_sum_top10 = 0.0

    for idx, r in enumerate(rows):
        value = float(r["value_usd"] or 0.0)
        weight_pct = (value / total_value_usd * 100.0) if total_value_usd > 0 else 0.0

        if idx < 10:
            running_sum_top10 += weight_pct

        holdings.append(
            {
                "rank": idx + 1,
                "issuer": r["issuer"],
                "cusip": r["cusip"],
                "shares": float(r["shares"] or 0.0),
                "value_usd": value,
                "weight_pct": weight_pct,
            }
        )

    snapshot: Dict[str, Any] = {
        "manager": {
            "cik": manager_cik,
            "name": manager_name,
            "ticker": manager_ticker,
            "latest_period": report_period,
            "latest_filed_at": latest_filed_at,
            "latest_accession": latest_accession,
            "total_value_usd": total_value_usd,
            "num_positions": num_positions,
            "top10_concentration_pct": running_sum_top10,
        },
        "holdings": holdings,
    }

    con.close()
    return snapshot


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: py fetch_manager_snapshot.py <manager_cik> [report_period]\n"
            "Example: py fetch_manager_snapshot.py 1558481 2025-11-03"
        )
        sys.exit(1)

    manager_cik = sys.argv[1]
    report_period = sys.argv[2] if len(sys.argv) >= 3 else None

    snapshot = build_manager_snapshot(manager_cik, report_period)

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(
        OUT_DIR,
        f"snapshot_{snapshot['manager']['cik']}_{snapshot['manager']['latest_period']}.json",
    )

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    print(f"✅ Wrote snapshot to {out_path}")
    print(
        f"Manager {snapshot['manager']['cik']} | "
        f"{snapshot['manager']['name']} | "
        f"period={snapshot['manager']['latest_period']}"
    )
    print(
        f"Total value: ${snapshot['manager']['total_value_usd']:,.0f} "
        f"across {snapshot['manager']['num_positions']} positions"
    )
    print(
        f"Top 10 concentration: "
        f"{snapshot['manager']['top10_concentration_pct']:.2f}% of portfolio"
    )


if __name__ == "__main__":
    main()
