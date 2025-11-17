# parse_13f.py
"""
Parses new 13F filings from SEC EDGAR into structured holdings data.

Triggered automatically when new 13F filings appear in the filings table.
Stores normalized holdings in positions_13f.
"""

import re
import sqlite3
import sys
import xml.etree.ElementTree as ET
import urllib.request
import urllib.error
import gzip
import zlib

# IMPORTANT: put a real contact email here so SEC is happy.
USER_AGENT = "HoldIQ/1.0 (contact: your-email@example.com)"


def fetch_text(url: str) -> bytes:
    """
    Download a filing and return its raw *decompressed* bytes.

    Handles gzip/deflate compression used by SEC EDGAR.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Connection": "close",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        enc = (r.headers.get("Content-Encoding") or "").lower()

    # Decompress when necessary
    if "gzip" in enc:
        raw = gzip.decompress(raw)
    elif "deflate" in enc:
        try:
            raw = zlib.decompress(raw)
        except zlib.error:
            # Fallback for raw deflate streams
            raw = zlib.decompress(raw, -zlib.MAX_WBITS)

    return raw


def find_13f_xml(blob: bytes):
    """
    Return only the <informationTable> XML if present.
    If not present, return None so caller can gracefully skip this filing.
    """
    m = re.search(br'(<informationTable\b.*?</informationTable>)', blob, re.S | re.I)
    return m.group(1) if m else None


def parse_and_upsert(con, acc, url, manager_cik, period):
    """
    Download a single 13F filing, extract the holdings, and upsert into positions_13f.
    Also records success in filings_parsed.
    """
    raw = fetch_text(url)
    xml = find_13f_xml(raw)

    if xml is None:
        # Old/legacy filing with no XML informationTable
        raise ValueError("No <informationTable> XML found in filing (non-XML/legacy format)")

    # Parse XML
    root = ET.fromstring(xml)

    # Handle optional namespace
    ns = {}
    if root.tag.startswith("{"):
        nsuri = root.tag.split("}")[0].strip("{")
        ns = {"n": nsuri}

    rows = []

    info_tables = root.findall(".//n:infoTable", ns) + root.findall(".//infoTable")
    for it in info_tables:
        def g(tag: str) -> str:
            return (it.findtext(f"n:{tag}", namespaces=ns) or it.findtext(tag) or "").strip()

        issuer = g("nameOfIssuer")
        clss = g("titleOfClass")
        cusip = g("cusip")

        # 13F 'value' is in thousands of dollars
        val = float(g("value") or 0) * 1000.0

        sh = float(
            it.findtext("n:shrsOrPrnAmt/n:sshPrnamt", namespaces=ns)
            or it.findtext("shrsOrPrnAmt/sshPrnamt")
            or 0
        )

        putc = g("putCall")
        disc = g("investmentDiscretion")

        # Voting authority, namespaced or not
        vsole = int(
            it.findtext("n:votingAuthority/n:Sole", namespaces=ns)
            or it.findtext("votingAuthority/Sole")
            or 0
        )
        vshrd = int(
            it.findtext("n:votingAuthority/n:Shared", namespaces=ns)
            or it.findtext("votingAuthority/Shared")
            or 0
        )
        vnone = int(
            it.findtext("n:votingAuthority/n:None", namespaces=ns)
            or it.findtext("votingAuthority/None")
            or 0
        )

        rows.append(
            (
                manager_cik,
                period,
                cusip,
                issuer,
                clss,
                sh,
                val,
                putc,
                disc,
                vsole,
                vshrd,
                vnone,
            )
        )

    # Upsert into positions_13f and mark filings_parsed success
    with con:
        con.execute(
            "DELETE FROM positions_13f WHERE manager_cik=? AND report_period=?",
            (manager_cik, period),
        )
        con.executemany(
            """
            INSERT OR REPLACE INTO positions_13f
            (manager_cik, report_period, cusip, issuer, class, shares, value_usd,
             put_call, discretion, voting_sole, voting_shared, voting_none)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        con.execute(
            """
            INSERT OR REPLACE INTO filings_parsed
            (accessionNo, formType, parsed_at, succeeded, err)
            VALUES (?,?,datetime('now'),1,NULL)
            """,
            (acc, "13F-HR"),
        )


def main():
    db = r".\data\holdiq.db"
    con = sqlite3.connect(db)

    # Grab one pending 13F from queue
    row = con.execute(
        """
        SELECT accessionNo, formType, filingUrl
        FROM parse_queue
        WHERE formType IN ('13F-HR', '13F-HR/A')
        ORDER BY enqueued_at
        LIMIT 1
        """
    ).fetchone()

    if not row:
        print("✅ No pending 13F filings to parse.")
        return

    acc, ftype, url = row

    # Get CIK, reportPeriod, and filedAt
    cik, period, filed_at = con.execute(
        "SELECT cik, reportPeriod, filedAt FROM filings WHERE accessionNo=?",
        (acc,),
    ).fetchone()

    # If reportPeriod is empty/null, fall back to filedAt as the effective period
    if not period:
        period = filed_at

    print(f"Parsing 13F: {acc} | {url} | period={period}")
    try:
        parse_and_upsert(con, acc, url, cik, period)
        con.execute("DELETE FROM parse_queue WHERE accessionNo=?", (acc,))
        con.commit()
        print("✅ Parse complete.")
    except Exception as e:
        print(f"❌ Error parsing {acc}: {e}")
        # Record failure
        con.execute(
            """
            INSERT OR REPLACE INTO filings_parsed
            (accessionNo, formType, parsed_at, succeeded, err)
            VALUES (?,?,datetime('now'),0,?)
            """,
            (acc, ftype, str(e)),
        )
        # Remove this filing from the queue so we don't get stuck on it
        con.execute("DELETE FROM parse_queue WHERE accessionNo=?", (acc,))
        con.commit()
    finally:
        con.close()


if __name__ == "__main__":
    main()
