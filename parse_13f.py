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
from urllib.request import urlopen

def fetch_text(url):
    """Download a filing and return its text bytes."""
    with urlopen(url) as r:
        return r.read()

def find_13f_xml(blob: bytes) -> bytes:
    """Extract the <informationTable> block from .txt filings."""
    m = re.search(br'(<informationTable\b.*?</informationTable>)', blob, re.S | re.I)
    return m.group(1) if m else blob

def parse_and_upsert(con, acc, url, manager_cik, period):
    raw = fetch_text(url)
    xml = find_13f_xml(raw)
    root = ET.fromstring(xml)

    ns = {}
    if root.tag.startswith("{"):
        nsuri = root.tag.split("}")[0].strip("{")
        ns = {"n": nsuri}

    rows = []
    for it in root.findall(".//n:infoTable", ns) + root.findall(".//infoTable"):
        def g(tag):
            return (it.findtext(f"n:{tag}", namespaces=ns) or it.findtext(tag) or "").strip()

        issuer = g("nameOfIssuer")
        clss = g("titleOfClass")
        cusip = g("cusip")
        val = float(g("value") or 0) * 1000.0  # 13F values are in $ thousands
        sh = float(it.findtext("n:shrsOrPrnAmt/n:sshPrnamt", namespaces=ns) or
                   it.findtext("shrsOrPrnAmt/sshPrnamt") or 0)
        putc = g("putCall")
        disc = g("investmentDiscretion")
        vsole = int(g("votingAuthority/n:Sole") or it.findtext("votingAuthority/Sole") or 0)
        vshrd = int(g("votingAuthority/n:Shared") or it.findtext("votingAuthority/Shared") or 0)
        vnone = int(g("votingAuthority/n:None") or it.findtext("votingAuthority/None") or 0)

        rows.append((manager_cik, period, cusip, issuer, clss, sh, val, putc, disc, vsole, vshrd, vnone))

    with con:
        con.execute("DELETE FROM positions_13f WHERE manager_cik=? AND report_period=?", (manager_cik, period))
        con.executemany("""
          INSERT OR REPLACE INTO positions_13f
          (manager_cik, report_period, cusip, issuer, class, shares, value_usd,
           put_call, discretion, voting_sole, voting_shared, voting_none)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        con.execute("""
          INSERT OR REPLACE INTO filings_parsed(accessionNo, formType, parsed_at, succeeded, err)
          VALUES(?,?,datetime('now'),1,NULL)
        """, (acc, '13F-HR'))

def main():
    db = r".\data\holdiq.db"
    con = sqlite3.connect(db)

    # grab one pending 13F from queue
    row = con.execute("""
      SELECT accessionNo, formType, filingUrl
      FROM parse_queue
      WHERE formType IN ('13F-HR', '13F-HR/A')
      ORDER BY enqueued_at
      LIMIT 1
    """).fetchone()
    if not row:
        print("✅ No pending 13F filings to parse.")
        return

    acc, ftype, url = row
    cik, period = con.execute("SELECT cik, reportPeriod FROM filings WHERE accessionNo=?", (acc,)).fetchone()

    print(f"Parsing 13F: {acc} | {url}")
    try:
        parse_and_upsert(con, acc, url, cik, period)
        con.execute("DELETE FROM parse_queue WHERE accessionNo=?", (acc,))
        con.commit()
        print("✅ Parse complete.")
    except Exception as e:
        print(f"❌ Error parsing {acc}: {e}")
        con.execute("INSERT OR REPLACE INTO filings_parsed(accessionNo, formType, parsed_at, succeeded, err) VALUES(?,?,datetime('now'),0,?)",
                    (acc, ftype, str(e)))
    finally:
        con.close()

if __name__ == "__main__":
    main()
