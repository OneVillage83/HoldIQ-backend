#!/usr/bin/env python3
r"""
Enrich a CSV of master-index filings with:
- accessionNo (derived from filingUrl if present)
- indexJsonUrl (per-filing folder index.json)
- filingDetailUrl (folder URL)
- ticker_enriched (from SEC company_tickers.json)
- primaryDoc_from_index (best guess of primary doc from index.json)

Usage:
  py enrich_holdiq.py .\data\holdiq_2024_tiers.csv .\data\holdiq_2024_tiers_enriched.csv
"""

import csv, json, os, re, sys, time, urllib.request

UA = os.environ.get("SEC_USER_AGENT", "HoldIQ Bot <info@holdiq.io>")

def http_get_json(url, sleep=0.4, retries=5):
    last = None
    for i in range(retries+1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/json",
                "Referer": "https://www.sec.gov/",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8", "replace"))
        except Exception as e:
            last = e
            if i == retries:
                raise
            time.sleep(sleep * (1.5**i))
    raise last  # not reached

def derive_accession_parts(filing_url: str):
    # ex: https://www.sec.gov/Archives/edgar/data/320193/0000320193-24-000010/form10k.htm
    m = re.search(r"/data/(\d+)/([0-9\-]+)/", filing_url or "")
    if not m:
        return None, None
    acc_dash = m.group(2)
    acc_nodash = acc_dash.replace("-", "")
    return acc_dash, acc_nodash

def load_cik_ticker_map():
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        d = http_get_json(url)
        out = {}
        for _, rec in d.items():
            out[str(rec.get("cik_str"))] = rec.get("ticker")
        return out
    except Exception:
        return {}

def main(in_csv, out_csv):
    cik2ticker = load_cik_ticker_map()

    with open(in_csv, "r", encoding="utf-8", newline="") as inf:
        r = csv.DictReader(inf)
        base_fields = r.fieldnames or []
        # Only add extras that are NOT already present
        extras = [
            "accessionNo",           # may already exist; we avoid dup
            "indexJsonUrl",
            "filingDetailUrl",
            "ticker_enriched",
            "primaryDoc_from_index",
        ]
        add_these = [f for f in extras if f not in base_fields]
        fieldnames = base_fields + add_these

        with open(out_csv, "w", encoding="utf-8", newline="") as outf:
            w = csv.DictWriter(outf, fieldnames=fieldnames)
            w.writeheader()

            for row in r:
                filing_url = row.get("filingUrl") or ""
                acc_dash, acc_nodash = derive_accession_parts(filing_url)

                # accessionNo: only fill if column exists and row is empty (don’t duplicate header!)
                if "accessionNo" in fieldnames:
                    if not row.get("accessionNo") and acc_dash:
                        row["accessionNo"] = acc_dash

                # Enrichment columns (safe even if not in base)
                if "indexJsonUrl" in fieldnames:
                    row["indexJsonUrl"] = (
                        f"https://www.sec.gov/Archives/edgar/data/{(row.get('cik') or '').lstrip('0')}/{acc_nodash}/index.json"
                        if acc_nodash and row.get("cik") else ""
                    )
                if "filingDetailUrl" in fieldnames:
                    row["filingDetailUrl"] = (
                        f"https://www.sec.gov/Archives/edgar/data/{(row.get('cik') or '').lstrip('0')}/{acc_nodash}/"
                        if acc_nodash and row.get("cik") else ""
                    )
                if "ticker_enriched" in fieldnames:
                    cik_str = (row.get("cik") or "").lstrip("0")
                    row["ticker_enriched"] = cik2ticker.get(cik_str) or row.get("ticker") or ""

                # primary document guess from index.json
                primary_from_index = ""
                if acc_nodash and row.get("cik") and "indexJsonUrl" in fieldnames and row["indexJsonUrl"]:
                    try:
                        j = http_get_json(row["indexJsonUrl"])
                        items = (j.get("directory") or {}).get("item") or []
                        for itm in items:
                            name = (itm or {}).get("name", "").lower()
                            if name.endswith((".htm", ".html", ".txt")):
                                primary_from_index = (itm or {}).get("name", "")
                                break
                    except Exception:
                        pass
                if "primaryDoc_from_index" in fieldnames:
                    row["primaryDoc_from_index"] = primary_from_index

                w.writerow({k: row.get(k, "") for k in fieldnames})

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: py enrich_holdiq.py <input.csv> <output.csv>")
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
