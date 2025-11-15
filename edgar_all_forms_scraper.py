#!/usr/bin/env python3
"""
EDGAR Full-Text Search scraper (resilient, offline-friendly)

- Primary path: EFTS search-index (POST JSON) -> rich hits
- Fallback path: master.idx archive (GET text) -> robust, no-token edge cases
- Outputs: <out-prefix>.jsonl, <out-prefix>.csv, <out-prefix>.checkpoint.json
"""

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
import urllib.request
import urllib.error

# ---------- Environment / Constants ----------

# Optional SSL import (some environments lack it)
try:
    import ssl as _ssl  # type: ignore
    HAS_SSL = True
except Exception:
    _ssl = None
    HAS_SSL = False

HAS_HTTPS_HANDLER = hasattr(urllib.request, "HTTPSHandler")
SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
MASTER_URL_TMPL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/master.idx"
DEFAULT_SIZE = 200
PF_FORMS = {"PF", "PF-ADV", "PF-NR", "PF-R"}

# Curated "tiers"
TIER_FORMS = [
    "13F-HR", "13F-NT", "13FCONP", "13F-E",
    "NPORT-P", "NPORT-EX", "NPORT-N", "NPORT-CR",
    "N-CSR", "N-CSRS", "N-30D", "N-Q",
    "485BPOS", "485APOS", "497", "497K", "497H2",
    "10-K", "10-K/A", "10-Q", "10-Q/A", "8-K",
    "20-F", "6-K", "40-F",
    "S-1", "S-3", "S-4", "S-8",
    "SC TO-C", "SC TO-I", "SC TO-T",
    "3", "4", "5",
    "DEF 14A", "DEFA14A", "DEFM14A", "DFAN14A",
    "POS AM", "POS EX", "POSASR", "POS 8C",
    "ADV", "ADV-E", "ADV-H-T", "ADV-NR",
    "ABS-EE", "ABS-15G", "SD",
    "F-1", "F-1MEF", "F-2", "F-3", "F-3ASR", "F-3D", "F-3DPOS",
    "F-4", "F-4 POS", "F-4EF", "F-4MEF",
    "F-6", "F-6 POS", "F-6EF",
    "F-7", "F-8", "F-8 POS",
    "F-9", "F-9 POS", "F-9EF", "F-9MEF",
    "F-10", "F-10EF", "F-10MEF", "F-10POS",
]

# ---------- Types ----------

@dataclass
class Checkpoint:
    query_hash: str
    from_index: int = 0
    seen: int = 0

# ---------- Capability helpers ----------

def _https_supported() -> bool:
    return bool(HAS_SSL and HAS_HTTPS_HANDLER and _ssl is not None)

def _build_opener(user_agent: str) -> urllib.request.OpenerDirector:
    handlers: List[urllib.request.BaseHandler] = []
    if _https_supported():
        try:
            ctx = _ssl.create_default_context()  # type: ignore[arg-type]
            handlers.append(urllib.request.HTTPSHandler(context=ctx))
        except Exception:
            pass
    try:
        handlers.append(urllib.request.HTTPHandler())
    except Exception:
        pass
    opener = urllib.request.build_opener(*handlers)
    opener.addheaders = [("User-Agent", user_agent), ("Accept", "application/json")]
    return opener

# ---------- EFTS (POST JSON) ----------

def http_post_json(url: str, payload: Dict, user_agent: str, retries: int, sleep: float,
                   offline: bool, require_network: bool) -> Dict:
    if offline:
        return {"hits": {"hits": []}}

    if not _https_supported():
        if require_network:
            raise RuntimeError("HTTPS not supported. Install OpenSSL or run with --offline.")
        return {"hits": {"hits": []}}

    data = json.dumps(payload).encode("utf-8")
    opener = _build_opener(user_agent)
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, data=data, method="POST")
            # Attach critical headers directly
            req.add_header("User-Agent", user_agent)
            req.add_header("Content-Type", "application/json")
            req.add_header("Accept", "application/json")
            req.add_header("Origin", "https://efts.sec.gov")
            req.add_header("Referer", "https://efts.sec.gov/")
            req.add_header("X-Requested-With", "XMLHttpRequest")

            with opener.open(req, timeout=60) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8"))

        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = "<no body>"
            sys.stderr.write(f"[http] HTTPError {e.code} {e.reason} — attempt {attempt+1}/{retries+1}\n{body}\n")
            last_err = e
            time.sleep(sleep * (1.5 ** attempt))

        except Exception as e:
            last_err = e
            time.sleep(sleep * (1.5 ** attempt))

    raise RuntimeError(f"HTTP POST failed after {retries} retries: {last_err}")

def build_payload(form_types: Optional[List[str]], from_index: int, size: int,
                  startdt: Optional[str] = None, enddt: Optional[str] = None) -> Dict:
    payload: Dict = {
        "keys": ["formType"],
        "category": "custom",
        "from": from_index,
        "size": size,
        "sort": [{"filedAt": {"order": "desc"}}],
    }
    if form_types:
        payload["forms"] = form_types
    if startdt and enddt:
        payload["dateRange"] = "custom"
        payload["startdt"] = startdt
        payload["enddt"] = enddt
    else:
        payload["dateRange"] = "all"
    return payload

def normalize_row(hit: Dict) -> Dict:
    c = hit.get("_source", {})
    cik = str(c.get("cik", "")).lstrip("0")
    acc_no = c.get("adsh") or c.get("accessionNo") or ""
    acc_no_nodash = acc_no.replace("-", "")
    primary_doc = c.get("primaryDocument") or c.get("primaryDocDescription") or ""
    filing_url = (
        c.get("linkToFilingDetails")
        or c.get("linkToHtml")
        or (f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_nodash}/{primary_doc}"
            if cik and acc_no_nodash and primary_doc else "")
    )
    return {
        "cik": c.get("cik"),
        "ticker": c.get("ticker"),
        "company": c.get("displayNames") or c.get("name"),
        "formType": c.get("formType"),
        "filedAt": c.get("filedAt"),
        "reportPeriod": c.get("periodOfReport") or c.get("reportDate"),
        "accessionNo": acc_no,
        "primaryDocument": primary_doc,
        "filingUrl": filing_url,
        "size": c.get("size"),
    }

# ---------- Master Index (GET text) ----------

def _http_get(url: str, user_agent: str, timeout: int = 60) -> str:
    req = urllib.request.Request(url, method="GET")
    req.add_header("User-Agent", user_agent)
    req.add_header("Accept", "text/plain")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def iter_master_index_rows(years: List[str], forms_filter: Optional[List[str]], user_agent: str):
    """
    Yields normalized rows from master.idx files for the given years (all 4 quarters).
    master.idx format: CIK|Company Name|Form Type|Date Filed|Filename
    """
    forms_set = set(f.strip() for f in (forms_filter or [])) if forms_filter else None

    for y in years:
        for q in (1, 2, 3, 4):
            url = MASTER_URL_TMPL.format(year=y, q=q)
            try:
                sys.stderr.write(f"[info] Fetching {url}\n")
                txt = _http_get(url, user_agent)
            except Exception as e:
                sys.stderr.write(f"[warn] Failed to fetch {url}: {e}\n")
                continue

            lines = txt.splitlines()
            # Skip until dashed separator line
            sep_idx = -1
            for i, ln in enumerate(lines):
                if ln.startswith("-----"):
                    sep_idx = i
                    break
            if sep_idx < 0:
                sys.stderr.write(f"[warn] Unexpected format for {url} (no separator)\n")
                continue

            for ln in lines[sep_idx+1:]:
                parts = ln.split("|")
                if len(parts) != 5:
                    continue
                cik, company, form_type, filed_at, filename = parts
                if forms_set and form_type not in forms_set:
                    continue
                yield {
                    "cik": cik.lstrip("0") or cik,
                    "ticker": None,
                    "company": company,
                    "formType": form_type,
                    "filedAt": filed_at,
                    "reportPeriod": None,
                    "accessionNo": "",
                    "primaryDocument": filename.rsplit("/", 1)[-1],
                    "filingUrl": f"https://www.sec.gov/Archives/{filename}",
                    "size": None,
                }

# ---------- IO ----------

def write_outputs(rows: List[Dict], jsonl_path: str, csv_path: str, csv_header_written: bool) -> bool:
    # JSONL
    with open(jsonl_path, "a", encoding="utf-8") as jf:
        for r in rows:
            jf.write(json.dumps(r, ensure_ascii=False) + "\n")

    # CSV
    fieldnames = [
        "cik", "ticker", "company", "formType", "filedAt", "reportPeriod",
        "accessionNo", "primaryDocument", "filingUrl", "size"
    ]
    with open(csv_path, "a", newline="", encoding="utf-8") as cf:
        w = csv.DictWriter(cf, fieldnames=fieldnames)
        if not csv_header_written:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})
    return True

def hash_query(forms: Optional[List[str]], startdt: Optional[str], enddt: Optional[str]) -> str:
    s = json.dumps({"forms": forms or "ALL", "startdt": startdt, "enddt": enddt}, sort_keys=True)
    return str(abs(hash(s)))

# ---------- Args ----------

def parse_args_safe(argv: Optional[List[str]] = None):
    argv = list(argv or sys.argv[1:])
    if "--run-tests" in argv:
        return argparse.Namespace(run_tests=True)
    if "--diagnostics" in argv:
        return argparse.Namespace(diagnostics=True)

    parser = argparse.ArgumentParser(description="EDGAR scraper")
    g_forms = parser.add_mutually_exclusive_group()
    g_forms.add_argument("--forms", choices=["tiers"])
    g_forms.add_argument("--forms-file")
    g_forms.add_argument("--all-forms", action="store_true")

    parser.add_argument("--out-prefix")
    parser.add_argument("--user-agent")
    parser.add_argument("--slice-years", nargs="*")
    parser.add_argument("--sleep", type=float, default=0.8)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--page-size", type=int, default=DEFAULT_SIZE)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--require-network", action="store_true")
    parser.add_argument(
        "--use-master-index",
        action="store_true",
        help="Use the public master.idx archive (GET) instead of EFTS API (POST)."
    )

    if len(argv) == 0:
        out_prefix = os.environ.get("EDGAR_OUT_PREFIX", "data/holdiq_demo")
        user_agent = os.environ.get("SEC_USER_AGENT", "HoldIQ Dev <dev@example.com>")
        if "dev@example.com" in user_agent:
            sys.stderr.write("[warn] Using placeholder User-Agent. Set SEC_USER_AGENT env to comply with SEC policy.\n")
        offline = False
        if not _https_supported():
            offline = True
            sys.stderr.write("[info] HTTPS not available — switching to OFFLINE mode. Use --require-network to fail instead.\n")
        sys.stderr.write(f"[info] No CLI args provided — running demo with out-prefix '{out_prefix}' and forms='tiers'.\n")
        return argparse.Namespace(
            forms="tiers", forms_file=None, all_forms=False, out_prefix=out_prefix,
            user_agent=user_agent, slice_years=None, sleep=0.8, max_retries=5,
            page_size=DEFAULT_SIZE, offline=offline, require_network=False,
            run_tests=False, diagnostics=False, use_master_index=False
        )

    args = parser.parse_args(argv)
    if not getattr(args, "user_agent", None):
        env_ua = os.environ.get("SEC_USER_AGENT")
        if env_ua:
            args.user_agent = env_ua
        else:
            parser.error("--user-agent is required (or set SEC_USER_AGENT env)")
    if not getattr(args, "out_prefix", None):
        env_out = os.environ.get("EDGAR_OUT_PREFIX")
        if env_out:
            args.out_prefix = env_out
        else:
            parser.error("--out-prefix is required (or set EDGAR_OUT_PREFIX env)")

    if not _https_supported() and not args.require_network:
        sys.stderr.write("[info] HTTPS not available — switching to OFFLINE mode. Use --require-network to fail instead.\n")
        args.offline = True

    args.diagnostics = False
    return args

# ---------- Main ----------

def run_master_index_flow(years: List[str], forms: Optional[List[str]],
                          jsonl_path: str, csv_path: str,
                          user_agent: str) -> int:
    csv_header_written = os.path.exists(csv_path)
    total_written_global = 0
    batch: List[Dict] = []
    batch_size = 1000
    for row in iter_master_index_rows(years, forms, user_agent):
        batch.append(row)
        if len(batch) >= batch_size:
            write_outputs(batch, jsonl_path, csv_path, csv_header_written)
            csv_header_written = True
            total_written_global += len(batch)
            batch.clear()
    if batch:
        write_outputs(batch, jsonl_path, csv_path, csv_header_written)
        total_written_global += len(batch)
    return total_written_global

def main(argv: Optional[List[str]] = None):
    args = parse_args_safe(argv)
    if getattr(args, "run_tests", False):
        return run_tests()
    if getattr(args, "diagnostics", False):
        print(json.dumps({
            "https_supported": _https_supported(),
            "has_ssl": HAS_SSL,
            "has_https_handler": HAS_HTTPS_HANDLER,
            "user_agent": os.environ.get("SEC_USER_AGENT", "<unset>"),
        }, indent=2))
        return

    # Decide form set
    if args.all_forms:
        forms = None
    elif args.forms == "tiers":
        forms = TIER_FORMS
    elif args.forms_file:
        with open(args.forms_file, "r", encoding="utf-8") as f:
            raw = f.read()
        parts = [x.strip() for x in raw.replace("\n", ",").split(",")]
        forms = [x for x in parts if x]
    else:
        forms = TIER_FORMS

    # Remove confidential PF forms if present
    if forms:
        forbidden = PF_FORMS.intersection(forms)
        if forbidden:
            sys.stderr.write(f"[warn] Skipping confidential PF forms: {sorted(forbidden)}\n")
            forms = [f for f in forms if f not in PF_FORMS]

    jsonl_path = f"{args.out_prefix}.jsonl"
    csv_path = f"{args.out_prefix}.csv"
    ckpt_path = f"{args.out_prefix}.checkpoint.json"
    os.makedirs(os.path.dirname(jsonl_path) or ".", exist_ok=True)

    # If user asked for master-index explicitly
    if args.use_master_index:
        years = args.slice_years or []
        if not years:
            sys.stderr.write("[info] --use-master-index requires --slice-years (e.g., 2024 2025)\n")
            sys.exit(2)
        total = run_master_index_flow(years, forms, jsonl_path, csv_path, args.user_agent)
        print(f"Slice {','.join(years)} via master.idx done -> {total} rows written so far")
        print(f"Done (LIVE). JSONL: {jsonl_path}  CSV: {csv_path}  (total rows this run: {total})")
        return

    # Otherwise try EFTS, paging; on 403 fallback to master-index if years provided
    slices: List[Tuple[Optional[str], Optional[str]]] = []
    if args.slice_years:
        for y in args.slice_years:
            slices.append((f"{y}-01-01", f"{y}-12-31"))
    else:
        slices.append((None, None))  # all-time

    csv_header_written = os.path.exists(csv_path)
    total_written_global = 0
    try:
        for (startdt, enddt) in slices:
            qhash = hash_query(forms, startdt, enddt)
            ckpt = Checkpoint(query_hash=qhash)
            if os.path.exists(ckpt_path):
                try:
                    prev = json.load(open(ckpt_path, "r", encoding="utf-8"))
                    if prev.get("query_hash") == qhash:
                        ckpt = Checkpoint(**prev)
                except Exception:
                    pass

            from_idx = ckpt.from_index
            total_written = ckpt.seen
            while True:
                payload = build_payload(forms, from_idx, args.page_size, startdt, enddt)
                resp = http_post_json(SEARCH_URL, payload, args.user_agent, args.max_retries, args.sleep, args.offline, args.require_network)
                hits = resp.get("hits", {}).get("hits", [])
                if not hits:
                    break
                rows = [normalize_row(h) for h in hits]
                write_outputs(rows, jsonl_path, csv_path, csv_header_written)
                csv_header_written = True
                from_idx += len(hits)
                total_written += len(rows)
                total_written_global += len(rows)
                with open(ckpt_path, "w", encoding="utf-8") as f:
                    json.dump(asdict(Checkpoint(qhash, from_idx, total_written)), f)
                time.sleep(args.sleep)
            print(f"Slice {startdt or 'ALL'}..{enddt or 'ALL'} done -> {total_written} rows written so far")

        mode = "OFFLINE" if args.offline or not _https_supported() else "LIVE"
        print(f"Done ({mode}). JSONL: {jsonl_path}  CSV: {csv_path}  (total rows this run: {total_written_global})")
        return

    except RuntimeError as e:
        msg = str(e)
        # Automatic fallback on 403 if years available
        if "403" in msg and args.slice_years:
            sys.stderr.write("[info] EFTS returned 403 — falling back to master.idx for given --slice-years.\n")
            total = run_master_index_flow(args.slice_years, forms, jsonl_path, csv_path, args.user_agent)
            print(f"Slice {','.join(args.slice_years)} via master.idx done -> {total} rows written so far")
            print(f"Done (LIVE). JSONL: {jsonl_path}  CSV: {csv_path}  (total rows this run: {total})")
            return
        # Otherwise bubble up
        raise

# ---------- Tests (no network) ----------

def run_tests():
    # Existing test preserved
    sample = {
        "_source": {
            "cik": "0001067983",
            "formType": "10-K",
            "filedAt": "2024-03-01T12:00:00Z",
            "accessionNo": "0001067983-24-000012",
            "primaryDocument": "form10k.htm",
            "displayNames": "Sample Co",
        }
    }
    row = normalize_row(sample)
    assert row["filingUrl"].startswith(
        "https://www.sec.gov/Archives/edgar/data/1067983/000106798324000012/form10k.htm"
    ), row["filingUrl"]

    # Additional light tests (no network)
    p = build_payload(["10-K"], 0, 50)
    assert p["dateRange"] == "all" and p["forms"] == ["10-K"] and p["from"] == 0 and p["size"] == 50

    h1 = hash_query(["10-K"], None, None)
    h2 = hash_query(["10-Q"], None, None)
    assert h1 != h2

    print("All tests passed.")

if __name__ == "__main__":
    if "--run-tests" in sys.argv:
        run_tests()
    else:
        main()
