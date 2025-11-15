# build_positions_delta.py
import sqlite3, math

con = sqlite3.connect(r".\data\holdiq.db")
c = con.cursor()
# All (manager, period) pairs we have snapshots for
pairs = c.execute("""SELECT DISTINCT manager_cik, report_period FROM positions_13f""").fetchall()

for cik, per in pairs:
    # previous quarter
    prev = c.execute("""SELECT report_period FROM positions_13f
                        WHERE manager_cik=? AND report_period < ?
                        ORDER BY report_period DESC LIMIT 1""",(cik, per)).fetchone()
    prev = prev[0] if prev else None

    # current portfolio and ranking by value
    cur = c.execute("""SELECT cusip, issuer, shares, value_usd FROM positions_13f
                       WHERE manager_cik=? AND report_period=? ORDER BY value_usd DESC""",(cik, per)).fetchall()
    tot_val = sum(v for _,_,__,v in cur) or 1.0
    rank = {cusip:i+1 for i,(cusip,*,__,___) in enumerate(cur)}

    prev_map = {}
    if prev:
        prev_map = {cusip:(sh,val) for cusip,sh,val in c.execute(
            "SELECT cusip, shares, value_usd FROM positions_13f WHERE manager_cik=? AND report_period=?",(cik,prev)
        )}

    rows = []
    for cusip, issuer, sh, val in cur:
        if cusip not in prev_map:
            action = "NEW"
            sh_prev = val_prev = 0.0
        else:
            sh_prev, val_prev = prev_map[cusip]
            if sh == sh_prev: action = "UNCHANGED"
            elif sh > sh_prev: action = "INCREASE"
            else: action = "DECREASE"
        rows.append((cik, per, cusip, issuer, action, sh_prev, sh, val_prev, val,
                     sh - sh_prev, val - val_prev, val/tot_val, rank.get(cusip, None)))

    # SOLD_OUT rows (exist in prev, not in current)
    for cusip, (sh_prev, val_prev) in prev_map.items():
        if cusip not in {r[2] for r in rows}:
            rows.append((cik, per, cusip, None, "SOLD_OUT", sh_prev, 0.0, val_prev, 0.0, -sh_prev, -val_prev, 0.0, None))

    with con:
        con.execute("DELETE FROM positions_13f_delta WHERE manager_cik=? AND report_period=?", (cik, per))
        con.executemany("""INSERT OR REPLACE INTO positions_13f_delta
          (manager_cik,report_period,cusip,issuer,action,shares_prev,shares_now,value_prev,value_now,shares_delta,value_delta,pct_of_port,rank_in_port)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)

con.close()
