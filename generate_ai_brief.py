# generate_ai_brief.py

import os
import json
import sqlite3
from datetime import datetime

from openai import OpenAI

DB_PATH = r".\data\holdiq.db"
client = OpenAI()  # uses OPENAI_API_KEY from env

def fetch_manager_snapshot(con, cik, report_period):
    cur = con.cursor()

    # 1) Manager name & basic filing info (from filings)
    mgr = cur.execute("""
        SELECT company, filedAt, formType
        FROM filings
        WHERE cik = ?
          AND reportPeriod = ?
          AND formType LIKE '13F%'
        ORDER BY filedAt DESC
        LIMIT 1
    """, (cik, report_period)).fetchone()

    manager_name = mgr[0] if mgr else None
    filed_at = mgr[1] if mgr else None
    form_type = mgr[2] if mgr else "13F-HR"

    # 2) Positions snapshot (current quarter)
    positions_rows = list(cur.execute("""
        SELECT cik, reportPeriod, ticker, companyName,
               value_usd, shares, weight_pct
        FROM positions_13f
        WHERE cik = ? AND reportPeriod = ?
        ORDER BY weight_pct DESC
        LIMIT 200
    """, (cik, report_period)))

    positions = [
        dict(zip(
            ["cik", "reportPeriod", "ticker", "companyName",
             "value_usd", "shares", "weight_pct"],
            row
        ))
        for row in positions_rows
    ]

    # --- Portfolio-level stats for richer AI commentary ---
    total_value = sum(p["value_usd"] or 0 for p in positions) if positions else 0
    n_positions = len(positions)

    positions_sorted = sorted(
        positions,
        key=lambda p: p["weight_pct"] or 0,
        reverse=True
    )
    top10 = positions_sorted[:10]
    top10_weight = sum(p["weight_pct"] or 0 for p in top10) if positions_sorted else 0
    largest_position = positions_sorted[0] if positions_sorted else None
    tiny_positions = [p for p in positions_sorted if (p["weight_pct"] or 0) < 0.1]

    portfolio_stats = {
        "total_value_usd": total_value,
        "n_positions": n_positions,
        "top10_weight_pct": top10_weight,
        "largest_position": largest_position,   # full dict
        "n_tiny_positions": len(tiny_positions),
    }

    # 3) Changes vs prior quarter (deltas), INCLUDING SHARES & VALUES
    deltas_rows = list(cur.execute("""
        SELECT
            delta_type,
            ticker,
            companyName,
            delta_value_usd,
            delta_weight_pct,
            old_weight_pct,
            new_weight_pct,
            old_shares,
            new_shares,
            delta_shares,
            old_value_usd,
            new_value_usd
        FROM positions_13f_delta
        WHERE cik = ? AND reportPeriod = ?
        ORDER BY
            CASE delta_type
                WHEN 'new' THEN 1
                WHEN 'increase' THEN 2
                WHEN 'decrease' THEN 3
                WHEN 'closed' THEN 4
                ELSE 5
            END,
            ABS(delta_value_usd) DESC
        LIMIT 200
    """, (cik, report_period)))

    deltas = [
        dict(zip(
            [
                "delta_type",
                "ticker",
                "companyName",
                "delta_value_usd",
                "delta_weight_pct",
                "old_weight_pct",
                "new_weight_pct",
                "old_shares",
                "new_shares",
                "delta_shares",
                "old_value_usd",
                "new_value_usd",
            ],
            row
        ))
        for row in deltas_rows
    ]

    # 4) Insider transactions
    insiders_rows = list(cur.execute("""
        SELECT insider_name, ticker, companyName, tx_type, shares, price, tx_date
        FROM insider_tx
        WHERE cik = ?
          AND tx_date >= date(?, '-90 day')
          AND tx_date <= date(?, '+30 day')
        ORDER BY tx_date DESC
        LIMIT 200
    """, (cik, report_period, report_period)))

    insiders = [
        dict(zip(
            ["insider_name", "ticker", "companyName", "tx_type", "shares", "price", "tx_date"],
            row
        ))
        for row in insiders_rows
    ]

    return {
        "cik": cik,
        "manager_name": manager_name,
        "reportPeriod": report_period,
        "filedAt": filed_at,
        "formType": form_type,
        "portfolio_stats": portfolio_stats,
        "positions": positions,
        "deltas": deltas,
        "insiders": insiders,
    }

def build_prompt(context):
    """
    Build the prompt given the structured context.
    The context includes:
      - portfolio_stats
      - positions
      - deltas  (with delta_value_usd, delta_weight_pct, old_weight_pct, new_weight_pct, etc.)
      - insiders
    """

    return f"""
You are HoldIQ, an AI analyst that explains institutional portfolios
and insider activity for retail investors in clear language.

You are given structured data for one institutional manager's 13F filing,
including positions, quarter-over-quarter changes, and insider transactions
for related tickers.

DATA (JSON):
{json.dumps(context, indent=2)}

PRIMARY GOAL:
Do ALL the interpretive work for the end user. They should not need
to calculate anything themselves. When you mention a ticker or a
position change, you must include specific numbers whenever they are
available in the JSON.

CONCRETE NUMERIC BEHAVIOR (VERY IMPORTANT):

- When describing a change for a specific ticker (e.g., AAPL, MSFT, UNH, CVS):
  - Use the numeric fields from the JSON such as:
    - delta_value_usd
    - delta_weight_pct
    - old_weight_pct, new_weight_pct
    - old_shares, new_shares, delta_shares (if present)
  - Express them in investor-friendly form, for example:
    - "BlackRock trimmed AAPL by about $520M, cutting it from 7.2% to 5.8% of the portfolio."
    - "They added roughly 2.3M shares of UNH, increasing its weight from 2.1% to 3.5%."
  - Do NOT invent numbers; only transform the numbers already in the JSON
    into rounded, readable figures.

- If data is missing for a certain dimension:
  - Use what is available (e.g., only dollar change or only weight change).
  - Do NOT fabricate missing fields.

- When talking about portfolio-level structure:
  - Use portfolio_stats (e.g., total_value_usd, n_positions, top10_weight_pct)
    and quote approximate figures like:
    - "The manager reported roughly $18.4B across 63 positions."
    - "The top 10 holdings account for about 58% of the disclosed portfolio."

TASKS:

1. Portfolio snapshot & style:
   - Describe the overall portfolio: how many positions, how big it is in USD,
     how concentrated it is in the top 10 holdings (with actual numbers).
   - Comment on the apparent style: concentrated vs diversified, growth vs value
     if reasonably inferable from the tickers/sectors (do not invent).
   - Identify the single largest position and its approximate share of the portfolio
     (e.g., "TSLA at ~9% of reported holdings").

2. Changes vs prior quarter (deltas):
   - List and explain the most important NEW positions (largest new allocations), with numbers.
   - List and explain the biggest ADDITIONS (increased stakes), with numbers.
   - List and explain the biggest TRIMS (reduced stakes), with numbers.
   - List positions that were FULLY EXITED, especially if they were previously large,
     and quantify the size they exited if you can (e.g., "previously ~3% position").

3. Thematic & sector insights:
   - Identify any sector or theme tilt (e.g., more tech, less healthcare,
     more defensives, more cyclicals) if visible from the top holdings.
   - Highlight clear rotations (e.g., out of mega-cap tech into financials),
     tying to the numeric changes where possible.

4. "Insider flavor" – connect insider activity to 13F moves:
   - For tickers where insider_tx data exists, explain whether insiders
     have been net buyers or sellers recently, including:
       - rough number of shares
       - direction (buy/sell)
       - general timing
   - Explain how insider activity aligns or conflicts
     with the manager's moves (e.g., manager buying while insiders are selling).
   - If there is little or no insider data, briefly state that.

5. Risk and opportunity framing:
   - RISKS: Identify portfolio-level risks (concentration, sector exposure,
     crowding into popular names, high volatility names, etc.).
   - OPPORTUNITIES: Identify potential upside themes (e.g., high conviction in a sector
     or company with a strong narrative), but do NOT make price predictions.

6. Output format:
   Produce a JSON object with this exact structure:

   {{
     "headline": "...",               // max ~120 characters
     "short_summary": "...",          // 2–3 sentences
     "long_summary": "...",           // 3–6 paragraphs, detailed but clear
     "bullets_free": ["...", "..."],  // 3–5 bullet points for free users
     "bullets_premium": ["...", "..."], // 5–10 more detailed bullets
     "risks": ["...", "..."],         // 3–5 key risk bullet points
     "opportunities": ["...", "..."]  // 3–5 opportunity bullet points
   }}

RULES:
- Use only information implied by the provided JSON.
- Do NOT invent positions, trades, or numbers.
- Always prefer precise, numeric statements over vague language when the data exists.
- Avoid explicit investment advice or price targets; focus on explanation and structure.
- Use plain-English, retail-friendly wording while remaining accurate.
"""

def call_model(prompt):
    # You can change model name as desired
    resp = client.chat.completions.create(
        model="gpt-5.1-mini",
        messages=[
            {"role": "system", "content": "You are a financial analyst AI called HoldIQ."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.4,
    )
    content = resp.choices[0].message.content
    usage = resp.usage

    # Ensure JSON parse
    data = json.loads(content)
    return data, usage, resp.model

def save_brief(con, cik, manager_name, report_period, form_type,
               brief_type, payload, model_name, usage):
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO ai_briefs
            (cik, manager_name, reportPeriod, formType, brief_type,
             json_payload, created_at, model_name, input_tokens, output_tokens)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?)
    """, (
        cik,
        manager_name,
        report_period,
        form_type,
        brief_type,
        json.dumps(payload),
        model_name,
        getattr(usage, "prompt_tokens", None),
        getattr(usage, "completion_tokens", None),
    ))
    con.commit()

def generate_brief_for_manager(cik, report_period):
    con = sqlite3.connect(DB_PATH)

    context = fetch_manager_snapshot(con, cik, report_period)

    if not context["positions"]:
        print(f"No positions_13f data for CIK {cik} / {report_period}")
        return

    prompt = build_prompt(context)
    payload, usage, model_name = call_model(prompt)

    save_brief(
        con,
        cik=context["cik"],
        manager_name=context["manager_name"],
        report_period=context["reportPeriod"],
        form_type="13F-HR",
        brief_type="13F-summary",
        payload=payload,
        model_name=model_name,
        usage=usage,
    )

    con.close()
    print(f"✅ Saved AI brief for CIK {cik} / {report_period}")

if __name__ == "__main__":
    # Example: hardcode for now; later you loop over all managers/periods
    import sys
    if len(sys.argv) != 3:
        print("Usage: python generate_ai_brief.py <cik> <reportPeriod-YYYY-MM-DD>")
        raise SystemExit(1)

    cik = sys.argv[1]
    report_period = sys.argv[2]
    generate_brief_for_manager(cik, report_period)
