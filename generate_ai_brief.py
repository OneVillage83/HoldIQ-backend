import json
import os
import sqlite3
import sys
from typing import Optional, Tuple

from openai import OpenAI

DB_PATH = r".\data\holdiq.db"
OUT_DIR = r".\out"
SNAPSHOT_DIR = OUT_DIR  # snapshots like snapshot_{cik}_{period}.json live here


# -------------------- DB HELPERS --------------------


def ensure_ai_briefs_table(con: sqlite3.Connection) -> None:
    """
    Ensure the ai_briefs table exists with a unique row per (manager_cik, report_period, model).
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_briefs (
            manager_cik TEXT NOT NULL,
            report_period TEXT NOT NULL,
            model       TEXT NOT NULL,
            brief_md    TEXT NOT NULL,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (manager_cik, report_period, model)
        )
        """
    )
    con.commit()


def get_cached_brief(
    con: sqlite3.Connection, cik: str, period: str, model_label: str
) -> Optional[str]:
    """
    Return cached brief text for (cik, period, model_label) if present.
    """
    row = con.execute(
        """
        SELECT brief_md
        FROM ai_briefs
        WHERE manager_cik = ?
          AND report_period = ?
          AND model = ?
        """,
        (cik, period, model_label),
    ).fetchone()

    return row[0] if row else None


def cache_brief(
    con: sqlite3.Connection, cik: str, period: str, model_label: str, brief_md: str
) -> None:
    """
    Upsert brief text into ai_briefs.
    """
    con.execute(
        """
        INSERT OR REPLACE INTO ai_briefs
            (manager_cik, report_period, model, brief_md, created_at)
        VALUES
            (?, ?, ?, ?, datetime('now'))
        """,
        (cik, period, model_label, brief_md),
    )
    con.commit()


def get_latest_period(con: sqlite3.Connection, cik: str) -> Optional[str]:
    """
    Get the most recent report_period in positions_13f for this manager.
    """
    row = con.execute(
        """
        SELECT report_period
        FROM positions_13f
        WHERE manager_cik = ?
          AND report_period IS NOT NULL
          AND report_period <> ''
        ORDER BY report_period DESC
        LIMIT 1
        """,
        (cik,),
    ).fetchone()

    return row[0] if row else None


# -------------------- SNAPSHOT I/O --------------------


def load_snapshot(cik: str, period: str) -> dict:
    """
    Load precomputed JSON snapshot: snapshot_{cik}_{period}.json
    This file is created by fetch_manager_snapshot.py.
    """
    path = os.path.join(SNAPSHOT_DIR, f"snapshot_{cik}_{period}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Snapshot file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -------------------- MODEL / TIER RESOLUTION --------------------


def resolve_model(mode: Optional[str]) -> Tuple[str, str, str]:
    """
    Map a user 'mode' string to (model_name, model_label, tier):

    - nano/fast      -> gpt-5-nano   (tier='nano')
    - standard/mini  -> gpt-5-mini   (tier='mini')
    - premium/pro/5.1-> gpt-5.1      (tier='premium')

    If mode is None or unknown, default to nano.
    """
    if not mode:
        return "gpt-5-nano", "gpt-5-nano", "nano"

    m = mode.lower()
    if m in ("fast", "nano"):
        return "gpt-5-nano", "gpt-5-nano", "nano"
    if m in ("standard", "mini"):
        return "gpt-5-mini", "gpt-5-mini", "mini"
    if m in ("premium", "pro", "5.1", "gpt-5.1"):
        return "gpt-5.1", "gpt-5.1", "premium"

    # Fallback
    return "gpt-5-nano", "gpt-5-nano", "nano"


# -------------------- PROMPT BUILDER --------------------


def build_prompt_from_snapshot(snapshot: dict, tier: str) -> str:
    """
    Build the appropriate prompt template (nano, mini, premium)
    using the snapshot dict.
    """

    # We'll embed the snapshot as JSON so the model sees full context.
    snapshot_str = json.dumps(snapshot, ensure_ascii=False)

    if tier == "nano":
        prompt = f"""
You are an AI that generates concise, factual portfolio summaries using only the data provided.

Portfolio Data (JSON):
{snapshot_str}

Produce a clear, bullet-based summary with:
- Total number of holdings
- Total portfolio value
- Top 5 holdings with:
   • ticker
   • issuer name
   • weight (%) 
   • market value ($)

- Top 3 sectors by total value
- One-sentence risk summary

Format strictly like this:

Portfolio Summary
-----------------
• Total Value: $X  
• Total Positions: N  
• Top Holdings:
   1. TICKER – WEIGHT% (~$VALUE)
   2. …
   3. …
   4. …
   5. …

Sector Mix
----------
• Sector – %  
• Sector – %  
• Sector – %  

Risk Note
---------
• Single bullet on the portfolio’s main concentration risk.
"""
        return prompt.strip()

    if tier == "mini":
        prompt = f"""
You are a portfolio analyst producing a professional but concise portfolio brief for a financial intelligence platform.

Portfolio Data (JSON):
{snapshot_str}

Generate:

1) Overview (2–3 sentences)
   - Describe style, diversification, sector bias, and concentration.

2) Top Holdings Table
   - Top 5 holdings with ticker, issuer, weight %, and market value.

3) Sector Allocation
   - Top 3 sectors by total weight or value.

4) Key Insights (3 bullets)
   - Focus on concentration risk, notable exposures, and performance drivers.

5) What This Means for Investors (2 bullets)
   - Keep it actionable and practical.

Tone: concise, factual, professional. Use section headers.
"""
        return prompt.strip()

    # premium
    prompt = f"""
You are a senior institutional equity strategist creating a high-depth research brief based solely on the provided holdings dataset.

Portfolio Data (JSON):
{snapshot_str}

Deliver:

1. Executive Summary (3–4 sentences)
   - Portfolio style, concentration profile, risk drivers, and factor tilts.

2. Top Holdings Analysis
   - Top 10 holdings with ticker, issuer, weight %, and market value.
   - Commentary on how these holdings define portfolio behavior.

3. Sector & Factor Exposure
   - Top 5 sectors with approximate weights.
   - Discuss style tilts: growth/value, large/small cap, AI/semiconductor exposure, defensives vs cyclicals.

4. Concentration & Risk Assessment
   - Concentration metrics and dependency on mega-cap leadership.
   - Sensitivity to sector/factor rotation.
   - 2–3 concrete risk scenarios.

5. Suggested Investor Interpretation
   - What the positioning implies.
   - What to monitor going forward.

Tone: authoritative, analytical, data-driven.
Use clear section headers. Do not hallucinate data—rely only on the JSON snapshot.
"""
    return prompt.strip()


# -------------------- OPENAI CALL --------------------


def generate_brief_text(snapshot: dict, model: str, tier: str) -> str:
    """
    Call OpenAI Chat Completions API and return the brief text.
    """
    client = OpenAI()

    prompt = build_prompt_from_snapshot(snapshot, tier)

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You are a meticulous institutional equity analyst.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
    )

    text = resp.choices[0].message.content or ""
    return text.strip()


# -------------------- CLI PARSING --------------------


def parse_args(argv: list[str]) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Return (cik, period, mode).

    Allowed call patterns:
        py generate_ai_brief.py CIK
        py generate_ai_brief.py CIK PERIOD
        py generate_ai_brief.py CIK PERIOD MODE
        py generate_ai_brief.py CIK MODE
    """
    if len(argv) < 2:
        print("Usage: py generate_ai_brief.py CIK [PERIOD] [MODE]")
        sys.exit(1)

    cik = argv[1]

    if len(argv) == 2:
        # Only CIK
        return cik, None, None

    if len(argv) == 3:
        # Could be (CIK, MODE) OR (CIK, PERIOD)
        a2 = argv[2].lower()
        if a2 in ("standard", "mini", "fast", "nano", "premium", "pro", "5.1", "gpt-5.1"):
            return cik, None, a2
        # Treat as (CIK, PERIOD)
        return cik, argv[2], None

    # len(argv) >= 4 -> treat as (CIK, PERIOD, MODE)
    return cik, argv[2], argv[3]


# -------------------- MAIN --------------------


def main() -> None:
    cik, period, mode = parse_args(sys.argv)
    model, model_label, tier = resolve_model(mode)

    if "OPENAI_API_KEY" not in os.environ:
        print("❌ OPENAI_API_KEY not found in environment.")
        return

    os.makedirs(OUT_DIR, exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    ensure_ai_briefs_table(con)

    # If no period provided, use most recent
    if not period:
        period = get_latest_period(con, cik)
        if not period:
            print(f"❌ No report_period found for CIK {cik} in positions_13f.")
            con.close()
            return
        print(f"ℹ️ Using latest report_period for {cik}: {period}")

    print(f"Manager {cik} | period={period} | model={model_label} (tier={tier})")

    # Check cache
    cached = get_cached_brief(con, cik, period, model_label)
    if cached:
        print("✅ Using cached brief from ai_briefs.")
        brief = cached
    else:
        # Load snapshot
        snapshot = load_snapshot(cik, period)

        print("🧠 Calling OpenAI Chat Completions API...")
        try:
            brief = generate_brief_text(snapshot, model, tier)
        except Exception as e:
            print(f"❌ Error generating brief: {e}")
            con.close()
            return

        cache_brief(con, cik, period, model_label, brief)
        print("✅ Cached brief in ai_briefs.")

    # Write to Markdown file
    safe_model = model_label.replace(".", "_")
    out_path = os.path.join(OUT_DIR, f"brief_{cik}_{period}_{safe_model}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(brief)

    print(f"✅ Wrote brief to {out_path}\n")
    print("--- Preview ---\n")
    print(brief[:1000])

    con.close()


if __name__ == "__main__":
    main()
