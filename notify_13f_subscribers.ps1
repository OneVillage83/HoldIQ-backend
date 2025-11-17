# notify_13f_subscribers.ps1
# Run this after your 13F parsing step, or on a schedule.
# It:
#   1) Finds all (cik, latest_report_period, tiers) that have active subscribers
#   2) Generates briefs for each (cik, period, tier)
#   3) Calls send_brief_emails.py to email subscribers

Write-Host "📡 HoldIQ: notifying 13F subscribers..."

# 1) Ask SQLite which managers + periods + tiers we need to cover
$py_targets = @'
import sqlite3

DB_PATH = r".\data\holdiq.db"

con = sqlite3.connect(DB_PATH)

# Ensure subscribers table exists (in case script runs before manual SQL)
con.execute("""
CREATE TABLE IF NOT EXISTS subscribers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    cik   TEXT NOT NULL,
    tier  TEXT NOT NULL CHECK (tier IN ('nano','mini','premium')),
    active INTEGER NOT NULL DEFAULT 1,
    billing_provider TEXT,
    customer_id TEXT,
    subscription_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
)
""")
con.commit()

rows = con.execute("""
    SELECT s.cik,
           MAX(p.report_period) AS latest_period,
           GROUP_CONCAT(DISTINCT s.tier) AS tiers
    FROM subscribers s
    JOIN positions_13f p
      ON p.manager_cik = s.cik
    WHERE s.active = 1
      AND p.report_period IS NOT NULL
    GROUP BY s.cik
""").fetchall()

for cik, period, tiers in rows:
    if not period:
        continue
    print(f"{cik}|{period}|{tiers}")

con.close()
'@

# Run the inline Python and capture output
$targetsText = $py_targets | py -

if (-not $targetsText) {
    Write-Host "ℹ️ No active subscribers with parsed 13F positions found."
    exit 0
}

# Split into lines
$lines = $targetsText -split "`r?`n" | Where-Object { $_.Trim() -ne "" }

foreach ($line in $lines) {
    $parts = $line.Split('|')
    if ($parts.Count -lt 3) { continue }

    $cik    = $parts[0]
    $period = $parts[1]
    $tiersCsv = $parts[2]

    Write-Host "🔎 Processing CIK $cik @ period $period (tiers: $tiersCsv)"

    $tiers = $tiersCsv.Split(',')

    foreach ($tier in $tiers) {
        $tierTrim = $tier.Trim().ToLower()

        if ($tierTrim -eq "nano") {
            Write-Host "  🧠 Generating nano brief for $cik @ $period"
            py .\generate_ai_brief.py $cik $period nano
        }
        elseif ($tierTrim -eq "mini") {
            Write-Host "  🧠 Generating mini brief for $cik @ $period"
            py .\generate_ai_brief.py $cik $period mini
        }
        elseif ($tierTrim -eq "premium") {
            Write-Host "  🧠 Generating premium brief for $cik @ $period"
            py .\generate_ai_brief.py $cik $period premium
        }
        else {
            Write-Host "  ⚠️ Unknown tier '$tierTrim' for CIK $cik – skipping."
        }
    }
}

Write-Host "📨 Sending emails to subscribers..."
py .\send_brief_emails.py
Write-Host "✅ notify_13f_subscribers.ps1 complete."
