param([switch]$Append = $true)

# Robust script folder resolution:
# - $PSScriptRoot works when running as a file
# - $PSCommandPath is a fallback
# - Get-Location lets it still work when pasted into a console
$here = if ($PSScriptRoot) {
  $PSScriptRoot
} elseif ($PSCommandPath) {
  Split-Path -LiteralPath $PSCommandPath -Parent
} else {
  (Get-Location).Path
}

Set-Location -LiteralPath $here

# Ensure log directory exists
$logDir = Join-Path $here "data"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$log = Join-Path $logDir "auto_update_realtime.log"

function Log($lvl, $msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  $line = "$ts [$lvl] $msg"
  if ($Append) { $line | Out-File -Append -FilePath $log -Encoding utf8 }
  else { Write-Host $line }
}

try {
  Log "info" "--- EOD maintenance start ---"

  # ---- Python sanity via stdin ----
  $py = @'
import sqlite3
con = sqlite3.connect(r".\data\holdiq.db")
print("Rows:", con.execute("SELECT COUNT(*) FROM filings").fetchone()[0])
print("Max filedAt:", con.execute("SELECT MAX(filedAt) FROM filings").fetchone()[0])
con.close()
'@
  $py | py -

  # ---- Rebuild indexes and views ----
  py ".\create_indexes.py"
  py ".\ensure_views_now.py" --db ".\data\holdiq.db"

  # ---- Maintenance & compaction ----
  $py2 = @'
import sqlite3
con = sqlite3.connect(r".\data\holdiq.db")
con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
con.execute("VACUUM")
con.execute("REINDEX")
con.close()
print("Compaction done")
'@
  $py2 | py -

  Log "info" "--- EOD maintenance done ---"
}
catch {
  Log "error" ($_.Exception.Message)
  throw
}
