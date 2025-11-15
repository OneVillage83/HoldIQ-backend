param(
  [int]$IntervalMinutes = 15,
  [string]$UserAgent = "HoldIQ Bot <info@holdiq.io> (Windows; PowerShell)",
  [string]$DbPath = ".\data\holdiq.db",
  [string]$OutRoot = ".\data\master",
  [switch]$Append
)

# ---------- CONFIG ----------
$ET_StartHour = 6    # 6:00 a.m. ET
$ET_EndHour   = 22   # 10:00 p.m. ET
$Table        = "filings"
$LockPath     = ".\data\.holdiq_realtime.lock"
$LogPath      = ".\data\auto_update_realtime.log"

# ---------- BOOTSTRAP ----------
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$null = New-Item -ItemType Directory -Path ".\data" -ErrorAction SilentlyContinue
$null = New-Item -ItemType Directory -Path $OutRoot -ErrorAction SilentlyContinue
$env:SEC_USER_AGENT = $UserAgent

function Write-Log([string]$msg, [string]$level="info") {
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $line = "$ts [$level] $msg"
  Write-Host $line
  Add-Content -Path $LogPath -Value $line
}

# === Robust EDGAR window helpers (replace old Get-NowEastern / Is-USFederalHoliday / In-EdgarWindow) ===

function Get-NowEastern {
  $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
  [System.TimeZoneInfo]::ConvertTimeFromUtc([DateTime]::UtcNow, $tz)
}

function Test-EdgarOpen {
  param([Parameter(Mandatory=$true)][datetime]$EtNow)

  # Window: 6:00–22:00 ET, Mon–Fri
  $open  = $EtNow.Date.AddHours(6)
  $close = $EtNow.Date.AddHours(22)

  $isBizDay = ($EtNow.DayOfWeek -ge [DayOfWeek]::Monday -and $EtNow.DayOfWeek -le [DayOfWeek]::Friday)

  # Minimal fixed-date holiday list (expand later if you want observed rules)
  $holidayMMDD = @('01-01','07-04','11-11','12-25')
  $isHoliday = $holidayMMDD -contains $EtNow.ToString('MM-dd')

  $isOpen = $isBizDay -and -not $isHoliday -and ($EtNow -ge $open) -and ($EtNow -lt $close)

  $sleepUntil = $null
  if (-not $isBizDay -or $isHoliday -or $EtNow -ge $close) {
    $sleepUntil = $EtNow.Date.AddDays(1).AddHours(6)
  } elseif ($EtNow -lt $open) {
    $sleepUntil = $open
  }

  [pscustomobject]@{
    IsOpen     = $isOpen
    EtNow      = $EtNow
    Open       = $open
    Close      = $close
    SleepUntil = $sleepUntil
  }
}

# === Use this where you previously called In-EdgarWindow ===
$status = Test-EdgarOpen -EtNow (Get-NowEastern)
if (-not $status.IsOpen) {
  $mins = 15
  if ($status.SleepUntil) {
    $delta = $status.SleepUntil - $status.EtNow  # scalar TimeSpan — no op_Subtraction bug
    if ($delta.TotalMinutes -gt 1) { $mins = [math]::Ceiling($delta.TotalMinutes) }
  }
  Write-Log ("Outside EDGAR window (now ET: {0}). Sleeping {1} min..." -f $status.EtNow.ToString('MM/dd/yyyy HH:mm:ss'), $mins)
  Start-Sleep -Seconds ($mins * 60)
  return
}

function Get-YearsToCheck {
  $nowET = Get-NowEastern
  $y = $nowET.Year
  # Also backfill prior year for early January/overhang
  return @($y, $y-1) | Sort-Object -Unique
}

function Ensure-NoOverlap {
  try {
    $fi = Get-Item -LiteralPath $LockPath -ErrorAction SilentlyContinue
    if ($fi) {
      # Force a single DateTime and compute elapsed minutes robustly
      $age     = [datetime]$fi.LastWriteTime
      $elapsed = (Get-Date) - $age
      if ($elapsed.TotalMinutes -lt ($IntervalMinutes * 2)) {
        Write-Log "Another run appears active (lock file present; age=$([int]$elapsed.TotalSeconds)s). Skipping this cycle." "warn"
        return $false
      }
      # Stale lock -> clean up
      Remove-Item -LiteralPath $LockPath -Force -ErrorAction SilentlyContinue
    }
    # Create fresh lock
    Set-Content -LiteralPath $LockPath -Value ([DateTime]::UtcNow.ToString("o")) -Encoding ASCII
    return $true
  } catch {
    Write-Log "Ensure-NoOverlap error: $($_.Exception.Message)" "error"
    return $false
  }
}

function Clear-Lock { Remove-Item $LockPath -ErrorAction SilentlyContinue }

# ---------- MAIN LOOP ----------
Write-Log "----- HoldIQ realtime updater started (interval=${IntervalMinutes}m) -----"

while ($true) {
  try {
    # Check if we’re inside the EDGAR filing window (6 AM – 10 PM ET)
$status = Test-EdgarOpen -EtNow (Get-NowEastern)
if (-not $status.IsOpen) {
  $mins = 15
  if ($status.SleepUntil) {
    $delta = $status.SleepUntil - $status.EtNow
    if ($delta.TotalMinutes -gt 1) { $mins = [math]::Ceiling($delta.TotalMinutes) }
  }
  Write-Log ("Outside EDGAR window (now ET: {0}). Sleeping {1} min..." -f $status.EtNow.ToString('MM/dd/yyyy HH:mm:ss'), $mins) "info"
  Start-Sleep -Seconds ($mins * 60)
  continue
}
    if (-not (Ensure-NoOverlap)) {
      Start-Sleep -Seconds ([Math]::Max(60, $IntervalMinutes*60))
      continue
    }

    $years = Get-YearsToCheck
    foreach ($y in $years) {
      $prefix = Join-Path $OutRoot ("edgar_{0}" -f $y)
      Write-Log "Scraping master index for year $y -> $prefix"
      py ".\edgar_all_forms_scraper.py" `
        --use-master-index `
        --all-forms `
        --slice-years $y `
        --out-prefix $prefix | Out-Host

      $csv = Join-Path $OutRoot ("edgar_{0}.csv" -f $y)
      if (Test-Path $csv) {
        Write-Log "UPSERT -> $csv -> $DbPath"
        py ".\load_csv_to_sqlite_upsert.py" $csv $DbPath $Table | Out-Host
      } else {
        Write-Log "Missing CSV for $y at $csv (skip load)" "warn"
      }
    }

    # Views + quick sanity
    Write-Log "Ensuring views"
    py ".\ensure_views_now.py" --db $DbPath | Out-Host

    Write-Log "Sanity snapshot"
    $py = @'
import sqlite3
con=sqlite3.connect(r".\data\holdiq.db")
tot = con.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
mx  = con.execute("SELECT MAX(filedAt) FROM filings").fetchone()[0]
print("Rows:", tot)
print("Max filedAt:", mx)
con.close()
'@
    $py | py - | Out-Host
    Write-Log "Cycle complete."
  } catch {
    Write-Log "ERROR: $($_.Exception.Message)" "error"
  } finally {
    Clear-Lock
  }

  Start-Sleep -Seconds ($IntervalMinutes*60)
}
