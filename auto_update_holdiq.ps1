param(
  [int]$StartYear = (Get-Date).Year - 1,
  [int]$EndYear   = (Get-Date).Year,
  [string]$OutRoot   = ".\data\master",
  [string]$DbPath    = ".\data\holdiq.db",
  [string]$UserAgent = "HoldIQ Bot <info@holdiq.io> (Windows; PowerShell)",
  [switch]$Append,             # if omitted and DB exists, it will be deleted
  [int]$MaxRetries = 4,
  [int]$SleepMs    = 1500
)

# --------------------- setup ---------------------
$ErrorActionPreference = "Stop"
$ProgressPreference    = "SilentlyContinue"
if ($PSCommandPath) {
  $here = Split-Path -Path $PSCommandPath -Parent
} elseif ($MyInvocation.MyCommand.Path) {
  $here = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
} else {
  $here = Get-Location
}
Set-Location $here

$env:SEC_USER_AGENT = $UserAgent
$null = New-Item -ItemType Directory -Path ".\data" -ErrorAction SilentlyContinue
$null = New-Item -ItemType Directory -Path $OutRoot -ErrorAction SilentlyContinue

# Logging
$log = Join-Path ".\data" "auto_update.log"
function Log([string]$msg) {
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  "$ts $msg" | Tee-Object -FilePath $log -Append
}

Log "----- Start auto update (StartYear=$StartYear EndYear=$EndYear Append=$Append) -----"

# Remove DB unless Append switch is passed
if (-not $Append -and (Test-Path $DbPath)) {
  Log "[info] Removing existing DB: $DbPath"
  Remove-Item $DbPath -Force
}

function Invoke-WithRetry {
  param(
    [Parameter(Mandatory=$true)][scriptblock]$Script,
    [int]$Retries = 3,
    [int]$DelayMs = 1200,
    [string]$What = "operation"
  )
  for ($i=0; $i -le $Retries; $i++) {
    try {
      return & $Script
    } catch {
      if ($i -eq $Retries) { throw }
      Log "[warn] $What failed ($($i+1)/$($Retries+1)) - $($_.Exception.Message)"
      Start-Sleep -Milliseconds ([int]($DelayMs * [Math]::Pow(1.6, $i)))
    }
  }
}

function YearCsvPath([int]$y) { Join-Path $OutRoot ("edgar_{0}.csv" -f $y) }
function YearOutPrefix([int]$y) { Join-Path $OutRoot ("edgar_{0}" -f $y) }

# --------------------- phase 1: download per-year via master index ---------------------
for ($y = $StartYear; $y -le $EndYear; $y++) {
  $csvPath = YearCsvPath $y
  $outPref = YearOutPrefix $y

  if (Test-Path $csvPath) {
    Log "[skip] CSV exists -> $csvPath"
    continue
  }

  Log "[info] Scraping year $y"
  Invoke-WithRetry -Retries $MaxRetries -DelayMs $SleepMs -What "scrape $y" -Script {
    & py ".\edgar_all_forms_scraper.py" `
        --use-master-index `
        --all-forms `
        --slice-years $y `
        --out-prefix $outPref | ForEach-Object { Log $_ }
  }
}

# --------------------- phase 2: load into SQLite (upsert) ---------------------
for ($y = $StartYear; $y -le $EndYear; $y++) {
  $csv = YearCsvPath $y
  if (-not (Test-Path $csv)) {
    Log "[warn] Missing $csv - skipping load."
    continue
  }
  Log "[info] Loading year $y from $csv"
  Invoke-WithRetry -Retries $MaxRetries -DelayMs $SleepMs -What "load $y" -Script {
    & py ".\load_csv_to_sqlite_upsert.py" $csv $DbPath "filings" | ForEach-Object { Log $_ }
  }
}

# --------------------- phase 3: ensure indexes and views ---------------------
# If you have create_indexes.py, run it (optional)
if (Test-Path ".\create_indexes.py") {
  Log "[info] Creating indexes"
  Invoke-WithRetry -Retries 2 -DelayMs 800 -What "create indexes" -Script {
    & py ".\create_indexes.py" | ForEach-Object { Log $_ }
  }
}

# Ensure core views exist (uses the lightweight ensure script)
if (Test-Path ".\ensure_views_now.py") {
  Log "[info] Ensuring views"
  Invoke-WithRetry -Retries 2 -DelayMs 800 -What "ensure views" -Script {
    & py ".\ensure_views_now.py" --db $DbPath | ForEach-Object { Log $_ }
  }
} elseif (Test-Path ".\create_views_all.py") {
  Log "[info] Creating views (full)"
  Invoke-WithRetry -Retries 2 -DelayMs 800 -What "create views" -Script {
    & py ".\create_views_all.py" --db $DbPath | ForEach-Object { Log $_ }
  }
} else {
  Log "[warn] No views script found; skipping views creation"
}

# --------------------- phase 4: quick sanity snapshot ---------------------
Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [info] Sanity snapshot"
try {
  py ".\sanity_sql.py" 2>&1 | Tee-Object -FilePath ".\data\auto_update.log" -Append
} catch {
  Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [warn] Sanity check failed: $($_.Exception.Message)"
}

Log "----- Done auto update -----"
