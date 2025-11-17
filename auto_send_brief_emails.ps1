# auto_send_brief_emails.ps1
# Long-running loop to send HoldIQ brief emails every ~20 minutes
# during EDGAR hours.

param(
    [int]$IntervalSeconds = 1200  # 20 minutes
)

$ErrorActionPreference = "Stop"

# --- Helper: approximate EDGAR hours (09:30–16:00 ET, Mon–Fri) ---
function In-EdgarWindow {
    # Convert local time to Eastern Time roughly.
    # If your Windows time zone is already ET, this is just Get-Date.
    $nowLocal = Get-Date
    # Adjust this if you're not on US Eastern; you're in CA so:
    $nowET = $nowLocal.AddHours(3)

    # Monday=1 .. Sunday=7
    $dow = [int]$nowET.DayOfWeek
    if ($dow -lt 1 -or $dow -gt 5) { return $false }

    $hour = $nowET.Hour
    $minute = $nowET.Minute

    # 09:30 <= time <= 16:00 ET
    if ($hour -lt 9 -or $hour -gt 16) { return $false }
    if ($hour -eq 9 -and $minute -lt 30) { return $false }

    return $true
}

# --- Main loop ---

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$python = "py"
$sendScript = ".\send_brief_emails.py"

Write-Host "[email] Starting auto_send_brief_emails.ps1 (interval: $IntervalSeconds seconds)"

while ($true) {
    $now = Get-Date
    if (-not (Test-Path $sendScript)) {
        Write-Host "[email] $(Get-Date -Format 'u') send_brief_emails.py not found in $scriptDir"
        Start-Sleep -Seconds $IntervalSeconds
        continue
    }

    if (-not (In-EdgarWindow)) {
        Write-Host "[email] $(Get-Date -Format 'u') Outside EDGAR window. Sleeping..."
        Start-Sleep -Seconds $IntervalSeconds
        continue
    }

    Write-Host "[email] $(Get-Date -Format 'u') Running send_brief_emails.py..."
    try {
        & $python $sendScript
        Write-Host "[email] $(Get-Date -Format 'u') Finished send_brief_emails.py"
    }
    catch {
        Write-Host "[email] $(Get-Date -Format 'u') ERROR in send_brief_emails.py: $_"
    }

    Start-Sleep -Seconds $IntervalSeconds
}
