# run_send_brief_emails.ps1
# One-shot script: send any new HoldIQ briefs via email.

$ErrorActionPreference = "Stop"

# Root of your project
$root = "C:\Users\OneVi\Desktop\Data Scraper\HoldIQ Scraper"
Set-Location $root

# Use the venv's Python directly so we don't need to "activate" anything
$python = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    Write-Host "❌ Python not found at $python"
    exit 1
}

if (-not (Test-Path ".\send_brief_emails.py")) {
    Write-Host "❌ send_brief_emails.py not found in $root"
    exit 1
}

Write-Host ("[email] {0:u} Running send_brief_emails.py..." -f (Get-Date))

& $python ".\send_brief_emails.py"

Write-Host ("[email] {0:u} Finished send_brief_emails.py" -f (Get-Date))
