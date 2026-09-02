cd "C:\Users\user\Documents\GitHub\sqlite-hkex-express"
# 1. Force PowerShell internal output encoding to UTF-8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Define log file path and script paths
$LogFile       = ".\temp\run-scripts.log"

# Clear previous log file if it exists
if (Test-Path $LogFile) { Remove-Item $LogFile }

Write-Host "=== Starting Script Executions ===" -ForegroundColor Cyan

# 1. Run Python Script
Write-Host "Running Python script 01" -ForegroundColor Yellow
cmd /c "py scripts/dn-hkex-data.py 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "Running Python script 02" -ForegroundColor Yellow
cmd /c "py scripts/dn-yfinance-hk.py 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "Running Python script 03" -ForegroundColor Yellow
cmd /c "py scripts/proc-daily-price-hk.py 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "Running Python script 03" -ForegroundColor Yellow
cmd /c "py scripts/proc-statistics.py 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "Running Python script 04" -ForegroundColor Yellow
cmd /c "py scripts/to-gsheet.py 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "=== All scripts finished. Log saved to $LogFile ===" -ForegroundColor Green