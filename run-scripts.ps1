cd "C:\Users\user\Documents\GitHub\sqlite-hkex-express"

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

# 2. Run Node.js Script
Write-Host "Running Node.js script..." -ForegroundColor Yellow
cmd /c "npm run lpv2 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "Running Python script 03" -ForegroundColor Yellow
cmd /c "py scripts/populate-gspread.py 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "=== All scripts finished. Log saved to $LogFile ===" -ForegroundColor Gree