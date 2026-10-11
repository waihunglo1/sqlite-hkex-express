cd "C:\Users\user\Documents\GitHub\sqlite-hkex-express"
# 1. Force PowerShell internal output encoding to UTF-8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Define log file path and script paths
$LogFile       = ".\temp\run-scripts-us.log"

# Clear previous log file if it exists
if (Test-Path $LogFile) { Remove-Item $LogFile }

# Helper function to run a command, log output, and stop on error
function Validate-PythonScript {
    param (
        [string]$Description
    )

    if ($LASTEXITCODE -ne 0) {
        Write-Host "$Description Script failed with exit code $exitCode" -ForegroundColor Red
        exit $exitCode
    }
}

Write-Host "=== Starting Script Executions ===" -ForegroundColor Cyan

# 1. Run Python Script
Write-Host "Running Python script 01" -ForegroundColor Yellow
cmd /c "py scripts/dn-yf-stock-info.py --market us  2>&1" | Tee-Object -FilePath $LogFile -Append
Validate-PythonScript -Description "Running Python script 01"

Write-Host "Running Python script 02" -ForegroundColor Yellow
cmd /c "py scripts/proc-daily-price.py --market us 2>&1" | Tee-Object -FilePath $LogFile -Append
Validate-PythonScript -Description "Running Python script 02"

Write-Host "Running Python script 03" -ForegroundColor Yellow
cmd /c "py scripts/dn-stockchart-rs.py --market us 2>&1" | Tee-Object -FilePath $LogFile -Append
Validate-PythonScript -Description "Running Python script 03"

Write-Host "Running Python script 04" -ForegroundColor Yellow
cmd /c "py scripts/proc-statistics.py --market us 2>&1" | Tee-Object -FilePath $LogFile -Append
Validate-PythonScript -Description "Running Python script 04"

Write-Host "=== All scripts finished. Log saved to $LogFile ===" -ForegroundColor Green