cd "C:\Users\user\Documents\GitHub\sqlite-hkex-express"

# Define log file path and script paths
$LogFile       = ".\temp\run-backup.log"

# Clear previous log file if it exists
if (Test-Path $LogFile) { Remove-Item $LogFile }

Write-Host "=== Starting backup ===" -ForegroundColor Cyan

# 1. Run Python Script
Write-Host "Running Python backup data" -ForegroundColor Yellow
copy data/*market-sqlite.db 'G:\My Drive\backup\sqlite-hkex-express-data'

Write-Host "=== Backup done. Log saved to $LogFile ===" -ForegroundColor Gree