cd "C:\Users\user\Documents\GitHub\sqlite-hkex-express"

# Define log file path and script paths
$LogFile       = ".\temp\run-backup.log"

# Clear previous log file if it exists
if (Test-Path $LogFile) { Remove-Item $LogFile }

Write-Host "=== Starting backup ===" -ForegroundColor Cyan

# 1. Run Python Script
Write-Host "Running Python backup data" -ForegroundColor Yellow
cmd /c "py scripts/backup-db.py 2>&1" | Tee-Object -FilePath $LogFile -Append

Write-Host "=== Backup done. Log saved to $LogFile ===" -ForegroundColor Gree