@echo on

for /f %%i in ('powershell -Command "Get-Date -Format 'yyyyMMdd'"') do set current_date=%%i
echo %current_date%
:: Output: 2026-07-26

sqlite3 .\hkex-market-breadth.db .dump > ../sql/backup.%current_date%.sql
7z a -v100m ../sql/backup.%current_date%.sql.zip ../sql/backup.%current_date%.sql
dir ../sql/backup.%current_date%.sql