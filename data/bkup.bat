@echo on
sqlite3 .\hkex-market-breadth.db .dump > ../sql/backup.20260715.sql
7z a -v100m ../sql/backup.20260715.sql.zip ../sql/backup.20260715.sql