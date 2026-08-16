echo on
cd C:\Users\user\Documents\GitHub\sqlite-hkex-express
py scripts/dn-hkex-data.py
py scripts/dn-yfinance-hk.py
npm run lpv2
py scripts/populate-gspread.py