echo on
cd C:\Users\user\Documents\GitHub\sqlite-hkex-express
py scripts/dn-hkex-data.py
py scripts/dn-yfinance-hk.py
py scripts/populate-data.py
npm run lpv2