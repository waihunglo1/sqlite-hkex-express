from yahooquery import Ticker
import pandas as pd

tickers = Ticker(['AAPL', 'MSFT', 'GOOGL', 'AMZN'])

# 1. 獲取數據
raw_data = tickers.summary_detail

# 2. 強制將 dict 轉換為 DataFrame，並轉置 (Transpose) 讓欄位對齊
if isinstance(raw_data, dict):
    summary_df = pd.DataFrame.from_dict(raw_data, orient='index')
else:
    summary_df = raw_data

# 3. 此時就可以安全地篩選欄位
print(summary_df[['marketCap', 'trailingPE', 'fiftyTwoWeekHigh']])

# 4. 匯出 CSV
summary_df[['marketCap', 'trailingPE', 'fiftyTwoWeekHigh']].to_csv("yahooquery_batch.csv")
