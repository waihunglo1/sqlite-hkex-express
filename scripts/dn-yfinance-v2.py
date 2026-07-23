import yfinance as yf
import pandas as pd
from yahooquery import Ticker


def tickersFromXls():
    # Define converters to force the first 3 columns (by index 0, 1, 2) to string
    df = pd.read_excel(
        "/Users/user/Downloads/ListOfSecurities_c.xlsx",
        skiprows=2,
        usecols=[0, 1, 2],
        converters={0: str, 1: str, 2: str}
    )

    filterDf = df[df.iloc[:, 2].isin(['股本', '交易所買賣產品'])]

    # Clean and format the first column without lambda
    clean_codes = filterDf.iloc[:, 0].str.split('.').str[0].str.strip().str.lstrip('0')

    # Pad with leading zeros to 4 digits and add .HK
    filterDf.iloc[:, 0] = clean_codes.str.zfill(4) + '.HK'
    tickerList = filterDf.iloc[:, 0].tolist()
    return tickerList

def extractFromYFinance(tickerBatch, yfTickers, csvData):
    # Extract data locally from the pre-fetched tickers object
    for symbol in tickerBatch:
        try:
            info = yfTickers.tickers[symbol].info
            
            csvData.append({
                "Ticker": symbol,
                "Name": info.get("longName"),
                "Sector": info.get("sector"),
                "Market Cap": info.get("marketCap"),
                "Trailing P/E": info.get("trailingPE"),
                "Current Price": info.get("currentPrice")
            })
        except Exception as e:
            print(f"Skipping {symbol}: {e}")

def yahooQuery(tickerBatch):
    tickers = Ticker(tickerBatch, country='hong kong')

    # 1. 獲取數據
    # Fetch the raw data dictionaries
    summary = tickers.summary_detail
    quote = tickers.quotes
    print(len(summary), len(quote))

    # 使用迴圈遍歷並列印每個股票的公司名稱
    for symbol in tickerBatch:
        try:
            # print(quote[symbol])
            # print(summary[symbol])
            stock_name = quote[symbol]['longName']
            # industry = quote[symbol]['industry']
            # sector = quote[symbol]['sector']
            print(f"{symbol}: {stock_name}")
        except KeyError:
            print(f"{symbol}: 無法獲取資料")

#
# Main Program
# 

tickerList = tickersFromXls()
print(len(tickerList))

# Split ticker_list into batches of 100 items
csvData = []
batch_size = 100
for i in range(0, len(tickerList), batch_size):
    tickerBatch = tickerList[i : i + batch_size]
    yahooQuery(tickerBatch)

# Export to CSV
df = pd.DataFrame(csvData)
df.to_csv("c:/temp/optimized_batch_summary.csv", index=False)
print("Data saved to optimized_batch_summary.csv")