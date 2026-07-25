import yfinance as yf
import pandas as pd
from yahooquery import Ticker
import configparser
import sqlite3

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
    # summary = tickers.summary_detail
    profile_data = tickers.asset_profile
    quote = tickers.quotes
    # print(len(profile_data), len(quote))

    records = []
    for symbol in tickerBatch:
        try:
            records.append({
                'symbol': symbol,
                'name'  : quote[symbol]['longName'],
                'sector': profile_data[symbol]['sector'],
                'industry': profile_data[symbol]['industry'],
                'marketCap' : quote[symbol]['marketCap']
            })
            
        except:
            errorRecords.append(
                {
                    'symbol': symbol
                }
            )
            # print(f"{symbol}: 無法獲取資料")

    # update sqlite
    if len(records) > 0:
        updated = insertOrReplace(records)
        print(f"Changed Row: {updated} data size: {len(records)}")

def insertOrReplace(records):
    df = pd.DataFrame(records)
    print(df.to_markdown(index=False))

    updated = 0
    try:
        with sqlite3.connect(sqliteFile, timeout=10) as conn:
            cursor = conn.cursor()
        
            # Use SQLite "INSERT OR REPLACE" logic row-by-row
            for _, row in df.iterrows():
                cursor.execute('''
                    REPLACE INTO STOCK (symbol,name,industry,sector,market_cap) VALUES (?, ?, ?, ?, ?)
                ''', (row['symbol'], row['name'], row['industry'], row['sector'], row['marketCap']))
                # 📜 獲取受影響的行數
                updated += cursor.rowcount
            
            conn.commit()
    except sqlite3.Error as e:
        print(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
        exit
    except Exception as e:
        print(f"❌ ⚪ 未知錯誤: {e}")
        exit

    return updated
#
# Main Program
# 

# Initialize the parser
config = configparser.ConfigParser()
config.read('config/analyst-data.ini', encoding='utf-8')
sqliteFile = config['SQLITE']['FILE']
print(f"SQLITE : {sqliteFile}") 

# read xls
tickerList = tickersFromXls()
print(f"SIZE : {len(tickerList)}")

# Split ticker_list into batches of 100 items
errorRecords = []
batch_size = 100
for i in range(0, len(tickerList), batch_size):
    tickerBatch = tickerList[i : i + batch_size]
    yahooQuery(tickerBatch)

if len(errorRecords) > 0:
    df = pd.DataFrame(errorRecords)
    print(df.to_markdown(index=False))  