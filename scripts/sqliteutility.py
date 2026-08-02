
import sqlite3
import logging
import sys

def insertOrReplaceStockInfo(sqliteFile, df):
    df["symbol"] = df["symbol"].str.replace("/", "-", regex=False)
    updated = 0
    try:
        with sqlite3.connect(sqliteFile, timeout=10) as conn:
            cursor = conn.cursor()
        
            # Use SQLite "INSERT OR REPLACE" logic row-by-row
            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT INTO STOCK (symbol, name, sector, market_cap) 
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        market_cap = EXCLUDED.market_cap
                ''', (row['symbol'], row['name'], row['industry'], row['marketCap']))
                updated += cursor.rowcount
            
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
        sys.exit()
    except Exception as e:
        logging.error(f"❌ ⚪ 未知錯誤: {e}")
        sys.exit()

    return updated

def fetchTickers(sqliteFile, whereClause):
    try:
        with sqlite3.connect(sqliteFile, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT symbol FROM stock WHERE " + whereClause + " order by symbol")

            # 4. Retrieve and print the results
            symbols = [row[0] for row in cursor.fetchall()]

            if not symbols:
                logging.info("沒有需要更新的股票。")
            return symbols
    except sqlite3.Error as e:
        logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
        sys.exit()
    except Exception as e:
        logging.error(f"❌ ⚪ 未知錯誤: {e}")
        sys.exit()      