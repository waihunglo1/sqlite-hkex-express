import duckdb
import logging
import sys
import pandas as pd

class DuckDbHelper:
    """Helper class for managing SQLite database operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.duckConn = duckdb.connect(db_path)

    def insertOrReplaceStockInfo(self, df):
        df["symbol"] = df["symbol"].str.replace("/", "-", regex=False)

        # Clean/map DataFrame columns to match table schema
        # (Handling missing columns safely with default values)
        data_df = pd.DataFrame({
            "symbol": df["symbol"],
            "name": df.get("name", ""),
            "sector": df.get("industry", ""),
            "market_cap": df.get("marketCap", None),
        })

        try:
            # Connect to DuckDB database file
            with duckdb.connect(self.db_path) as conn:
                # Execute batch UPSERT directly from the in-memory Pandas DataFrame 'data_df'
                cursor = conn.execute(
                    """
                    INSERT INTO STOCK (symbol, name, sector, market_cap) 
                    SELECT symbol, name, sector, market_cap FROM data_df
                    ON CONFLICT(symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        market_cap = EXCLUDED.market_cap
                """
                )

                # DuckDB's rowcount returns the total affected/updated rows
                updated = cursor.rowcount if cursor.rowcount != -1 else len(data_df)

        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated

    def fetchTickers(self, whereClause):
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT symbol FROM stock WHERE " + whereClause + " order by symbol")

                # 4. Retrieve and print the results
                symbols = [row[0] for row in cursor.fetchall()]

                if not symbols:
                    logging.info("沒有需要更新的股票。")
                return symbols
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit()
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)    

    def insertOrReplaceHistPrice(self, hist):    
        hist.rename(columns={'symbol': 'ticker'}, inplace=True)
        hist['date'] = (
            pd.to_datetime(hist['date'], utc=True)
            .dt.strftime('%Y%m%d')
        )
        required_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adjclose', 'volume']
        for col in required_columns:
            if col not in hist.columns:
                hist[col] = None
                
        hist_final = hist[required_columns]

        static_sql = """
            INSERT OR REPLACE INTO daily_stock_price 
            (symbol, period, dt, open, high, low, close, adj_close, volume, open_int) 
            VALUES (?, 'D', ?, ?, ?, ?, ?, ?, ?, 0)
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
                data_tuples = list(hist_final.itertuples(index=False, name=None))
                cursor.executemany(static_sql, data_tuples)
                conn.commit()
                logging.info(f"✅ 成功將 {len(hist_final)} 筆歷史數據以 100% 靜態安全語法更新至資料庫。")

                return len(data_tuples)
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ 寫入資料庫時出錯: {e}")

        return 0 
    
    def insertOrReplaceStockInfo2(self, records) -> int:
        df = pd.DataFrame(records)
        # logging.info("\n" + df.to_markdown(index=False).rstrip)

        updated = 0
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
            
                # Use SQLite "INSERT OR REPLACE" logic row-by-row
                for _, row in df.iterrows():
                    cursor.execute('''
                        INSERT INTO STOCK (symbol, name, industry, sector, market_cap,industry_en,sector_en) 
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(symbol) DO UPDATE SET
                                name = EXCLUDED.name,
                                industry = EXCLUDED.industry,
                                sector = EXCLUDED.sector,
                                market_cap = EXCLUDED.market_cap,   
                                industry_en = EXCLUDED.industry_en,
                                sector_en = EXCLUDED.sector_en
                    ''', (
                        row['symbol'], row['name'], row['industry'], 
                        row['sector'], row['marketCap'],
                        row['industry_en'],row['sector_en']
                        )

                    )
                    # 📜 獲取受影響的行數
                    updated += cursor.rowcount
                
                conn.commit()
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            exit
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            exit

        return updated              