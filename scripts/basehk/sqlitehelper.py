
import sqlite3
import logging
import sys
import yfinance as yf
import pandas as pd
import sqlite3
import os
import pandas as pd
import time
from yahooquery import Ticker
from pathlib import Path
from common.basedbhelper import BaseDbHelper

class SqliteDbHelper(BaseDbHelper):
    """Helper class for managing SQLite database operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def insertDailyStockPrice(self, prices: list[dict]) -> int:
        if not prices:
            return 0

        sql = """
            INSERT OR REPLACE INTO DAILY_STOCK_PRICE (
                symbol, period, dt, tm, open, high, low, close, volume, adj_close, open_int
            ) VALUES (
                :symbol, :period, :dt, :tm, :open, :high, :low, :close, :volume, :adj_close, :open_int
            )
        """

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, prices)
            return cursor.rowcount  # Returns the total affected rows

    def insertOrReplaceStockInfo(self, records) -> int:
        df = pd.DataFrame(records)
        # logging.info("\n" + df.to_markdown(index=False).rstrip)

        updated = 0
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
            
                # Use SQLite "INSERT OR REPLACE" logic row-by-row
                for _, row in df.iterrows():
                    cursor.execute('''
                        REPLACE INTO STOCK (symbol,name,industry,sector,market_cap,industry_en,sector_en,quote_type) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row['symbol'], row['name'], row['industry'], 
                        row['sector'], row['marketCap'],
                        row['industry_en'], row['sector_en'],
                        row['quoteType']
                        )

                    )
                    # 📜 獲取受影響的行數
                    updated += cursor.rowcount
                
                conn.commit()
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            exit
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            exit

        return updated   

    def fetchAllRows(self, sql: str, params: tuple | list | dict = ()):
        cleaned = sql.replace("\r", " ").replace("\n", " ")
        try:
            # Start high-precision timer
            # start_time = time.perf_counter()

            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                # logging.info(f"Start running sql: {cleaned[:30]}..{cleaned[-20:]}")
                cursor = conn.execute(cleaned, params)
                rows = cursor.fetchall()  

                # Calculate elapsed time in milliseconds
                # elapsed_ms = (time.perf_counter() - start_time) * 1000

                # logging.info(
                #    f"⏱️ [{elapsed_ms:.2f} ms] ({len(rows)} rows) | SQL: {cleaned[:30]}..{cleaned[-20:]}"
                # )
                return rows         
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)

    def insertOrReplaceSectorRecords(self, records) -> int:
        df = pd.DataFrame(records)
        # logging.info("\n" + df.to_markdown(index=False).rstrip)

        updated = 0
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
            
                # Use SQLite "INSERT OR REPLACE" logic row-by-row
                for _, row in df.iterrows():
                    cursor.execute('''
                        REPLACE INTO DAILY_SECTORS_STATS 
                            (dt, 
                            XLB_U4SM,  XLB_D4SM,  XLB_SM, 
                            XLC_U4SM,  XLC_D4SM,  XLC_SM, 
                            XLY_U4SM,  XLY_D4SM,  XLY_SM, 
                            XLP_U4SM,  XLP_D4SM,  XLP_SM, 
                            XLE_U4SM,  XLE_D4SM,  XLE_SM, 
                            XLF_U4SM,  XLF_D4SM,  XLF_SM, 
                            XLV_U4SM,  XLV_D4SM,  XLV_SM, 
                            XLI_U4SM,  XLI_D4SM,  XLI_SM, 
                            XLRE_U4SM, XLRE_D4SM, XLRE_SM, 
                            XLK_U4SM,  XLK_D4SM,  XLK_SM, 
                            XLU_U4SM,  XLU_D4SM,  XLU_SM, 
                            XLX_U4SM,  XLX_D4SM,  XLX_SM       
                            ) 
                            VALUES (?,
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?, 
                            ?, ?, ?                              
                            )
                    ''', (
                        row["dt"],
                        row["XLB_U4SM"],
                        row["XLB_D4SM"],
                        row["XLB_SM"],
                        row["XLC_U4SM"],
                        row["XLC_D4SM"],
                        row["XLC_SM"],
                        row["XLY_U4SM"],
                        row["XLY_D4SM"],
                        row["XLY_SM"],
                        row["XLP_U4SM"],
                        row["XLP_D4SM"],
                        row["XLP_SM"],
                        row["XLE_U4SM"],
                        row["XLE_D4SM"],
                        row["XLE_SM"],
                        row["XLF_U4SM"],
                        row["XLF_D4SM"],
                        row["XLF_SM"],
                        row["XLV_U4SM"],
                        row["XLV_D4SM"],
                        row["XLV_SM"],
                        row["XLI_U4SM"],
                        row["XLI_D4SM"],
                        row["XLI_SM"],
                        row["XLRE_U4SM"],
                        row["XLRE_D4SM"],
                        row["XLRE_SM"],
                        row["XLK_U4SM"],
                        row["XLK_D4SM"],
                        row["XLK_SM"],
                        row["XLU_U4SM"],
                        row["XLU_D4SM"],
                        row["XLU_SM"],
                        row["XLX_U4SM"],
                        row["XLX_D4SM"],
                        row["XLX_SM"]
                    ))
                    updated += cursor.rowcount
                
                conn.commit()
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated

    def insertOrReplaceMarketRecords(self, records) -> int:
        insert_sql = """
        REPLACE INTO DAILY_MARKET_STATS 
        (dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, 
        up50pctin20d, dn50pctin20d, noofstocks, above200smapct, above150smapct, above50smapct, above20smapct, hsi, hsce) 
        VALUES (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """        
        updated = 0
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.executemany(insert_sql, records)
                return cursor.rowcount  # Returns the total affected rows                
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated                       

    def insertOrReplacePriceRecords(self, records) -> int:
        df = pd.DataFrame(records)
        updated = 0
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
            
                # Use SQLite "INSERT OR REPLACE" logic row-by-row
                for _, row in df.iterrows():
                    cursor.execute('''
                        REPLACE INTO DAILY_STOCK_PRICE 
                            (symbol,
                             period,dt,tm,open,high,low,close,
                             volume,adj_close,open_int
                            )
                        VALUES 
                        (
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?
                        )
                    ''', (
                        row["symbol"],
                        row["period"],
                        row["dt"],
                        row["tm"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        row["adj_close"],
                        row["open_int"]
                    ))
                    updated += cursor.rowcount
                
                conn.commit()
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated 

    def insertOrReplacePriceStats(self, records) -> int:
        df = pd.DataFrame(records)
        updated = 0
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
            
                # Use SQLite "INSERT OR REPLACE" logic row-by-row
                for _, row in df.iterrows():
                    cursor.execute('''
                        REPLACE INTO DAILY_STOCK_STATS 
                        (symbol, dt, start_dt, open, high, low, close, volume, 
                        prev_open, prev_high, prev_low, prev_close, prev_volume, 
                        roc020, roc125, rsi014, sma200, sma150, sma100, sma050, sma020, sma010, sma005, sma003, 
                        ema050, ema200, ema200pref, sma200pref, ema500pref, sma50pref, rsi14sctr, ppo01sctr, roc125sctr, sctr, 
                        histDay, chg_pct_1d, chg_pct_5d, chg_pct_10d, chg_pct_20d, chg_pct_50d, chg_pct_100d, sma10turnover, 
                        sma20turnover, sma50turnover, above_200d_sma ,above_150d_sma ,above_100d_sma ,above_50d_sma, 
                        above_20d_sma ,above_10d_sma ,above_5d_sma, vp_high, vp_low, vp_bullish, vp_bearish,
                        rs, normalise_rs, rs_priceOverSMA20, rs_slopeSMA20, rs_slopeSMA50, rs_slopeSMA150,
                        priceOverSMA20, slopeSMA20, slopeSMA50, slopeSMA150, adr20, adr05, slopeAdr20, slopeAdr05
                        )   
                        VALUES 
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, 
                        ?, ?, ?, ?,
                        ?, ?, ?, ?
                        )
                    ''', (
                        row["symbol"],
                        row["dt"],
                        row["start_dt"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        row["prev_open"],
                        row["prev_high"],
                        row["prev_low"],
                        row["prev_close"],
                        row["prev_volume"],
                        row["roc20"], 
                        row["roc125"],
                        row["rsi14"],
                        row["sma200"],
                        row["sma150"],
                        row["sma100"],
                        row["sma50"],
                        row["sma20"],
                        row["sma10"],
                        row["sma05"],
                        row["sma03"],
                        row["ema50"],   
                        row["ema200"],
                        row["ema200pref"],
                        row["sma200pref"],
                        row["ema500pref"],
                        row["sma50pref"],
                        row["rsi14sctr"],
                        row["ppo01sctr"],
                        row["roc125sctr"],
                        row["sctr"],
                        row["histDay"],  
                        row["chg_pct_1d"],
                        row["chg_pct_5d"],
                        row["chg_pct_10d"],
                        row["chg_pct_20d"],
                        row["chg_pct_50d"],
                        row["chg_pct_100d"],
                        row["sma10turnover"],
                        row["sma20turnover"],  
                        row["sma50turnover"],
                        row["above_200d_sma"],
                        row["above_150d_sma"],
                        row["above_100d_sma"],
                        row["above_50d_sma"],
                        row["above_20d_sma"],
                        row["above_10d_sma"],
                        row["above_5d_sma"],
                        row["vp_high"],
                        row["vp_low"],
                        row["vp_bullish"],
                        row["vp_bearish"],
                        row["rs"],
                        row["normalise_rs"],
                        row["rs_priceOverSMA20"],
                        row["rs_slopeSMA20"],
                        row["rs_slopeSMA50"],
                        row["rs_slopeSMA150"],
                        row["priceOverSMA20"],
                        row["slopeSMA20"],
                        row["slopeSMA50"],
                        row["slopeSMA150"],
                        row["adr20"],
                        row["adr05"],
                        row["slopeAdr20"],
                        row["slopeAdr05"]                                             
                    ))
                    updated += cursor.rowcount
                
                conn.commit()
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated 
    
    def _insertOrReplaceStockInfo(self, records):
        df = pd.DataFrame(records)
        df["symbol"] = df["symbol"].str.replace("/", "-", regex=False)
        updated = 0
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
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

    def fetchTickers(self, whereClause):
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
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

    def callbackWithConn(self, callback, sql, func_name):
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                df = callback(conn, sql, func_name)
                return df           
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit()
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit() 

        