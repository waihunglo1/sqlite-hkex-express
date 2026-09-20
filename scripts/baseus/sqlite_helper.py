import sqlite3
import logging
import sys
import pandas as pd
from common.basedbhelper import BaseDbHelper

class SqliteDbHelper(BaseDbHelper):
    """Helper class for managing SQLite database operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def insertOrReplaceStockInfo(self, df):
        df["symbol"] = df["symbol"].str.replace("/", "-", regex=False)

        # Clean/map DataFrame columns to match table schema
        data_df = pd.DataFrame({
            "symbol": df["symbol"],
            "name": df.get("name", ""),
            "sector": df.get("industry", ""),
            "market_cap": df.get("marketCap", None),
        })

        sql = """
            INSERT OR REPLACE INTO STOCK (symbol, name, sector, market_cap) 
            VALUES (?, ?, ?, ?)
        """

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                data_tuples = list(data_df.itertuples(index=False, name=None))
                cursor.executemany(sql, data_tuples)
                conn.commit()
                return cursor.rowcount if cursor.rowcount != -1 else len(data_df)

        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

    def fetchTickers(self, whereClause):
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT symbol FROM stock WHERE " + whereClause + " ORDER BY symbol")

                symbols = [row[0] for row in cursor.fetchall()]

                if not symbols:
                    logging.info("沒有需要更新的股票。")
                return symbols
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)    

    def insertDailyStockPrice(self, prices: list) -> int:
        if not prices:
            return 0

        sql = """
            INSERT OR REPLACE INTO DAILY_STOCK_PRICE (
                symbol, period, dt, tm, open, high, low, close, volume, adj_close, open_int
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        if isinstance(prices[0], dict):
            param_tuples = [
                (
                    p.get("symbol") or p.get("ticker"),
                    p.get("period", "D"),
                    p.get("dt"),
                    p.get("tm", "000000"),
                    p.get("open"),
                    p.get("high"),
                    p.get("low"),
                    p.get("close"),
                    p.get("volume"),
                    p.get("adj_close"),
                    0,
                )
                for p in prices
            ]
        else:
            param_tuples = prices

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.executemany(sql, param_tuples)
                conn.commit()
                return len(prices)
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)

    def insertOrReplaceHistPrice(self, hist) -> int: 
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
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                data_tuples = list(hist_final.itertuples(index=False, name=None))
                cursor.executemany(static_sql, data_tuples)
                conn.commit()
                logging.info(f"✅ 成功將 {len(hist_final)} 筆歷史數據以 100% 靜態安全語法更新至資料庫。")

                return len(data_tuples)
        except Exception as e:
            logging.error(f"❌ 寫入資料庫時出錯: {e}")

        return 0 
    
    def insertOrReplaceStockInfo2(self, records) -> int:
        df = pd.DataFrame(records)
        if df.empty:
            return 0

        sql = """
            INSERT OR REPLACE INTO STOCK (symbol, name, industry, sector, market_cap, industry_en, sector_en, quote_type) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                data_tuples = [
                    (
                        row['symbol'], row['name'], row['industry'], 
                        row['sector'], row['marketCap'],
                        row['industry_en'], row['sector_en'],
                        row['quoteType']
                    )
                    for _, row in df.iterrows()
                ]
                cursor.executemany(sql, data_tuples)
                conn.commit()
                return len(df)
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

    def fetchAllRows(self, sql: str, params: tuple | list | dict = ()):
        cleaned = sql.replace("\r", " ").replace("\n", " ")
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(cleaned, params)
                rows = cursor.fetchall()
                # Convert sqlite3.Row objects to standard Python dictionaries
                return [dict(row) for row in rows]
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)  

    def insertOrReplacePriceStats(self, records) -> int:
        df = pd.DataFrame(records)
        if df.empty:
            return 0
            
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                
                data_to_insert = list(df[[
                    "symbol", "dt", "start_dt", "open", "high", "low", "close", "volume",
                    "prev_open", "prev_high", "prev_low", "prev_close", "prev_volume",
                    "roc20", "roc125", "rsi14", "sma200", "sma150", "sma100", "sma50", "sma20", "sma10", "sma05", "sma03",
                    "ema50", "ema200", "ema200pref", "sma200pref", "ema500pref", "sma50pref", "rsi14sctr", "ppo01sctr", "roc125sctr", "sctr",
                    "histDay", "chg_pct_1d", "chg_pct_5d", "chg_pct_10d", "chg_pct_20d", "chg_pct_50d", "chg_pct_63d", 
                    "chg_pct_100d", "chg_pct_150d", "chg_pct_200d", 
                    "sma10turnover","sma20turnover", "sma50turnover", 
                    "above_200d_sma", "above_150d_sma", 
                    "above_100d_sma", "above_50d_sma",
                    "above_20d_sma", "above_10d_sma", "above_5d_sma", 
                    "vp_high", "vp_low", "vp_bullish", "vp_bearish",
                    "rs", "normalise_rs", "rs_priceOverSMA20", "rs_slopeSMA20", "rs_slopeSMA50", "rs_slopeSMA150",
                    "priceOverSMA20", "slopeSMA20", "slopeSMA50", "slopeSMA150", "adr20", "adr05", "slopeAdr20", "slopeAdr05",
                    "normalise_rs1","normalise_rs2","normalise_rs3","normalise_rs4","normalise_rs5",
                    "normalise_rs6","normalise_rs7","normalise_rs8","normalise_rs9","normalise_rs10",
                    "normalise_rs11","normalise_rs12","normalise_rs13","normalise_rs14","normalise_rs15",
                    "normalise_rs16","normalise_rs17","normalise_rs18","normalise_rs19","normalise_rs20",
                    "sctr1","sctr2","sctr3","sctr4","sctr5",                        
                    "sctr6","sctr7","sctr8","sctr9","sctr10",  
                    "sctr11","sctr12","sctr13","sctr14","sctr15",  
                    "sctr16","sctr17","sctr18","sctr19","sctr20"
                ]].itertuples(index=False, name=None))
                
                cursor.executemany('''
                    INSERT OR REPLACE INTO DAILY_STOCK_STATS (
                        symbol, dt, start_dt, open, high, low, close, volume, 
                        prev_open, prev_high, prev_low, prev_close, prev_volume, 
                        roc020, roc125, rsi014, sma200, sma150, sma100, sma050, sma020, sma010, sma005, sma003, 
                        ema050, ema200, ema200pref, sma200pref, ema500pref, sma50pref, rsi14sctr, ppo01sctr, roc125sctr, sctr, 
                        histDay, 
                        chg_pct_1d, chg_pct_5d, chg_pct_10d, chg_pct_20d, chg_pct_50d, chg_pct_63d,
                        chg_pct_100d, chg_pct_150d, chg_pct_200d,
                        sma10turnover, sma20turnover, sma50turnover, 
                        above_200d_sma, above_150d_sma, above_100d_sma, above_50d_sma, 
                        above_20d_sma, above_10d_sma, above_5d_sma, 
                        vp_high, vp_low, vp_bullish, vp_bearish,
                        rs, normalise_rs, rs_priceOverSMA20, rs_slopeSMA20, rs_slopeSMA50, rs_slopeSMA150,
                        priceOverSMA20, slopeSMA20, slopeSMA50, slopeSMA150, adr20, adr05, slopeAdr20, slopeAdr05,
                        normalise_rs1,normalise_rs2,normalise_rs3,normalise_rs4,normalise_rs5,
                        normalise_rs6,normalise_rs7,normalise_rs8,normalise_rs9,normalise_rs10,
                        normalise_rs11,normalise_rs12,normalise_rs13,normalise_rs14,normalise_rs15,
                        normalise_rs16,normalise_rs17,normalise_rs18,normalise_rs19,normalise_rs20,
                        sctr1,sctr2,sctr3,sctr4,sctr5,                        
                        sctr6,sctr7,sctr8,sctr9,sctr10,  
                        sctr11,sctr12,sctr13,sctr14,sctr15,  
                        sctr16,sctr17,sctr18,sctr19,sctr20
                    ) 
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?,?,?,?,?,
                        ?,?,?,?,?,
                        ?,?,?,?,?,
                        ?,?,?,?,?,
                        ?,?,?,?,?,
                        ?,?,?,?,?,
                        ?,?,?,?,?,
                        ?,?,?,?,?
                    )
                ''', data_to_insert)
                
                conn.commit()
                return len(data_to_insert)
                
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

    def insertOrReplaceSectorRecords(self, records) -> int:
        df = pd.DataFrame(records)
        if df.empty:
            return 0

        columns_order = [
            "dt", 
            "XLB_U4SM",  "XLB_D4SM",  "XLB_SM", 
            "XLC_U4SM",  "XLC_D4SM",  "XLC_SM", 
            "XLY_U4SM",  "XLY_D4SM",  "XLY_SM", 
            "XLP_U4SM",  "XLP_D4SM",  "XLP_SM", 
            "XLE_U4SM",  "XLE_D4SM",  "XLE_SM", 
            "XLF_U4SM",  "XLF_D4SM",  "XLF_SM", 
            "XLV_U4SM",  "XLV_D4SM",  "XLV_SM", 
            "XLI_U4SM",  "XLI_D4SM",  "XLI_SM", 
            "XLRE_U4SM", "XLRE_D4SM", "XLRE_SM", 
            "XLK_U4SM",  "XLK_D4SM",  "XLK_SM", 
            "XLU_U4SM",  "XLU_D4SM",  "XLU_SM", 
            "XLX_U4SM",  "XLX_D4SM",  "XLX_SM"
        ]

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                data_to_insert = list(df[columns_order].itertuples(index=False, name=None))
                placeholders = ", ".join(["?"] * len(columns_order))
                columns_str = ", ".join(columns_order)

                sql = f"INSERT OR REPLACE INTO DAILY_SECTORS_STATS ({columns_str}) VALUES ({placeholders})"
                
                cursor.executemany(sql, data_to_insert)
                conn.commit()
                return len(data_to_insert)
                
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

    def insertOrReplaceMarketRecords(self, records) -> int:
        if not records:
            return 0
            
        if isinstance(records, list) and isinstance(records[0], dict):
            df = pd.DataFrame(records)
            columns_order = [
                "dt", "up4pct1d", "dn4pct1d", "up25pctin100d", "dn25pctin100d", 
                "up25pctin20d", "dn25pctin20d", "up50pctin20d", "dn50pctin20d", 
                "noofstocks", "above200smapct", "above150smapct", "above50smapct", 
                "above20smapct", "hsi", "hsce"
            ]
            data_to_insert = list(df[columns_order].itertuples(index=False, name=None))
        else:
            data_to_insert = records

        sql = """
            INSERT OR REPLACE INTO DAILY_MARKET_STATS 
            (dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, 
            up50pctin20d, dn50pctin20d, noofstocks, above200smapct, above150smapct, above50smapct, above20smapct, hsi, hsce) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """        
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.executemany(sql, data_to_insert)
                conn.commit()
                return len(data_to_insert)
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

    def callbackWithConn(self, callback, sql):
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                df = callback(conn, sql)
                return df           
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

    def readDataFrame(self, sql_main):
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                df_main = pd.read_sql_query(sql_main, conn)
                return df_main           
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

    def storeIndustryStatistics(self, up_df_pivoted, dn_df_pivoted):
        # 1. Ensure both DataFrames share the same index name
        up_df_pivoted.index.name = "industry"
        dn_df_pivoted.index.name = "industry"

        # 2. Get sorted unique dates descending (latest date first)
        unique_dates = sorted(up_df_pivoted.columns, reverse=True)

        # 3. Align both DataFrames to the same sorted columns & fill NaNs with 0
        up_aligned = up_df_pivoted.reindex(columns=unique_dates).fillna(0).astype(int)
        dn_aligned = dn_df_pivoted.reindex(columns=unique_dates).fillna(0).astype(int)

        # 4. Construct the output DataFrame with explicit numeric up/down columns
        combined_df = pd.DataFrame(index=up_aligned.index)

        for i in range(90):
            day_num = i + 1
            up_col = f"up_day{day_num}"
            dn_col = f"down_day{day_num}"

            if i < len(unique_dates):
                date_str = unique_dates[i]
                combined_df[up_col] = up_aligned[date_str]
                combined_df[dn_col] = dn_aligned[date_str]
            else:
                combined_df[up_col] = 0
                combined_df[dn_col] = 0

        # 5. Calculate cumulative 90-day totals
        combined_df["total_up_90d"] = up_aligned.sum(axis=1)
        combined_df["total_down_90d"] = dn_aligned.sum(axis=1)

        # 6. Sort rows by up_day1 DESC, then total_up_90d DESC
        combined_df = combined_df.sort_values(
            by=["up_day1", "total_up_90d"], ascending=[False, False]
        )

        combined_df = combined_df.reset_index()

        # Generate explicit column order
        ordered_columns = ["industry"]
        for i in range(1, 91):
            ordered_columns.extend([f"up_day{i}", f"down_day{i}"])
        ordered_columns.extend(["total_up_90d", "total_down_90d"])

        combined_df = combined_df[ordered_columns]

        # 7. Build DDL Statement in 1 Go
        col_definitions = ['"industry" TEXT PRIMARY KEY']
        for i in range(1, 91):
            col_definitions.append(f'"up_day{i}" INTEGER NOT NULL DEFAULT 0')
            col_definitions.append(f'"down_day{i}" INTEGER NOT NULL DEFAULT 0')
        col_definitions.append('"total_up_90d" INTEGER NOT NULL DEFAULT 0')
        col_definitions.append('"total_down_90d" INTEGER NOT NULL DEFAULT 0')

        create_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS "DAILY_INDUSTRY_90D_MATRIX" (
            {', '.join(col_definitions)}
        );
        """

        # 8. Execute Schema Creation + Truncate & Insert in 1 Transaction
        logging.info("Writing UP/DOWN matrix to DAILY_INDUSTRY_90D_MATRIX...")
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                
                # Create table with DEFAULTS in 1 go if it doesn't exist
                cursor.execute(create_table_ddl)
                
                # Clear old snapshot data
                cursor.execute('DELETE FROM "DAILY_INDUSTRY_90D_MATRIX";')
                
                # Append new 90-day snapshot preserving the explicit schema
                combined_df.to_sql(
                    name="DAILY_INDUSTRY_90D_MATRIX",
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                )
                return combined_df
                
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)