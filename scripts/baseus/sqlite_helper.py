import sqlite3
import logging
import sys
import pandas as pd
from common.basedbhelper import BaseDbHelper

class SqliteDbHelper(BaseDbHelper):
    """Helper class for managing SQLite database operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def insertOrReplaceStockInfoTickerMap(self, df):
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
                return cursor.rowcount if cursor.rowcount != -1 else len(prices)
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

                return cursor.rowcount if cursor.rowcount != -1 else len(data_to_insert)    
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)



    def init_sctr_table(self, table_name: str):
        """Creates the SCTR database table with explicit column schema."""
        ddl_statement = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dt TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT,
            sctr REAL,
            delta REAL,
            marketcap REAL,
            vol REAL,
            close REAL,
            industry TEXT,
            sector TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (dt, symbol)
        );
        """

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:        
                conn.execute(ddl_statement)
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_sctr_dt ON {table_name} (dt DESC);"
                )
                conn.commit()
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)                

    def populateToSctrTable(self, df_clean, table_name):
        # Restrict DataFrame columns to ONLY match defined SQLite columns
        valid_db_columns = [
            "dt",
            "symbol",
            "name",
            "sctr",
            "close",
            "chg_pct",
            "vol",
            "marketcap",
            "delta",
            "industry",
            "sector"
        ]
        insert_cols = [col for col in valid_db_columns if col in df_clean.columns]
        df_clean = df_clean[insert_cols]

        # Convert to dictionary records for binding
        records = df_clean.to_dict(orient="records")
        return self._insertOrReplaceSctrTable(records, table_name, insert_cols)

    def _insertOrReplaceSctrTable(self, records, table_name, insert_cols):
        # Prepare SQLite Connection and initialize table
        self.init_sctr_table(table_name)

        col_names = ", ".join(insert_cols)
        placeholders = ", ".join([f":{col}" for col in insert_cols])

        insert_sql = f"""
            INSERT OR REPLACE INTO {table_name} ({col_names})
            VALUES ({placeholders})
        """

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:        
                cursor = conn.cursor()
                cursor.executemany(insert_sql, records)
                conn.commit()

                return cursor.rowcount if cursor.rowcount != -1 else len(records)
        except sqlite3.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)   


