import sys
import logging
import os
import time
import duckdb
import pandas as pd
from pathlib import Path
from common.basedbhelper import BaseDbHelper


class DuckDbHelper(BaseDbHelper):
    """Helper class for managing DuckDB database operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self):
        """Returns a connection to the DuckDB database."""
        return duckdb.connect(self.db_path, read_only=False)

    def insertDailyStockPrice(self, prices: list[dict]) -> int:
        if not prices:
            return 0

        df_prices = pd.DataFrame(prices)

        try:
            with self._get_connection() as conn:
                # High-performance DuckDB upsert directly from Pandas DataFrame
                conn.register("df_prices_temp", df_prices)
                conn.execute("""
                    INSERT OR REPLACE INTO DAILY_STOCK_PRICE (
                        symbol, period, dt, tm, open, high, low, close, volume, adj_close, open_int
                    ) 
                    SELECT 
                        symbol, period, dt, tm, open, high, low, close, volume, adj_close, open_int
                    FROM df_prices_temp
                """)
                return len(df_prices)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in insertDailyStockPrice: {e}")
            sys.exit(1)

    def insertOrReplaceStockInfo(self, records) -> int:
        if not records:
            return 0

        df = pd.DataFrame(records)
        # Handle dict column mapping if marketCap/quoteType are named differently
        if "marketCap" in df.columns:
            df.rename(columns={"marketCap": "market_cap"}, inplace=True)
        if "quoteType" in df.columns:
            df.rename(columns={"quoteType": "quote_type"}, inplace=True)

        try:
            with self._get_connection() as conn:
                conn.register("df_stock_temp", df)
                conn.execute("""
                    INSERT OR REPLACE INTO STOCK (
                        symbol, name, industry, sector, market_cap, industry_en, sector_en, quote_type
                    )
                    SELECT 
                        symbol, name, industry, sector, market_cap, industry_en, sector_en, quote_type 
                    FROM df_stock_temp
                """)
                return len(df)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in insertOrReplaceStockInfo: {e}")
            sys.exit(1)

    def fetchAllRows(self, sql: str, params: tuple | list | dict = ()):
        cleaned = sql.replace("\r", " ").replace("\n", " ")
        try:
            with self._get_connection() as conn:
                # Execute query with parameters
                cursor = conn.execute(cleaned, params if isinstance(params, (list, tuple)) else list(params.values()))
                
                # Retrieve column names and map rows to emulate sqlite3.Row dict-like behavior
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logging.error(f"❌ DuckDB Query Error: {e}")
            sys.exit(1)

    def insertOrReplaceSectorRecords(self, records) -> int:
        if not records:
            return 0
            
        df = pd.DataFrame(records)
        try:
            with self._get_connection() as conn:
                conn.register("df_sectors_temp", df)
                conn.execute("""
                    INSERT OR REPLACE INTO DAILY_SECTORS_STATS 
                    SELECT * FROM df_sectors_temp
                """)
                return len(df)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in insertOrReplaceSectorRecords: {e}")
            sys.exit(1)

    def insertOrReplaceMarketRecords(self, records) -> int:
        if not records:
            return 0

        df = pd.DataFrame(records) if not isinstance(records, pd.DataFrame) else records
        try:
            with self._get_connection() as conn:
                conn.register("df_market_temp", df)
                conn.execute("""
                    INSERT OR REPLACE INTO DAILY_MARKET_STATS 
                    SELECT * FROM df_market_temp
                """)
                return len(df)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in insertOrReplaceMarketRecords: {e}")
            sys.exit(1)

    def insertOrReplacePriceRecords(self, records) -> int:
        if not records:
            return 0

        df = pd.DataFrame(records)
        try:
            with self._get_connection() as conn:
                conn.register("df_price_temp", df)
                conn.execute("""
                    INSERT OR REPLACE INTO DAILY_STOCK_PRICE (
                        symbol, period, dt, tm, open, high, low, close, volume, adj_close, open_int
                    )
                    SELECT 
                        symbol, period, dt, tm, open, high, low, close, volume, adj_close, open_int
                    FROM df_price_temp
                """)
                return len(df)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in insertOrReplacePriceRecords: {e}")
            sys.exit(1)

    def insertOrReplacePriceStats(self, records) -> int:
        if not records:
            return 0

        df = pd.DataFrame(records)
        
        # Map DataFrame column names to database schema targets where names differ
        col_mappings = {
            "roc20": "roc020",
            "rsi14": "rsi014",
            "sma50": "sma050",
            "sma20": "sma020",
            "sma10": "sma010",
            "sma05": "sma005",
            "sma03": "sma003",
            "ema50": "ema050"
        }
        df.rename(columns=col_mappings, inplace=True)

        try:
            with self._get_connection() as conn:
                conn.register("df_stats_temp", df)
                
                # Fetch target table schema columns to ensure exact positional alignment
                target_cols = [
                    col[0] for col in conn.execute("DESCRIBE DAILY_STOCK_STATS").fetchall()
                ]
                
                # Select only matching columns present in the DataFrame
                valid_cols = [c for c in target_cols if c in df.columns]
                cols_str = ", ".join(valid_cols)

                conn.execute(f"""
                    INSERT OR REPLACE INTO DAILY_STOCK_STATS ({cols_str})
                    SELECT {cols_str} FROM df_stats_temp
                """)
                return len(df)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in insertOrReplacePriceStats: {e}")
            sys.exit(1)

    def _insertOrReplaceStockInfo(self, records) -> int:
        if not records:
            return 0

        df = pd.DataFrame(records)
        df["symbol"] = df["symbol"].str.replace("/", "-", regex=False)
        
        if "marketCap" in df.columns:
            df.rename(columns={"marketCap": "market_cap"}, inplace=True)
        if "industry" in df.columns and "sector" not in df.columns:
            df["sector"] = df["industry"]

        try:
            with self._get_connection() as conn:
                conn.register("df_stock_info_temp", df)
                conn.execute("""
                    INSERT INTO STOCK (symbol, name, sector, market_cap)
                    SELECT symbol, name, sector, market_cap FROM df_stock_info_temp
                    ON CONFLICT(symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        market_cap = EXCLUDED.market_cap
                """)
                return len(df)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in _insertOrReplaceStockInfo: {e}")
            sys.exit(1)

    def fetchTickers(self, whereClause: str) -> list[str]:
        try:
            with self._get_connection() as conn:
                sql = f"SELECT symbol FROM STOCK WHERE {whereClause} ORDER BY symbol"
                results = conn.execute(sql).fetchall()
                symbols = [row[0] for row in results]

                if not symbols:
                    logging.info("沒有需要更新的股票。")
                return symbols
        except Exception as e:
            logging.error(f"❌ DuckDB Error in fetchTickers: {e}")
            sys.exit(1)

    def callbackWithConn(self, callback, sql: str):
        try:
            with self._get_connection() as conn:
                return callback(conn, sql)
        except Exception as e:
            logging.error(f"❌ DuckDB Error in callbackWithConn: {e}")
            sys.exit(1)

    def readDataFrame(self, sql_main: str) -> pd.DataFrame:
        try:
            with self._get_connection() as conn:
                # Native DuckDB to Pandas conversion without triggering pandas UserWarnings
                return conn.execute(sql_main).df()
        except Exception as e:
            logging.error(f"❌ DuckDB Error in readDataFrame: {e}")
            sys.exit(1)