from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import sqlite3
import logging
import sys
import pandas as pd
from . import utility as helper

class BaseDbHelper(ABC):

    @abstractmethod
    def fetchAllRows(
        self, query: str, params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all query results formatted as a list of dictionaries."""
        pass

    @abstractmethod
    def insertOrReplacePriceStats(
        self, price_stats_list: List[Dict[str, Any]]
    ) -> int:
        """Upsert daily price statistics into the database."""
        pass

    @abstractmethod
    def insertOrReplaceSectorRecords(self, records) -> int:
        pass

    @abstractmethod
    def insertDailyStockPrice(self, prices: list[dict]) -> int:
        pass

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

    def storeIndustryStatistics(self, up_df_pivoted, dn_df_pivoted, above_50d_sma_pct_df_pivoted):
        # 1. Ensure both DataFrames share the same index name
        up_df_pivoted.index.name = "industry"
        dn_df_pivoted.index.name = "industry"
        above_50d_sma_pct_df_pivoted.index.name = "industry"

        # 2. Get sorted unique dates descending (latest date first)
        unique_dates = sorted(up_df_pivoted.columns, reverse=True)

        # 3. Align both DataFrames to the same sorted columns & fill NaNs with 0
        up_aligned = up_df_pivoted.reindex(columns=unique_dates).fillna(0).astype(int)
        dn_aligned = dn_df_pivoted.reindex(columns=unique_dates).fillna(0).astype(int)
        above_50d_sma_pct_aligned = above_50d_sma_pct_df_pivoted.reindex(columns=unique_dates).fillna(0).astype(int)

        # 4. Construct the output DataFrame with explicit numeric up/down columns
        combined_df = pd.DataFrame(index=up_aligned.index)

        for i in range(90):
            day_num = i + 1
            up_col = f"up_day{day_num}"
            dn_col = f"down_day{day_num}"
            sma50_col = f"sma50_day{day_num}"

            if i < len(unique_dates):
                date_str = unique_dates[i]
                combined_df[up_col] = up_aligned[date_str]
                combined_df[dn_col] = dn_aligned[date_str]
                combined_df[sma50_col] = above_50d_sma_pct_aligned[date_str]
            else:
                combined_df[up_col] = 0
                combined_df[dn_col] = 0
                combined_df[sma50_col] = 0

        # 5. Calculate cumulative 90-day totals
        combined_df["total_up_90d"] = up_aligned.sum(axis=1)
        combined_df["total_down_90d"] = dn_aligned.sum(axis=1)
        combined_df["total_sma50_90d"] = above_50d_sma_pct_aligned.sum(axis=1)

        # 6. Sort rows by up_day1 DESC, then total_up_90d DESC
        combined_df = combined_df.sort_values(
            by=["up_day1", "total_up_90d"], ascending=[False, False]
        )

        combined_df = combined_df.reset_index()

        # Generate explicit column order
        ordered_columns = ["industry"]
        for i in range(1, 91):
            ordered_columns.extend([f"up_day{i}", f"down_day{i}", f"sma50_day{i}"])
        ordered_columns.extend(["total_up_90d", "total_down_90d", "total_sma50_90d"])

        combined_df = combined_df[ordered_columns]

        # 7. Build DDL Statement in 1 Go
        col_definitions = ['"industry" TEXT PRIMARY KEY']
        for i in range(1, 91):
            col_definitions.append(f'"up_day{i}" INTEGER NOT NULL DEFAULT 0')
            col_definitions.append(f'"down_day{i}" INTEGER NOT NULL DEFAULT 0')
            col_definitions.append(f'"sma50_day{i}" REAL NOT NULL DEFAULT 0')
        col_definitions.append('"total_up_90d" INTEGER NOT NULL DEFAULT 0')
        col_definitions.append('"total_down_90d" INTEGER NOT NULL DEFAULT 0')
        col_definitions.append('"total_sma50_90d" INTEGER NOT NULL DEFAULT 0')

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
                        REPLACE INTO STOCK (symbol, name, industry, sector, market_cap, industry_en, sector_en, quote_type) 
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
            sys.exit(-1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(-1)

        return updated

    def dumpSectorStatistics(self):
        # price history row
        sectorSql = """
            SELECT sector, count(1) 
            FROM stock
            group by sector
            order by sector 
        """
        sectors = self.fetchAllRows(sectorSql)
        helper.prettyPrint(sectors)

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