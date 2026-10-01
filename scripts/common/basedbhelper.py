from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import sqlite3
import logging
import sys
import pandas as pd

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
    def insertOrReplaceMarketRecords(self, records) -> int:
        pass

    @abstractmethod
    def insertDailyStockPrice(self, prices: list[dict]) -> int:
        pass

    @abstractmethod
    def readDataFrame(self, sql_main:str):
        pass    

    @abstractmethod
    def callbackWithConn(self, callback, sql):
        pass

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