import logging
from typing import Any, Dict, List, Optional
import pandas as pd
import psycopg

from . import basedbhelper as dbHelper
from common import gsheethelper as gsheetHelper


VALID_SECTORS = [
    "XLB", "XLC", "XLY", "XLP", "XLE", "XLF",
    "XLV", "XLI", "XLRE", "XLK", "XLU", "XLX"
]

class StatisticsProcessor:

    def __init__(self, dbHelper, avienUri: str):
        self.dbHelper = dbHelper
        self.avienUri = avienUri

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------
    def _read_data(self, sql: str, label: str) -> pd.DataFrame:
        """Helper to safely fetch DataFrame from database and log status."""
        df = self.dbHelper.readDataFrame(sql)
        if df.empty:
            logging.info(f"⚠️ No data returned for [{label}]. Task skipped.")
            return pd.DataFrame()
        
        logging.info(f"{label}:\n{df.iloc[:5, :10].to_string()}")
        return df

    def _export_to_gsheet(self, df: pd.DataFrame, config_section: Dict[str, Any]) -> None:
        """Helper to push DataFrame results to Google Sheets based on config."""
        if df.empty:
            return
        
        spreadsheet_id = config_section.get("SPREAD_SHEET_ID")
        worksheet_id = config_section.get("WORK_SHEET_ID")
        
        if spreadsheet_id and worksheet_id:
            gsheetHelper.populate_df(df, spreadsheet_id, worksheet_id)

    @staticmethod
    def _ensure_dt_column(df: pd.DataFrame) -> pd.DataFrame:
        """Ensures 'dt' is present as a column rather than an index."""
        if "dt" not in df.columns and df.index.name == "dt":
            return df.reset_index()
        return df

    # -------------------------------------------------------------------------
    # Market & Sector Statistics
    # -------------------------------------------------------------------------
    def populateSectorStatistics(self, config: Dict[str, Any]) -> pd.DataFrame:
        sql = config["STATISTICS_MARKET_ANALYSIS"]["SQL.02"]
        raw_rows = self.dbHelper.fetchAllRows(sql)
        
        if not raw_rows:
            logging.info("⚠️ Sector statistics dataset empty.")
            return pd.DataFrame()

        df_raw = pd.DataFrame(raw_rows)
        
        # Filter for known sectors only
        invalid_sectors = df_raw[~df_raw["sector"].isin(VALID_SECTORS)]
        if not invalid_sectors.empty:
            for _, row in invalid_sectors.iterrows():
                logging.info(f"[ERROR] Unknown sector: {row['sector']} on {row['dt']}")

        df_filtered = df_raw[df_raw["sector"].isin(VALID_SECTORS)]

        # Pivot sector statistics into flat columns per date
        pivot_u4sm = df_filtered.pivot_table(index="dt", columns="sector", values="u4sm").add_suffix("_U4SM")
        pivot_d4sm = df_filtered.pivot_table(index="dt", columns="sector", values="d4sm").add_suffix("_D4SM")
        pivot_sm = df_filtered.pivot_table(index="dt", columns="sector", values="sm").add_suffix("_SM")

        df_sector_stats = pd.concat([pivot_u4sm, pivot_d4sm, pivot_sm], axis=1).fillna(0).reset_index()

        # Guarantee presence of all sector columns even if missing from SQL response
        expected_cols = ["dt"] + [f"{sec}_{metric}" for sec in VALID_SECTORS for metric in ["U4SM", "D4SM", "SM"]]
        df_sector_stats = df_sector_stats.reindex(columns=expected_cols, fill_value=0)

        logging.info("Sector Statistics:\n" + df_sector_stats.iloc[:5, :10].to_string())
        return df_sector_stats

    def populateMarketStatistics(self, config: Dict[str, Any]) -> pd.DataFrame:
        sql = config["STATISTICS_MARKET_ANALYSIS"]["SQL.01"]
        return self._read_data(sql, "Market Statistics")

    def doMarketAnalysis(self, config: Dict[str, Any]) -> None:
        market_df = self.populateMarketStatistics(config)
        sector_df = self.populateSectorStatistics(config)

        if market_df.empty:
            return

        market_df = self._ensure_dt_column(market_df)
        sector_df = self._ensure_dt_column(sector_df)

        df_merged = pd.merge(market_df, sector_df, on="dt", how="left")
        
        logging.info("Market / Sector Analysis:\n" + df_merged.iloc[:5, :10].to_string())
        self._export_to_gsheet(df_merged, config["STATISTICS_MARKET_ANALYSIS"])

    # -------------------------------------------------------------------------
    # Stock Statistics
    # -------------------------------------------------------------------------
    def populateStockStatistics(self, config: Dict[str, Any]) -> pd.DataFrame:
        sql = config["STATISTICS_STOCK_ANALYSIS"]["SQL.01"]
        return self._read_data(sql, "Stock Statistics")

    def doStockAnalysis(self, config: Dict[str, Any]) -> None:
        stock_df = self.populateStockStatistics(config)
        self._export_to_gsheet(stock_df, config["STATISTICS_STOCK_ANALYSIS"])

    # -------------------------------------------------------------------------
    # Industry Statistics
    # -------------------------------------------------------------------------
    def populateIndustryStatistics01(self, config: Dict[str, Any]) -> pd.DataFrame:
        sql_query = config["STATISTICS_INDUSTRY_ANALYSIS"]["SQL.01"]
        df_raw = self.dbHelper.readDataFrame(sql_query)

        if df_raw.empty:
            logging.info("⚠️ No data returned for Industry Statistics. Skipping.")
            return pd.DataFrame()

        # Create pivots for up, down, and 50d SMA %
        up_pivot = df_raw.pivot_table(
                index=["sector", "industry"], 
                columns="dt", 
                values="up4pct1d", 
                aggfunc="sum"
            ).fillna(0)
        dn_pivot = df_raw.pivot_table(
                index=["sector", "industry"], 
                columns="dt", 
                values="dn4pct1d", 
                aggfunc="sum"
            ).fillna(0)
        sma_pivot = df_raw.pivot_table(
                index=["sector", "industry"], 
                columns="dt", 
                values="above_50d_sma_pct", 
                aggfunc="sum"
            ).fillna(0)

        # Sort dates descending (newest first)
        unique_dates = sorted(up_pivot.columns, reverse=True)

        up_aligned = up_pivot.reindex(columns=unique_dates, fill_value=0).astype(int)
        dn_aligned = dn_pivot.reindex(columns=unique_dates, fill_value=0).astype(int)
        sma_aligned = sma_pivot.reindex(columns=unique_dates, fill_value=0).astype(int)

        combined_df = pd.DataFrame(index=up_aligned.index)

        # Build 90-day window metrics
        for i in range(90):
            day_num = i + 1
            if i < len(unique_dates):
                date_str = unique_dates[i]
                combined_df[f"up_day{day_num}"] = up_aligned[date_str]
                combined_df[f"down_day{day_num}"] = dn_aligned[date_str]
                combined_df[f"sma50_day{day_num}"] = sma_aligned[date_str]
            else:
                combined_df[f"up_day{day_num}"] = 0
                combined_df[f"down_day{day_num}"] = 0
                combined_df[f"sma50_day{day_num}"] = 0

        # Totals
        combined_df["total_up_90d"] = up_aligned.sum(axis=1)
        combined_df["total_down_90d"] = dn_aligned.sum(axis=1)
        combined_df["total_sma50_90d"] = sma_aligned.sum(axis=1)

        # Sort and rearrange columns
        combined_df = combined_df.sort_values(by=["up_day1", "total_up_90d"], ascending=[False, False]).reset_index()

        ordered_columns = ["sector", "industry"]
        for i in range(1, 91):
            ordered_columns.extend([f"up_day{i}"])
        ordered_columns.extend(["total_up_90d"])

        for i in range(1, 91):
            ordered_columns.extend([f"down_day{i}"])
        ordered_columns.extend(["total_down_90d"])

        for i in range(1, 91):
            ordered_columns.extend([f"sma50_day{i}"])
        ordered_columns.extend(["total_sma50_90d"])        

        combined_df = combined_df[ordered_columns]

        logging.info("Industry Statistics:\n" + combined_df.iloc[:5, :10].to_string())
        return combined_df

    def doIndustryStatistic01(self, config: Dict[str, Any]) -> None:
        df = self.populateIndustryStatistics01(config)
        self._export_to_gsheet(df, config["STATISTICS_INDUSTRY_ANALYSIS"])

    def populateIndustryStatistics02(
            self, config: Dict[str, Any]
        ) -> pd.DataFrame:
        sql_query = config["STATISTICS_INDUSTRY_ANALYSIS_MOMENTUM"]["SQL.01"]
        df_raw = self.dbHelper.readDataFrame(sql_query)

        if df_raw.empty:
            logging.info(
                "⚠️ No data returned for STATISTICS_INDUSTRY_ANALYSIS_MOMENTUM. Skipping."
            )
            return pd.DataFrame()

        # 1. Clean 'sector' and 'industry' columns (strip whitespace, set empty strings/spaces to NaN)
        df_raw["sector"] = df_raw["sector"].astype(str).str.strip()
        df_raw["industry"] = df_raw["industry"].astype(str).str.strip()

        # 2. Filter out rows where sector/industry is NaN, 'None', 'nan', or empty string ''
        invalid_values = {"", "nan", "none", "null", "n/a"}
        mask = (
            df_raw["sector"].notna()
            & ~df_raw["sector"].str.lower().isin(invalid_values)
            & df_raw["industry"].notna()
            & ~df_raw["industry"].str.lower().isin(invalid_values)
        )
        df_raw = df_raw[mask]

        if df_raw.empty:
            logging.info(
                "⚠️ No valid sector/industry data remaining after cleaning. Skipping."
            )
            return pd.DataFrame()

        df_pivoted = (
            df_raw.pivot_table(
                index=["sector", "industry"],
                columns="dt",
                values="noofstockmtm",
            )
            .fillna(0)
            .sort_index(axis=1, ascending=False)
        )

        latest_dt = df_pivoted.columns[0]
        df_pivoted = df_pivoted.sort_values(
            by=latest_dt, ascending=False
        ).reset_index()
        df_pivoted.columns.name = None

        logging.info(
            "Industry 02 Statistics:\n"
            + df_pivoted.iloc[:5, :10].to_string()
        )
        return df_pivoted

    def populateSectorStatistics02(self, config: Dict[str, Any], aggfunc: str = "sum") -> pd.DataFrame:
        sql_query = config["STATISTICS_INDUSTRY_ANALYSIS_MOMENTUM"]["SQL.01"]
        df_raw = self.dbHelper.readDataFrame(sql_query)

        if df_raw.empty:
            logging.info("⚠️ No data returned. Skipping.")
            return pd.DataFrame()

        # 1. Clean 'sector' column
        df_raw["sector"] = df_raw["sector"].astype(str).str.strip()
        invalid_values = {"", "nan", "none", "null", "n/a"}
        df_raw = df_raw[
            df_raw["sector"].notna() & 
            ~df_raw["sector"].str.lower().isin(invalid_values)
        ]

        if df_raw.empty:
            logging.info("⚠️ No valid sector data remaining. Skipping.")
            return pd.DataFrame()

        # 2. Pivot by sector only with aggregation (sum or mean)
        df_pivoted = (
            df_raw.pivot_table(
                index="sector",
                columns="dt",
                values="noofstockmtm",
                aggfunc=aggfunc,  # "sum" to total across industries, or "mean" for average
                fill_value=0
            )
            .sort_index(axis=1, ascending=False)
        )

        # 3. Sort by latest trading date descending and flatten index
        latest_dt = df_pivoted.columns[0]
        df_pivoted = df_pivoted.sort_values(by=latest_dt, ascending=False).reset_index()
        df_pivoted.columns.name = None

        logging.info("Sector Statistics:\n" + df_pivoted.iloc[:5, :10].to_string())
        return df_pivoted    
    
    def doIndustryStatistic02(self, config: Dict[str, Any]) -> None:
        df = self.populateIndustryStatistics02(config)
        self._export_to_gsheet(df, config["STATISTICS_INDUSTRY_ANALYSIS_MOMENTUM"])      

        df = self.populateSectorStatistics02(config)
        self._export_to_gsheet(df, config["STATISTICS_SECTOR_ANALYSIS_MOMENTUM"]) 

    # -------------------------------------------------------------------------
    # StockCharts ETF Relative Strength
    # -------------------------------------------------------------------------
    def populateStockChartsETFRelativeStrength(self, config: Dict[str, Any]) -> None:
        sql_query = config["STATISTICS_ETF_ANALYSIS"]["SQL.01"]
        df_raw = self.dbHelper.readDataFrame(sql_query)
        if df_raw.empty:
            logging.info("⚠️ No data returned for StockCharts ETF RS. Skipping.")
            return

        df_pivoted = (
            df_raw.pivot_table(index=["symbol", "name"], columns="dt", values="sctr")
            .fillna(0)
            .sort_index(axis=1, ascending=False)
        )

        latest_dt = df_pivoted.columns[0]
        df_pivoted = df_pivoted.sort_values(by=latest_dt, ascending=False).reset_index()
        df_pivoted.columns.name = None

        logging.info("Stock chart ETF RS Statistics:\n" + df_pivoted.iloc[:5, :10].to_string())
        self._export_to_gsheet(df_raw, config["STATISTICS_INDUSTRY_ANALYSIS"])

    # -------------------------------------------------------------------------
    # PostgreSQL / Aiven Integration
    # -------------------------------------------------------------------------
    def populateAvien(self, config: Dict[str, Any]) -> None:
        sql_main = config["AVIEN"]["SQL.01"]
        df_main = self.dbHelper.readDataFrame(sql_main)

        # Drop columns not present in Aiven target schema
        cols_to_drop = [col for col in ["sctr", "normalise_rs", "rs"] if col in df_main.columns]
        if cols_to_drop:
            df_main.drop(columns=cols_to_drop, inplace=True)

        self.push_df_to_aiven("daily_stock_stats", df_main)

    def push_df_to_aiven(self, table_name: str, df: pd.DataFrame) -> None:
        """Clears target Aiven table and uploads DataFrame records in batch."""
        if df.empty:
            return

        records = df.to_dict(orient="records")
        columns = list(records[0].keys())

        columns_formatted = ", ".join(columns)
        placeholders_formatted = ", ".join([f"%({c})s" for c in columns])
        sql_insert = f"INSERT INTO {table_name} ({columns_formatted}) VALUES ({placeholders_formatted});"

        with psycopg.connect(self.avienUri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name};")
                cur.executemany(sql_insert, records)
            conn.commit()

        logging.info(f"Successfully pushed {len(records)} rows to Aiven [{table_name}]")