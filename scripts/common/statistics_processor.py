import logging
import pandas as pd
import yfinance as yf
import psycopg
from . import basedbhelper as dbHelper

class StatisticsProcessor():

    def __init__(self, dbHelper, avienUri, indexes=["^HSI", "^HSCE"]):
        self.dbHelper = dbHelper
        self.indexes = indexes
        self.avienUri = avienUri

    def loadIndexDataByYahooFinance(self):
        start_date = "2006-10-13"

        for index_symbol in self.indexes:
            df = yf.download(
                index_symbol,
                start=start_date,
                interval="1d",
                progress=False,
                multi_level_index=False,  # Forces 1D columns
            )

            if df.empty:
                continue

            df = df.reset_index()

            # Vectorized column mapping
            records = pd.DataFrame(
                {
                    "symbol": index_symbol,
                    "period": "D",
                    "dt": df["Date"].dt.strftime("%Y%m%d"),
                    "tm": "000000",
                    "open": df["Open"],
                    "high": df["High"],
                    "low": df["Low"],
                    "close": df["Close"],
                    "volume": df["Volume"],
                    "adj_close": df["Close"],
                    "open_int": 0,
                }
            )

            logging.info("\n" + records.tail(5).to_string())

            rows = records.to_dict(orient="records")
            updatedRows = self.dbHelper.insertDailyStockPrice(rows)
            logging.info(f"Yahoo indexes Processed[{index_symbol}] : {len(rows)} / Updated : {updatedRows}")

    def newSectorStats(self) -> dict:
        return {
            "dt": "",
            "XLB_U4SM": 0,
            "XLB_D4SM": 0,
            "XLB_SM": 0,
            "XLC_U4SM": 0,
            "XLC_D4SM": 0,
            "XLC_SM": 0,
            "XLY_U4SM": 0,
            "XLY_D4SM": 0,
            "XLY_SM": 0,
            "XLP_U4SM": 0,
            "XLP_D4SM": 0,
            "XLP_SM": 0,
            "XLE_U4SM": 0,
            "XLE_D4SM": 0,
            "XLE_SM": 0,
            "XLF_U4SM": 0,
            "XLF_D4SM": 0,
            "XLF_SM": 0,
            "XLV_U4SM": 0,
            "XLV_D4SM": 0,
            "XLV_SM": 0,
            "XLI_U4SM": 0,
            "XLI_D4SM": 0,
            "XLI_SM": 0,
            "XLRE_U4SM": 0,
            "XLRE_D4SM": 0,
            "XLRE_SM": 0,
            "XLK_U4SM": 0,
            "XLK_D4SM": 0,
            "XLK_SM": 0,
            "XLU_U4SM": 0,
            "XLU_D4SM": 0,
            "XLU_SM": 0,
            "XLX_U4SM": 0,
            "XLX_D4SM": 0,
            "XLX_SM": 0,
        }


    def populateSectorStatistics(self, config):
        sql = config['STATISTICS_PROCESSOR']['SECTOR_STATS_SQL']
        sector_stats = self.dbHelper.fetchAllRows(sql)

        stats = self.newSectorStats()
        last_dt = ""
        count = 0

        sectorStats = []

        for sector_stat in sector_stats:
            current_dt = sector_stat["dt"]
            current_sector = sector_stat["sector"]

            if last_dt != "" and last_dt != current_dt:
                sectorStats.append(stats)
                stats = self.newSectorStats()
                count += 1


            stats["dt"] = current_dt

            # Dynamically set sector fields
            valid_sectors = [
                "XLB",
                "XLC",
                "XLY",
                "XLP",
                "XLE",
                "XLF",
                "XLV",
                "XLI",
                "XLRE",
                "XLK",
                "XLU",
                "XLX",
            ]

            if current_sector in valid_sectors:
                stats[f"{current_sector}_U4SM"] = sector_stat["u4sm"]
                stats[f"{current_sector}_D4SM"] = sector_stat["d4sm"]
                stats[f"{current_sector}_SM"] = sector_stat["sm"]
            else:
                logging.info(
                    f"[ERROR] Unknown sector: {current_sector} on {current_dt}"
                )

            last_dt = current_dt

        # new_sector_status_to_db(last_stats)
        df = pd.DataFrame(sectorStats)
        logging.info("\n" + df.iloc[:5, :10].to_string())
        updatedRow = self.dbHelper.insertOrReplaceSectorRecords(sectorStats)
        logging.info(f"Sectors stats updated. Total records: {count} / Row updated : {updatedRow}")


    def populateMarketStatistics(self, config):
        sql = config['STATISTICS_PROCESSOR']['MARKET_STATS_SQL']
        market_stats = self.dbHelper.fetchAllRows(sql)

        params = [
            (
                ms["dt"],
                ms["up4pct1d"],
                ms["dn4pct1d"],
                ms["up25pctin100d"],
                ms["dn25pctin100d"],
                ms["up25pctin20d"],
                ms["dn25pctin20d"],
                ms["up50pctin20d"],
                ms["dn50pctin20d"],
                ms["noofstocks"],
                ms["above200smapct"],
                ms["above150smapct"],
                ms["above50smapct"],
                ms["above20smapct"],
                ms["hsi"],
                ms["hsce"],
            )
            for ms in market_stats
        ]

        df = pd.DataFrame(params)
        logging.info("\n" + df.head(5).to_string())
        updatedRow = self.dbHelper.insertOrReplaceMarketRecords(params)
        logging.info(f"Market stats updated. Total records: {len(market_stats)} / Row updated : {updatedRow}")

    def populateAvien(self, config):
        df = self.populate_daily_stock_stats(self.dbHelper, config)
        self.push_df_to_aiven("daily_stock_stats", df)

    def push_df_to_aiven(self, table_name: str, df: pd.DataFrame):
        """Clears target Aiven table and uploads DataFrame records in batch."""
        records = df.to_dict(orient="records")
        if not records:
            return

        columns = list(records[0].keys())
        sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([f'%({c})s' for c in columns])});"

        with psycopg.connect(self.avienUri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name};")
                cur.executemany(sql_insert, records)
            conn.commit()
        logging.info(f"Successfully pushed {len(records)} rows to Aiven [{table_name}]")

    def populate_daily_stock_stats(self, dbHelper, config):
            sql_main = config['STATISTICS_PROCESSOR']['AVIEN_STATS_SQL']
            df_main = dbHelper.readDataFrame(sql_main)

            # 4. DROP columns that do NOT exist in the PostgreSQL target schema
            cols_to_drop = [
                col
                for col in ["sctr", "normalise_rs","rs"]
                if col in df_main.columns
            ]
            df_main.drop(columns=cols_to_drop, inplace=True)

            return df_main        
