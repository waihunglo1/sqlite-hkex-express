import pandas as pd
import os
import logging
import yfinance as yf
from . import utility as helper

class IndexDataExtractor():
    def __init__(self, config, dbHelper):
        self.dbHelper = dbHelper
        self.indexes = helper.splitStringToArray(config["YAHOO-FINANCE"]["INDEXES"])

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