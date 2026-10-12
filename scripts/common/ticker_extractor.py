import pandas as pd
import os
import logging

from pathlib import Path
from . import utility as helper

class TickerExtractor():
    def __init__(self, config, dbHelper):
        self.dbHelper = dbHelper
        self.config = config

    def downloadStockListFile(self):
        # download hkex stock list
        targetUrl = self.config["TICKERS"]["URL"]
        downloadPath = self.config["TICKERS"]["DOWNLOAD_PATH"]
        helper.downloadByChrome(targetUrl, downloadPath)     

    def tickersFromXls(self):
        downloadPath = self.config["TICKERS"]["DOWNLOAD_PATH"]
        fileName = self.config["TICKERS"]["LIST_OF_SECURITIES"]
        current_dir = Path.cwd()
        targetPath = os.path.join(str(current_dir), downloadPath)
        save_path = os.path.join(targetPath, fileName)

        # Define converters to force the first 3 columns (by index 0, 1, 2) to string
        df = pd.read_excel(
            save_path,
            skiprows=2,
            usecols=[0, 1, 2],
            converters={0: str, 1: str, 2: str}
        )

        filterDf = df[df.iloc[:, 2].isin(['股本', '交易所買賣產品'])]

        # Clean and format the first column without lambda
        clean_codes = filterDf.iloc[:, 0].str.split('.').str[0].str.strip().str.lstrip('0')

        # Pad with leading zeros to 4 digits and add .HK
        filterDf.iloc[:, 0] = clean_codes.str.zfill(4) + '.HK'
        column_hashmap = dict(zip(filterDf.iloc[:, 0], filterDf.iloc[:, 1]))
        # tickerList = filterDf.iloc[:, 0].tolist()
        return column_hashmap

    def extractTickerMapFromHkex(self):
        self.downloadStockListFile()
        tickerMap = self.tickersFromXls()
        logging.info(f"XLS TICKERS SIZE : {len(tickerMap)}")

        return tickerMap    

    def usTickerFromGitAte329(self):
        url = self.config["TICKERS"]["URL"]
        df = pd.read_csv(url)
        logging.info("\n" + df.head(5).to_string())
        rowUpdated = self.dbHelper.insertOrReplaceStockInfoTickerMap(df)
        logging.info(f"URL : {url}")  
        logging.info(f"Stock Info ({len(df)}) rows updated : {rowUpdated}")

        column_hashmap = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
        return column_hashmap

    def tickerMap(self):
        if self.config["PARAMETER"]["MARKET"] == "us":
            return self.usTickerFromGitAte329()
        else:
            return self.extractTickerMapFromHkex()