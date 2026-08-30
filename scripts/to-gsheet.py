import os
from datetime import date, timedelta
import logging
import gspread
import os
import certifi
import logging
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from core import config, sqliteDbHelper, quoteParser
from core import utility as helper
from core import gsheethelper as gsheetHelper

# Create a wrapped function with retry mechanism
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    reraise=True,
)

def fillStocksRelativeStrength(df, conn):   
    logging.info("正在為每檔股票填充前 20 日的歷史 Normalize RS 數據...")    
    records = df.to_dict('records')
    for row in records:
        fill_column(row, conn, "normalise_rs")

    # 4. 將擴充完（多了 20 個欄位）的字典列表，重新轉回 DataFrame
    extended_df = pd.DataFrame(records)  
    return extended_df  

def fillStocksSCTR(df, conn):    
    logging.info("正在為每檔股票填充前 20 日的歷史 SCTR 數據...")    
    records = df.to_dict('records')
    for row in records:
        fill_column(row, conn, "sctr")

    # 4. 將擴充完（多了 20 個欄位）的字典列表，重新轉回 DataFrame
    extended_df = pd.DataFrame(records)  
    return extended_df  

def fill_column(df, conn, columnName):
    sql_rs = f"SELECT {columnName} FROM DAILY_STOCK_STATS WHERE symbol = ? ORDER BY dt DESC LIMIT 20"
    cursor = conn.cursor()
    cursor.execute(sql_rs, (df["symbol"],))
    
    rs_list = [row[0] for row in cursor.fetchall()]
    
    if len(rs_list) < 20:
        logging.info(f"Not enough rs data for {df['symbol']}. Only {len(rs_list)} records found.")
        rs_list += [0] * (20 - len(rs_list))
        
    # 動態寫入 daily_stat["normalise_rs1"] 到 daily_stat["normalise_rs20"]
    for i, val in enumerate(rs_list, start=1):
        df[f"{columnName}{i}"] = val if val is not None else 0        

def _publish_with_retry(df, file, tabName):
    gsheetHelper.publish_gsheet(df, file, tabName)

def moveColumns(df, moveColsToEnd):
    colNames = helper.splitStringToArray(moveColsToEnd) or []
    if len(colNames) > 0:
        for colName in colNames:
            col = df.pop(colName)
            df[colName] = col
        df = df.copy()

def populate(config, id):
    # step1 query data from sqlite file
    sql = config[id]['SQL']
    funcNames = config[id]['FUNCTION_NAME']
    df = sqliteDbHelper.fetch_and_populate(sql, funcNames)

    # move columns to end of dataframe
    moveColsToEnd = config[id]['MOVE_COLS_TO_END']
    moveColumns(df, moveColsToEnd)

    # publish to google-sheet
    targetFile = config[id]['FILE']
    tabName = config[id]['TAB_NAME']
    _publish_with_retry(df, targetFile, tabName)

if __name__ == "__main__":
    # populate(config,'GOOGLE-SPREADSHEET-01')
    populate(config,'GOOGLE-SPREADSHEET-02')