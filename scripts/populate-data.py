import time
import os
import utility as util
from datetime import date, timedelta
import configparser
import logging
import time
import random
import math
import sys
import sqlitehelper as sqliteUtil
import gspreadhelper as gspreadUtil

# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

#
# Main program
#
config = configparser.ConfigParser()
config.read('config/analyst-data-hk.ini', encoding='utf-8')
sqliteFile = config['SQLITE']['FILE']
logging.info(f"SQLITE : {sqliteFile}") 

def populate(config, id):
    sql = config[id]['SQL']
    tabName = config[id]['TAB_NAME']
    df = sqliteUtil.fetch_and_populate(sqliteFile,sql)
    gspreadUtil.publish_gsheet(df,tabName)

if __name__ == "__main__":
    populate(config,'GOOGLE-SPREADSHEET-01')
    populate(config,'GOOGLE-SPREADSHEET-02')


