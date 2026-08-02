import time
import os
import utility as util
from datetime import date, timedelta
import configparser
import logging
import time
import random
import math

# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

def downloadLastDays(hkexConfig, targetDays):
  # Get today's date
  today = date.today()

  # Generate the last 5 dates (including today)
  last_5_dates = [today - timedelta(days=i) for i in range(targetDays)]

  for dt in last_5_dates:
    if(util.isWeekDay(dt)):
        dateStr = dt.strftime("%y%m%d")
        util.downloadHtm(hkexConfig, "d" + dateStr + "e.htm")

#
# Main program
#
config = configparser.ConfigParser()
config.read('config/analyst-data.ini', encoding='utf-8')
hkexConfig = config['HKEX']
logging.info(f"hkexConfig : {hkexConfig}") 

# download hkex stock list
targetUrl = hkexConfig['URL']
downloadPath = hkexConfig['DOWNLOAD_PATH']
downloadFileName = hkexConfig['listOfSecurities']
util.downloadByChrome(targetUrl, downloadPath)

# download stock price file
util.removeHistorialFiles(hkexConfig)
downloadLastDays(hkexConfig, 7)  
