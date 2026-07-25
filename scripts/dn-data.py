import time
import os
import utility as util
from datetime import date, timedelta
import configparser

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
print(f"hkexConfig : {hkexConfig}") 

# download hkex stock list
util.downloadByChrome(hkexConfig)

# download stock price file
util.removeHistorialFiles(hkexConfig)
downloadLastDays(hkexConfig, 7)  
