import time
import os
import utility as util
from datetime import date, timedelta


def downloadLastDays(targetDays):
  # Get today's date
  today = date.today()

  # Generate the last 5 dates (including today)
  last_5_dates = [today - timedelta(days=i) for i in range(targetDays)]

  for dt in last_5_dates:
    if(util.isWeekDay(dt)):
        dateStr = dt.strftime("%y%m%d")
        util.downloadHtm("d" + dateStr + "e.htm")


#Main program
util.removeHistorialFiles()
downloadLastDays(7)  
