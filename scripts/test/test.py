import gspread
import os
import certifi
import logging
from translate import Translator
import pandas as pd


# 1. 設定日誌格式：包含 [時間] [層級] 檔案名稱:行數 - 訊息
logging.basicConfig(
    level=logging.INFO,  # 設定最低捕捉層級
    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)04d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 精簡時間格式
)

def dummy():
    gc = gspread.service_account(filename='.service_account.json')
    sh = gc.open("HK-STOCKS-ANALYSIS-01")
    # sh.update_acell("A1", "Hello World")

def moveCol():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
    print(df.to_string())

    # 1. Move 'C' to the first position (Index 0)
    col_c = df.pop("C")
    df.insert(0, "C", col_c)
    print(df.to_string())

    # 2. Move 'A' to the last position
    col_a = df.pop("A")
    df["A"] = col_a
    print(df.to_string())

# translate()
# dummy()    

moveCol()