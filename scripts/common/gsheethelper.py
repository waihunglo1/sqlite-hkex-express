import os
import certifi
import logging
import gspread
import pandas as pd
import sys

os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where()

def cleanPayload(full_payload):
    # --- 新增：處理 full_payload 資料，防止特定欄位變成超連結 ---
    # 假設你想處理整張表，或者你可以加入 if 條件只針對特定欄位索引（例如 col_idx == 2）
    cleaned_payload = []
    for row in full_payload:
        cleaned_row = []
        for col_idx, item in enumerate(row):
            item_str = str(item) if item is not None else ""
            # 如果內容以 http 開頭，或包含你想防止變超連結的文字
            if item_str.startswith("http://") or item_str.startswith("https://") or item_str.endswith(".HK"):
                cleaned_row.append(f'=TEXT("{item_str}", "@")')
            else:
                cleaned_row.append(item)
        cleaned_payload.append(cleaned_row)
    # --------------------------------------------------------    
    return cleaned_payload

def publish_gsheet(df, file, tabName):
    headers = df.columns.tolist()
    data_rows = df.values.tolist()
    data_to_upload = data_rows
    full_payload = [headers] + data_rows
    clean_payload = cleanPayload(full_payload)

    logging.info("資料預覽（前 5 行）：")
    logging.info(f"\n{df.head()}")   

    # 2. 連接 Google Sheets 並寫入資料
    logging.info("正在連接 Google Sheets...")
    try:
        gc = gspread.service_account(filename='.service_account.json')
        sh = gc.open(file)

        # 開啟指定工作表，若未指定則開啟第一個
        worksheet = (
            sh.worksheet(tabName) if tabName else sh.sheet1
        )

        logging.info("正在清除舊資料...")
        worksheet.clear()  # 寫入新數據前，先清空整個工作表

        # 計算範圍（例如：A1 到 P100）
        # gspread v6+ 推薦語法：range 在前，data 在後
        num_rows = len(clean_payload)
        num_cols = len(headers)
        logging.info(f"cols : {num_cols} row: {num_rows}")

        # 將欄位索引（數字）轉換為 Excel 欄位字母（例如 16 欄 = P）
        end_col_letter = gspread.utils.rowcol_to_a1(1, num_cols)[:-1]
        target_range = f"A1:{end_col_letter}{num_rows}"

        logging.info(f"正在批次更新數據到範圍 {target_range}...")
        worksheet.update(
            range_name=target_range, 
            values=clean_payload,
            value_input_option='USER_ENTERED'
        )

        logging.info(f"🎉 資料已成功同步至 Google Sheets！ {file} / {tabName}")

    except Exception as e:
        logging.error(f"發生錯誤：{e}  {file} / {tabName}")

def fetch_and_populate(conn, sql, funcNames):
    try:
        logging.info("正在從 DB 讀取資料...")

        # --- PANDAS 優化：直接讀取為 DataFrame，保持真實的數據型態 (int, float, object) ---
        df = pd.read_sql_query(sql, conn)
        if df.empty:
            logging.error("未找到任何資料。")
            return None   

        funcNamesList = splitStringToArray(funcNames)
        for funcName in funcNamesList:
            df = call_main_function(funcName, df, conn)

        # 2. 資料清洗：將 SQLite 的 None (在 Pandas 中為 NaN) 轉成空字串 ""
        # 這樣既能保持數值欄位的真實數值型態，又不會在寫入 Google Sheets 時出錯
        processed_df = df.fillna("")
        return processed_df  
    except Exception as e:
        logging.error(f"資料庫查詢失敗: {e}")
        return None
        
def call_main_function(func_name: str, *args, **kwargs):
    """Dynamically calls a function in main.py by name."""
    main_module = sys.modules.get("__main__")

    # Get the function attribute from main.py
    func = getattr(main_module, func_name, None)

    if callable(func):
        return func(*args, **kwargs)
    else:
        raise AttributeError(
            f"Function '{func_name}' not found or not callable in main.py"
        )         

def splitStringToArray(input):
    if isinstance(input, str) and input:
        names = [name.strip() for name in input.split(",")]
        return names
    else:
        logging.error("設定檔錯誤：'run_mode' 為空或格式不正確。")
        return None                