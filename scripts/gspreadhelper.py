import gspread
import os
import certifi
import logging

os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where()

def dummy():
    gc = gspread.service_account(filename='.service_account.json')
    sh = gc.open("MARKET-ANALYSIS").sheet1
    sh.update_acell("A1", "Hello World")

def publish_gsheet(df, tabName):
    headers = df.columns.tolist()
    data_rows = df.values.tolist()
    data_to_upload = data_rows
    full_payload = [headers] + data_rows

    logging.info("資料預覽（前 5 行）：")
    logging.info(f"\n{df.head()}")   

    # 2. 連接 Google Sheets 並寫入資料
    logging.info("正在連接 Google Sheets...")
    try:
        gc = gspread.service_account(filename='.service_account.json')
        sh = gc.open("MARKET-ANALYSIS")

        # 開啟指定工作表，若未指定則開啟第一個
        worksheet = (
            sh.worksheet(tabName) if tabName else sh.sheet1
        )

        logging.info("正在清除舊資料...")
        worksheet.clear()  # 寫入新數據前，先清空整個工作表

        # 計算範圍（例如：A1 到 P100）
        # gspread v6+ 推薦語法：range 在前，data 在後
        num_rows = len(full_payload)
        num_cols = len(headers)
        logging.info(f"cols : {num_cols} row: {num_rows}")

        # 將欄位索引（數字）轉換為 Excel 欄位字母（例如 16 欄 = P）
        end_col_letter = gspread.utils.rowcol_to_a1(1, num_cols)[:-1]
        target_range = f"A1:{end_col_letter}{num_rows}"

        logging.info(f"正在批次更新數據到範圍 {target_range}...")
        worksheet.update(
            range_name=target_range, 
            values=full_payload,
            value_input_option='RAW'
        )

        logging.info("🎉 資料已成功同步至 Google Sheets！")

    except Exception as e:
        logging.error(f"發生錯誤：{e}")