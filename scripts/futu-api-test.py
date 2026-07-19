from futu import *

# 1. 建立行情連線（預設 OpenD IP 為本機，埠號為 11111）
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

try:
    # --- 介面 1：獲取板塊列表 (Get Plate List) ---
    # 參數：Market.HK (港股), PlateClass.INDUSTRY (行業板塊)
    ret, df_plate_list = quote_ctx.get_plate_list(Market.HK, PlateClass.INDUSTRY)
    
    if ret == RET_OK:
        print("\n=== 港股行業板塊列表（前 5 筆） ===")
        print(df_plate_list[['code', 'plate_name']].head())
    else:
        print(f"獲取板塊列表失敗: {df_plate_list}")

    # --- 介面 2：獲取板塊內的股票列表 (Get Plate Stock) ---
    # 這裡以「半導體」或特定板塊代碼為例（請替換為實際獲取到的板塊代碼）
    target_plate = 'HK.BK1026'  # 範例代碼，實際代碼請從介面 1 獲取
    ret, df_plate_stocks = quote_ctx.get_plate_stock(target_plate)
    
    if ret == RET_OK:
        print(f"\n=== 板塊 {target_plate} 內的股票列表（前 5 筆） ===")
        print(df_plate_stocks[['code', 'stock_name', 'lot_size']])
    else:
        print(f"獲取板塊股票失敗: {df_plate_stocks}")

    # --- 介面 3：獲取股票所屬的板塊 (Get Owner Plate) ---
    # 查詢 騰訊控股 (HK.00700) 所屬的板塊
    ret, df_owner_plates = quote_ctx.get_owner_plate(['HK.00700'])
    
    if ret == RET_OK:
        print("\n=== 騰訊控股 (HK.00700) 所屬板塊（前 5 筆） ===")
        print(df_owner_plates[['code', 'plate_code', 'plate_name']].head())
    else:
        print(f"獲取股票所屬板塊失敗: {df_owner_plates}")

finally:
    # 2. 記得關閉連線，釋放資源
    quote_ctx.close()
