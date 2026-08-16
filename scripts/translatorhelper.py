# 1. 原始對照表
_SECTOR_RAW = {
    "Basic Materials": "基礎材料",
    "Communication Services": "通訊服務",
    "Consumer Cyclical": "非必需消費品",
    "Consumer Defensive": "必需消費品",
    "Energy": "能源",
    "Financial Services": "金融服務",
    "Healthcare": "醫療保健",
    "Industrials": "工業",
    "Real Estate": "房地產",
    "Technology": "科技",
    "Utilities": "公用事業",
    "NONE": "其他"
}

_INDUSTRY_RAW = {
    # --- 第一批行業別 ---
    "Advertising Agencies": "廣告代理",
    "Agricultural Inputs": "農業投入品",
    "Auto & Truck Dealerships": "汽車與卡車經銷商",
    "Auto Manufacturers": "汽車製造商",
    "Auto Parts": "汽車零部件",
    "Banks - Regional": "區域性銀行",
    "Beverages - Non-Alcoholic": "非酒精飲料",
    "Beverages - Wineries & Distilleries": "釀酒廠與蒸餾廠",
    "Biotechnology": "生物技術",
    "Building Products & Equipment": "建築產品與設備",
    "Capital Markets": "資本市場",
    "Chemicals": "化學品",
    "Coking Coal": "焦煤",
    "Communication Equipment": "通訊設備",
    "Computer Hardware": "電腦硬體",
    "Consumer Electronics": "消費電子產品",
    "Credit Services": "信用服務",
    "Diagnostics & Research": "診斷與研究",
    "Drug Manufacturers - General": "一般製藥商",
    "Drug Manufacturers - Specialty & Generic": "專利藥與學名藥製造商",
    "Education & Training Services": "教育與培訓服務",
    "Electrical Equipment & Parts": "電氣設備與零件",
    "Electronic Components": "電子元件",
    "Electronic Gaming & Multimedia": "電子遊戲與多媒體",
    "Engineering & Construction": "工程與建築",
    "Entertainment": "娛樂產業",
    "Farm Products": "農產品",
    "Financial Conglomerates": "金融集團",
    "Gambling": "博弈賭博",
    "Health Information Services": "醫療資訊服務",
    "Household & Personal Products": "家用與個人產品",
    "Information Technology Services": "資訊技術服務",
    "Infrastructure Operations": "基礎設施營運",
    "Insurance - Property & Casualty": "財產及意外傷害保險",
    "Integrated Freight & Logistics": "綜合貨運與物流",
    "Internet Content & Information": "網路內容與資訊",
    "Internet Retail": "網路零售 (電商)",
    "Leisure": "休閒娛樂",
    "Medical Devices": "醫療器材",
    "Medical Distribution": "醫療物資分銷",
    "Medical Instruments & Supplies": "醫療儀器與用品",
    "Oil & Gas Refining & Marketing": "石油與天然氣煉製與行銷",
    "Other Industrial Metals & Mining": "其他工業金屬與採礦",
    "Packaged Foods": "包裝食品",
    "Packaging & Containers": "包裝與容器",
    "Personal Services": "個人服務",
    "Pharmaceutical Retailers": "藥品零售商",
    "Real Estate - Development": "房地產開發",
    "Real Estate Services": "房地產服務",
    "Recreational Vehicles": "休閒娛樂車輛 (RV)",
    "Rental & Leasing Services": "租賃服務",
    "Restaurants": "餐飲店/餐廳",
    "Semiconductors": "半導體",
    "Software - Application": "應用軟體",
    "Software - Infrastructure": "基礎架構軟體",
    "Solar": "太陽能",
    "Specialty Chemicals": "特種化學品",
    "Specialty Industrial Machinery": "特種工業機械",
    "Specialty Retail": "專門零售商",
    "Staffing & Employment Services": "人力資源與就業服務",
    "Steel": "鋼鐵",
    "Tobacco": "菸草",
    "Travel Services": "旅遊服務",
    "Utilities - Regulated Gas": "受管制燃氣公用事業",
    "Utilities - Regulated Water": "受管制水務公用事業",

    # --- 新增的第二批行業別 ---
    "Aerospace & Defense": "航太與國防",
    "Airlines": "航空公司",
    "Airports & Air Services": "機場及航空服務",
    "Aluminum": "鋁業",
    "Apparel Manufacturing": "服裝製造",
    "Apparel Retail": "服飾零售",
    "Asset Management": "資產管理",
    "Banks - Diversified": "綜合型銀行",
    "Banks—Regional": "區域性銀行",  # 處理長破折號變體
    "Beverages - Brewers": "飲料 - 啤酒釀造商",
    "Broadcasting": "廣播電視",
    "Building Materials": "建築材料",
    "Business Equipment & Supplies": "商業設備與用品",
    "Confectioners": "糖果糕點商",
    "Conglomerates": "複合企業 (集團)",
    "Consulting Services": "諮詢服務",
    "Copper": "銅業",
    "Department Stores": "百貨公司",
    "Drug Manufacturers—Specialty & Generic": "專利藥與學名藥製造商", # 處理長破折號變體
    "Electronics & Computer Distribution": "電子與電腦分銷",
    "Farm & Heavy Construction Machinery": "農業與重型建築機械",
    "Financial Data & Stock Exchanges": "金融數據與證券交易所",
    "Food Distribution": "食品分銷",
    "Footwear & Accessories": "鞋類與配飾",
    "Furnishings, Fixtures & Appliances": "家具、固定裝置與家電",
    "Gold": "黃金採礦",
    "Grocery Stores": "雜貨店/超級市場",
    "Home Improvement Retail": "家居裝修零售",
    "Industrial Distribution": "工業品分銷",
    "Insurance - Life": "人壽保險",
    "Insurance - Reinsurance": "再保險",
    "Insurance Brokers": "保險經紀人",
    "Lodging": "住宿/旅館業",
    "Lumber & Wood Production": "伐木與木材生產",
    "Luxury Goods": "奢侈品",
    "Marine Shipping": "海運/航運",
    "Medical Care Facilities": "醫療護理設施",
    "Metal Fabrication": "金屬製品加工",
    "Mortgage Finance": "抵押貸款金融",
    "Oil & Gas Drilling": "石油與天然氣鑽探",
    "Oil & Gas E&P": "石油與天然氣勘探與生產",
    "Oil & Gas Equipment & Services": "石油與天然氣設備與服務",
    "Oil & Gas Integrated": "綜合石油與天然氣",
    "Oil & Gas Midstream": "石油與天然氣中游傳播",
    "Other Precious Metals & Mining": "其他貴金屬與採礦",
    "Paper & Paper Products": "紙與紙製品",
    "Pollution & Treatment Controls": "污染與治理控制",
    "Publishing": "出版業",
    "Railroads": "鐵路運輸",
    "Real Estate - Diversified": "多元化房地產",
    "Real Estate—Development": "房地產開發",  # 處理長破折號變體
    "Real Estate—Diversified": "多元化房地產",  # 處理長破折號變體
    "Residential Construction": "住宅建造",
    "Resorts & Casinos": "度假村與賭場",
    "Scientific & Technical Instruments": "科學與技術儀器",
    "Security & Protection Services": "安全與防護服務",
    "Semiconductor Equipment & Materials": "半導體設備與材料",
    "Shell Companies": "殼公司 (SPAC)",
    "Software—Application": "應用軟體",        # 處理長破折號變體
    "Software—Infrastructure": "基礎架構軟體",    # 處理長破折號變體
    "Specialty Business Services": "專業商業服務",
    "Telecom Services": "電信服務",
    "Textile Manufacturing": "紡織製造",
    "Thermal Coal": "動力煤",
    "Tools & Accessories": "工具與配件",
    "Trucking": "卡車貨運",
    "Uranium": "鈾業",
    "Utilities - Independent Power Producers": "獨立電力生產商",
    "Utilities - Regulated Electric": "受管制電力公用事業",
    "Utilities - Renewable": "再生能源公用事業",
    "Waste Management": "廢棄物管理",
    "UNKNOWN": "未知",
    "NONE": "其他"    
}

# 2. 自動將字典的 Keys 轉換為全大寫 (字典推導式)
SECTOR_TRANSLATION = {k.upper(): v for k, v in _SECTOR_RAW.items()}
INDUSTRY_TRANSLATION = {k.upper(): v for k, v in _INDUSTRY_RAW.items()}

# 3. 安全查詢函數 (自動將輸入值轉為大寫，並移除前後空格)
def financial_term(english_name, term_type="industry"):
    if not english_name or not isinstance(english_name, str):
        return english_name
    
    # 選擇對照字典
    mapping_dict = INDUSTRY_TRANSLATION if term_type == "industry" else SECTOR_TRANSLATION
    
    # 清洗輸入值：轉大寫、剔除首尾空白、並修正長短破折號異同
    lookup_key = english_name.strip().upper().replace("—", "-")
    
    # 查不到時預設回傳原名 (維持原有大小寫)
    return mapping_dict.get(lookup_key, english_name)
