import duckdb
import logging
import sys
import pandas as pd
from common.basedbhelper import BaseDbHelper

class DuckDbHelper(BaseDbHelper):
    """Helper class for managing database operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.duckConn = duckdb.connect(db_path)

    def insertOrReplaceStockInfo(self, df):
        df["symbol"] = df["symbol"].str.replace("/", "-", regex=False)

        # Clean/map DataFrame columns to match table schema
        # (Handling missing columns safely with default values)
        data_df = pd.DataFrame({
            "symbol": df["symbol"],
            "name": df.get("name", ""),
            "sector": df.get("industry", ""),
            "market_cap": df.get("marketCap", None),
        })

        try:
            # Connect to DuckDB database file
            with duckdb.connect(self.db_path) as conn:
                # Execute batch UPSERT directly from the in-memory Pandas DataFrame 'data_df'
                cursor = conn.execute(
                    """
                    INSERT INTO STOCK (symbol, name, sector, market_cap) 
                    SELECT symbol, name, sector, market_cap FROM data_df
                    ON CONFLICT(symbol) DO UPDATE SET
                        name = EXCLUDED.name,
                        sector = EXCLUDED.sector,
                        market_cap = EXCLUDED.market_cap
                """
                )

                # DuckDB's rowcount returns the total affected/updated rows
                updated = cursor.rowcount if cursor.rowcount != -1 else len(data_df)

        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated

    def fetchTickers(self, whereClause):
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT symbol FROM stock WHERE " + whereClause + " order by symbol")

                # 4. Retrieve and print the results
                symbols = [row[0] for row in cursor.fetchall()]

                if not symbols:
                    logging.info("沒有需要更新的股票。")
                return symbols
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit()
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)    

    def insertDailyStockPrice(self, prices: list) -> int:
        if not prices:
            return 0

        sql = """
            INSERT INTO DAILY_STOCK_PRICE (
                symbol, period, dt, tm, open, high, low, close, volume, adj_close, open_int
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, dt) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                adj_close = EXCLUDED.adj_close
        """

        # If 'prices' is a list of dicts, convert each dict to a tuple ordered by the placeholders
        if isinstance(prices[0], dict):
            param_tuples = [
                (
                    p.get("symbol") or p.get("ticker"),
                    p.get("period", "D"),
                    p.get("dt"),
                    p.get("tm", "000000"),
                    p.get("open"),
                    p.get("high"),
                    p.get("low"),
                    p.get("close"),
                    p.get("volume"),
                    p.get("adj_close"),
                    0,
                )
                for p in prices
            ]
        else:
            param_tuples = prices

        with duckdb.connect(self.db_path) as conn:
            conn.executemany(sql, param_tuples)

        return len(prices)
        
    def insertOrReplaceHistPrice(self, hist) -> int: 
        hist.rename(columns={'symbol': 'ticker'}, inplace=True)
        hist['date'] = (
            pd.to_datetime(hist['date'], utc=True)
            .dt.strftime('%Y%m%d')
        )
        required_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'adjclose', 'volume']
        for col in required_columns:
            if col not in hist.columns:
                hist[col] = None
                
        hist_final = hist[required_columns]

        static_sql = """
            INSERT OR REPLACE INTO daily_stock_price 
            (symbol, period, dt, open, high, low, close, adj_close, volume, open_int) 
            VALUES (?, 'D', ?, ?, ?, ?, ?, ?, ?, 0)
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
                data_tuples = list(hist_final.itertuples(index=False, name=None))
                cursor.executemany(static_sql, data_tuples)
                conn.commit()
                logging.info(f"✅ 成功將 {len(hist_final)} 筆歷史數據以 100% 靜態安全語法更新至資料庫。")

                return len(data_tuples)
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ 寫入資料庫時出錯: {e}")

        return 0 
    
    def insertOrReplaceStockInfo2(self, records) -> int:
        df = pd.DataFrame(records)
        # logging.info("\n" + df.to_markdown(index=False).rstrip)

        updated = 0
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
            
                for _, row in df.iterrows():
                    cursor.execute('''
                        INSERT INTO STOCK (symbol, name, industry, sector, market_cap, industry_en, sector_en, quote_type) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(symbol) DO UPDATE SET
                                name = EXCLUDED.name,
                                industry = EXCLUDED.industry,
                                sector = EXCLUDED.sector,
                                market_cap = EXCLUDED.market_cap,   
                                industry_en = EXCLUDED.industry_en,
                                sector_en = EXCLUDED.sector_en,
                                quote_type = EXCLUDED.quote_type
                    ''', (
                        row['symbol'], row['name'], row['industry'], 
                        row['sector'], row['marketCap'],
                        row['industry_en'], row['sector_en'],
                        row['quoteType']
                        )

                    )
                    # 📜 獲取受影響的行數
                    updated += cursor.rowcount
                
                conn.commit()
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            exit
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            exit

        return updated

    def fetchAllRows(self, sql: str, params: tuple | list | dict = ()):
        cleaned = sql.replace("\r", " ").replace("\n", " ")
        try:
            # Start high-precision timer
            # start_time = time.perf_counter()

            with duckdb.connect(self.db_path) as conn:
                # conn.row_factory = duckdb.Row
                # logging.info(f"Start running sql: {cleaned[:30]}..{cleaned[-20:]}")
                cursor = conn.execute(cleaned, params)
                return cursor.df().to_dict(orient="records")
                # rows = cursor.fetchall()  

                # Calculate elapsed time in milliseconds
                # elapsed_ms = (time.perf_counter() - start_time) * 1000

                # logging.info(
                #    f"⏱️ [{elapsed_ms:.2f} ms] ({len(rows)} rows) | SQL: {cleaned[:30]}..{cleaned[-20:]}"
                # )
                # return rows         
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit(1)  

    def _insertOrReplacePriceStats(self, records) -> int:
        df = pd.DataFrame(records)
        updated = 0
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
            
                # Use SQLite "INSERT OR REPLACE" logic row-by-row
                for _, row in df.iterrows():
                    cursor.execute('''
                        INSERT INTO DAILY_STOCK_STATS (
                            symbol, dt, start_dt, open, high, low, close, volume, 
                            prev_open, prev_high, prev_low, prev_close, prev_volume, 
                            roc020, roc125, rsi014, sma200, sma150, sma100, sma050, sma020, sma010, sma005, sma003, 
                            ema050, ema200, ema200pref, sma200pref, ema500pref, sma50pref, rsi14sctr, ppo01sctr, roc125sctr, sctr, 
                            histDay, chg_pct_1d, chg_pct_5d, chg_pct_10d, chg_pct_20d, chg_pct_50d, chg_pct_100d, sma10turnover, 
                            sma20turnover, sma50turnover, above_200d_sma, above_150d_sma, above_100d_sma, above_50d_sma, 
                            above_20d_sma, above_10d_sma, above_5d_sma, vp_high, vp_low, vp_bullish, vp_bearish,
                            rs, normalise_rs, rs_priceOverSMA20, rs_slopeSMA20, rs_slopeSMA50, rs_slopeSMA150,
                            priceOverSMA20, slopeSMA20, slopeSMA50, slopeSMA150, adr20, adr05, slopeAdr20, slopeAdr05
                        ) 
                        VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        ON CONFLICT (symbol, dt) DO UPDATE SET
                            start_dt = EXCLUDED.start_dt,
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            prev_open = EXCLUDED.prev_open,
                            prev_high = EXCLUDED.prev_high,
                            prev_low = EXCLUDED.prev_low,
                            prev_close = EXCLUDED.prev_close,
                            prev_volume = EXCLUDED.prev_volume,
                            roc020 = EXCLUDED.roc020,
                            roc125 = EXCLUDED.roc125,
                            rsi014 = EXCLUDED.rsi014,
                            sma200 = EXCLUDED.sma200,
                            sma150 = EXCLUDED.sma150,
                            sma100 = EXCLUDED.sma100,
                            sma050 = EXCLUDED.sma050,
                            sma020 = EXCLUDED.sma020,
                            sma010 = EXCLUDED.sma010,
                            sma005 = EXCLUDED.sma005,
                            sma003 = EXCLUDED.sma003,
                            ema050 = EXCLUDED.ema050,
                            ema200 = EXCLUDED.ema200,
                            ema200pref = EXCLUDED.ema200pref,
                            sma200pref = EXCLUDED.sma200pref,
                            ema500pref = EXCLUDED.ema500pref,
                            sma50pref = EXCLUDED.sma50pref,
                            rsi14sctr = EXCLUDED.rsi14sctr,
                            ppo01sctr = EXCLUDED.ppo01sctr,
                            roc125sctr = EXCLUDED.roc125sctr,
                            sctr = EXCLUDED.sctr,
                            histDay = EXCLUDED.histDay,
                            chg_pct_1d = EXCLUDED.chg_pct_1d,
                            chg_pct_5d = EXCLUDED.chg_pct_5d,
                            chg_pct_10d = EXCLUDED.chg_pct_10d,
                            chg_pct_20d = EXCLUDED.chg_pct_20d,
                            chg_pct_50d = EXCLUDED.chg_pct_50d,
                            chg_pct_100d = EXCLUDED.chg_pct_100d,
                            sma10turnover = EXCLUDED.sma10turnover,
                            sma20turnover = EXCLUDED.sma20turnover,
                            sma50turnover = EXCLUDED.sma50turnover,
                            above_200d_sma = EXCLUDED.above_200d_sma,
                            above_150d_sma = EXCLUDED.above_150d_sma,
                            above_100d_sma = EXCLUDED.above_100d_sma,
                            above_50d_sma = EXCLUDED.above_50d_sma,
                            above_20d_sma = EXCLUDED.above_20d_sma,
                            above_10d_sma = EXCLUDED.above_10d_sma,
                            above_5d_sma = EXCLUDED.above_5d_sma,
                            vp_high = EXCLUDED.vp_high,
                            vp_low = EXCLUDED.vp_low,
                            vp_bullish = EXCLUDED.vp_bullish,
                            vp_bearish = EXCLUDED.vp_bearish,
                            rs = EXCLUDED.rs,
                            normalise_rs = EXCLUDED.normalise_rs,
                            rs_priceOverSMA20 = EXCLUDED.rs_priceOverSMA20,
                            rs_slopeSMA20 = EXCLUDED.rs_slopeSMA20,
                            rs_slopeSMA50 = EXCLUDED.rs_slopeSMA50,
                            rs_slopeSMA150 = EXCLUDED.rs_slopeSMA150,
                            priceOverSMA20 = EXCLUDED.priceOverSMA20,
                            slopeSMA20 = EXCLUDED.slopeSMA20,
                            slopeSMA50 = EXCLUDED.slopeSMA50,
                            slopeSMA150 = EXCLUDED.slopeSMA150,
                            adr20 = EXCLUDED.adr20,
                            adr05 = EXCLUDED.adr05,
                            slopeAdr20 = EXCLUDED.slopeAdr20,
                            slopeAdr05 = EXCLUDED.slopeAdr05    
                    ''', (
                        row["symbol"],
                        row["dt"],
                        row["start_dt"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        row["prev_open"],
                        row["prev_high"],
                        row["prev_low"],
                        row["prev_close"],
                        row["prev_volume"],
                        row["roc20"], 
                        row["roc125"],
                        row["rsi14"],
                        row["sma200"],
                        row["sma150"],
                        row["sma100"],
                        row["sma50"],
                        row["sma20"],
                        row["sma10"],
                        row["sma05"],
                        row["sma03"],
                        row["ema50"],   
                        row["ema200"],
                        row["ema200pref"],
                        row["sma200pref"],
                        row["ema500pref"],
                        row["sma50pref"],
                        row["rsi14sctr"],
                        row["ppo01sctr"],
                        row["roc125sctr"],
                        row["sctr"],
                        row["histDay"],  
                        row["chg_pct_1d"],
                        row["chg_pct_5d"],
                        row["chg_pct_10d"],
                        row["chg_pct_20d"],
                        row["chg_pct_50d"],
                        row["chg_pct_100d"],
                        row["sma10turnover"],
                        row["sma20turnover"],  
                        row["sma50turnover"],
                        row["above_200d_sma"],
                        row["above_150d_sma"],
                        row["above_100d_sma"],
                        row["above_50d_sma"],
                        row["above_20d_sma"],
                        row["above_10d_sma"],
                        row["above_5d_sma"],
                        row["vp_high"],
                        row["vp_low"],
                        row["vp_bullish"],
                        row["vp_bearish"],
                        row["rs"],
                        row["normalise_rs"],
                        row["rs_priceOverSMA20"],
                        row["rs_slopeSMA20"],
                        row["rs_slopeSMA50"],
                        row["rs_slopeSMA150"],
                        row["priceOverSMA20"],
                        row["slopeSMA20"],
                        row["slopeSMA50"],
                        row["slopeSMA150"],
                        row["adr20"],
                        row["adr05"],
                        row["slopeAdr20"],
                        row["slopeAdr05"]                                             
                    ))
                    updated += cursor.rowcount
                conn.commit()
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated 

    def insertOrReplacePriceStats(self, records) -> int:
        df = pd.DataFrame(records)
        if df.empty:
            return 0
            
        updated = 0
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 2. Extract and match the DataFrame columns directly into a list of tuples
                # This completely replaces the slow `for _, row in df.iterrows():` loop
                data_to_insert = list(df[[
                    "symbol", "dt", "start_dt", "open", "high", "low", "close", "volume",
                    "prev_open", "prev_high", "prev_low", "prev_close", "prev_volume",
                    "roc20", "roc125", "rsi14", "sma200", "sma150", "sma100", "sma50", "sma20", "sma10", "sma05", "sma03",
                    "ema50", "ema200", "ema200pref", "sma200pref", "ema500pref", "sma50pref", "rsi14sctr", "ppo01sctr", "roc125sctr", "sctr",
                    "histDay", "chg_pct_1d", "chg_pct_5d", "chg_pct_10d", "chg_pct_20d", "chg_pct_50d", "chg_pct_100d", "sma10turnover",
                    "sma20turnover", "sma50turnover", "above_200d_sma", "above_150d_sma", "above_100d_sma", "above_50d_sma",
                    "above_20d_sma", "above_10d_sma", "above_5d_sma", "vp_high", "vp_low", "vp_bullish", "vp_bearish",
                    "rs", "normalise_rs", "rs_priceOverSMA20", "rs_slopeSMA20", "rs_slopeSMA50", "rs_slopeSMA150",
                    "priceOverSMA20", "slopeSMA20", "slopeSMA50", "slopeSMA150", "adr20", "adr05", "slopeAdr20", "slopeAdr05"
                ]].itertuples(index=False, name=None))
                
                # 3. Native bulk upsert via executemany
                cursor.executemany('''
                    INSERT INTO DAILY_STOCK_STATS (
                        symbol, dt, start_dt, open, high, low, close, volume, 
                        prev_open, prev_high, prev_low, prev_close, prev_volume, 
                        roc020, roc125, rsi014, sma200, sma150, sma100, sma050, sma020, sma010, sma005, sma003, 
                        ema050, ema200, ema200pref, sma200pref, ema500pref, sma50pref, rsi14sctr, ppo01sctr, roc125sctr, sctr, 
                        histDay, chg_pct_1d, chg_pct_5d, chg_pct_10d, chg_pct_20d, chg_pct_50d, chg_pct_100d, sma10turnover, 
                        sma20turnover, sma50turnover, above_200d_sma, above_150d_sma, above_100d_sma, above_50d_sma, 
                        above_20d_sma, above_10d_sma, above_5d_sma, vp_high, vp_low, vp_bullish, vp_bearish,
                        rs, normalise_rs, rs_priceOverSMA20, rs_slopeSMA20, rs_slopeSMA50, rs_slopeSMA150,
                        priceOverSMA20, slopeSMA20, slopeSMA50, slopeSMA150, adr20, adr05, slopeAdr20, slopeAdr05
                    ) 
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    ON CONFLICT (symbol, dt) DO UPDATE SET
                        start_dt = EXCLUDED.start_dt,
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        prev_open = EXCLUDED.prev_open,
                        prev_high = EXCLUDED.prev_high,
                        prev_low = EXCLUDED.prev_low,
                        prev_close = EXCLUDED.prev_close,
                        prev_volume = EXCLUDED.prev_volume,
                        roc020 = EXCLUDED.roc020,
                        roc125 = EXCLUDED.roc125,
                        rsi014 = EXCLUDED.rsi014,
                        sma200 = EXCLUDED.sma200,
                        sma150 = EXCLUDED.sma150,
                        sma100 = EXCLUDED.sma100,
                        sma050 = EXCLUDED.sma050,
                        sma020 = EXCLUDED.sma020,
                        sma010 = EXCLUDED.sma010,
                        sma005 = EXCLUDED.sma005,
                        sma003 = EXCLUDED.sma003,
                        ema050 = EXCLUDED.ema050,
                        ema200 = EXCLUDED.ema200,
                        ema200pref = EXCLUDED.ema200pref,
                        sma200pref = EXCLUDED.sma200pref,
                        ema500pref = EXCLUDED.ema500pref,
                        sma50pref = EXCLUDED.sma50pref,
                        rsi14sctr = EXCLUDED.rsi14sctr,
                        ppo01sctr = EXCLUDED.ppo01sctr,
                        roc125sctr = EXCLUDED.roc125sctr,
                        sctr = EXCLUDED.sctr,
                        histDay = EXCLUDED.histDay,
                        chg_pct_1d = EXCLUDED.chg_pct_1d,
                        chg_pct_5d = EXCLUDED.chg_pct_5d,
                        chg_pct_10d = EXCLUDED.chg_pct_10d,
                        chg_pct_20d = EXCLUDED.chg_pct_20d,
                        chg_pct_50d = EXCLUDED.chg_pct_50d,
                        chg_pct_100d = EXCLUDED.chg_pct_100d,
                        sma10turnover = EXCLUDED.sma10turnover,
                        sma20turnover = EXCLUDED.sma20turnover,
                        sma50turnover = EXCLUDED.sma50turnover,
                        above_200d_sma = EXCLUDED.above_200d_sma,
                        above_150d_sma = EXCLUDED.above_150d_sma,
                        above_100d_sma = EXCLUDED.above_100d_sma,
                        above_50d_sma = EXCLUDED.above_50d_sma,
                        above_20d_sma = EXCLUDED.above_20d_sma,
                        above_10d_sma = EXCLUDED.above_10d_sma,
                        above_5d_sma = EXCLUDED.above_5d_sma,
                        vp_high = EXCLUDED.vp_high,
                        vp_low = EXCLUDED.vp_low,
                        vp_bullish = EXCLUDED.vp_bullish,
                        vp_bearish = EXCLUDED.vp_bearish,
                        rs = EXCLUDED.rs,
                        normalise_rs = EXCLUDED.normalise_rs,
                        rs_priceOverSMA20 = EXCLUDED.rs_priceOverSMA20,
                        rs_slopeSMA20 = EXCLUDED.rs_slopeSMA20,
                        rs_slopeSMA50 = EXCLUDED.rs_slopeSMA50,
                        rs_slopeSMA150 = EXCLUDED.rs_slopeSMA150,
                        priceOverSMA20 = EXCLUDED.priceOverSMA20,
                        slopeSMA20 = EXCLUDED.slopeSMA20,
                        slopeSMA50 = EXCLUDED.slopeSMA50,
                        slopeSMA150 = EXCLUDED.slopeSMA150,
                        adr20 = EXCLUDED.adr20,
                        adr05 = EXCLUDED.adr05,
                        slopeAdr20 = EXCLUDED.slopeAdr20,
                        slopeAdr05 = EXCLUDED.slopeAdr05    
                ''', data_to_insert)
                
                updated = cursor.rowcount
                conn.commit()
                
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)
            
        return updated

    def insertOrReplaceSectorRecords(self, records) -> int:
        df = pd.DataFrame(records)
        if df.empty:
            return 0

        columns_order = [
            "dt", 
            "XLB_U4SM",  "XLB_D4SM",  "XLB_SM", 
            "XLC_U4SM",  "XLC_D4SM",  "XLC_SM", 
            "XLY_U4SM",  "XLY_D4SM",  "XLY_SM", 
            "XLP_U4SM",  "XLP_D4SM",  "XLP_SM", 
            "XLE_U4SM",  "XLE_D4SM",  "XLE_SM", 
            "XLF_U4SM",  "XLF_D4SM",  "XLF_SM", 
            "XLV_U4SM",  "XLV_D4SM",  "XLV_SM", 
            "XLI_U4SM",  "XLI_D4SM",  "XLI_SM", 
            "XLRE_U4SM", "XLRE_D4SM", "XLRE_SM", 
            "XLK_U4SM",  "XLK_D4SM",  "XLK_SM", 
            "XLU_U4SM",  "XLU_D4SM",  "XLU_SM", 
            "XLX_U4SM",  "XLX_D4SM",  "XLX_SM"
        ]

        updated = 0
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Extract structured memory tuples instantly
                data_to_insert = list(df[columns_order].itertuples(index=False, name=None))
                
                # Generate the dynamic UPDATE clause so you don't have to write 36 columns manually
                update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns_order if col != "dt"])
                placeholders = ", ".join(["?"] * len(columns_order))
                columns_str = ", ".join(columns_order)

                upsert_sql = f"""
                    INSERT INTO DAILY_SECTORS_STATS ({columns_str})
                    VALUES ({placeholders})
                    ON CONFLICT (dt) DO UPDATE SET {update_clause}
                """
                
                cursor.executemany(upsert_sql, data_to_insert)
                updated = cursor.rowcount
                conn.commit()
                
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated

    def insertOrReplaceMarketRecords(self, records) -> int:
        if not records:
            return 0
            
        # If input is dictionaries or mixed, normalize it into a DataFrame to ensure column order matching
        if isinstance(records, list) and isinstance(records[0], dict):
            df = pd.DataFrame(records)
            columns_order = [
                "dt", "up4pct1d", "dn4pct1d", "up25pctin100d", "dn25pctin100d", 
                "up25pctin20d", "dn25pctin20d", "up50pctin20d", "dn50pctin20d", 
                "noofstocks", "above200smapct", "above150smapct", "above50smapct", 
                "above20smapct", "hsi", "hsce"
            ]
            data_to_insert = list(df[columns_order].itertuples(index=False, name=None))
        else:
            data_to_insert = records

        # DuckDB clean Upsert conversion from SQLite REPLACE
        upsert_sql = """
        INSERT INTO DAILY_MARKET_STATS 
        (dt, up4pct1d, dn4pct1d, up25pctin100d, dn25pctin100d, up25pctin20d, dn25pctin20d, 
        up50pctin20d, dn50pctin20d, noofstocks, above200smapct, above150smapct, above50smapct, above20smapct, hsi, hsce) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (dt) DO UPDATE SET
            up4pct1d = EXCLUDED.up4pct1d,
            dn4pct1d = EXCLUDED.dn4pct1d,
            up25pctin100d = EXCLUDED.up25pctin100d,
            dn25pctin100d = EXCLUDED.dn25pctin100d,
            up25pctin20d = EXCLUDED.up25pctin20d,
            dn25pctin20d = EXCLUDED.dn25pctin20d,
            up50pctin20d = EXCLUDED.up50pctin20d,
            dn50pctin20d = EXCLUDED.dn50pctin20d,
            noofstocks = EXCLUDED.noofstocks,
            above200smapct = EXCLUDED.above200smapct,
            above150smapct = EXCLUDED.above150smapct,
            above50smapct = EXCLUDED.above50smapct,
            above20smapct = EXCLUDED.above20smapct,
            hsi = EXCLUDED.hsi,
            hsce = EXCLUDED.hsce
        """        
        updated = 0
        try:
            with duckdb.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.executemany(upsert_sql, data_to_insert)
                updated = cursor.rowcount
                conn.commit()
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 DuckDB 錯誤: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit(1)

        return updated      

    def callbackWithConn(self, callback, sql, func_name):
        try:
            with duckdb.connect(self.db_path) as conn:
                df = callback(conn, sql, func_name)
                return df           
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit()
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit() 

    def readDataFrame(self, sql_main):
        try:
            with duckdb.connect(self.db_path) as conn:
                df_main = pd.read_sql_query(sql_main, conn)
                return df_main           
        except duckdb.Error as e:
            logging.error(f"❌ ⚫ 其他 SQLite 錯誤: {e}")
            sys.exit()
        except Exception as e:
            logging.error(f"❌ ⚪ 未知錯誤: {e}")
            sys.exit()                     
                          