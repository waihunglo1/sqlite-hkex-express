from dataclasses import dataclass, field
import logging
import math
import time
import sys
from typing import Any, Dict, List, Optional, Tuple
from scipy import stats

import numpy as np
import pandas as pd

from . import basedbhelper as dbHelper
from common import utility as helper

class DailyPriceProcessor():
    
    def __init__(self, dbHelper, whereClause):
        self.dbHelper = dbHelper
        self.whereClause = whereClause

    # ----------------------------------------------------------------------
    # Helper Utilities
    # ----------------------------------------------------------------------
    def slope_to_degrees(self, slope: float) -> float:
        """Converts a line slope to degrees."""
        return math.degrees(math.atan(slope))

    def percent_rank_inc(self, data_list: List[float], value: float) -> float:
        """Calculates percentile rank (0.0 to 1.0) for a given value within a dataset."""
        if not data_list or len(data_list) < 2:
            return 0.0
        
        # percentileofscore returns a percentage from 0 to 100, so divide by 100.0
        # kind='weak' includes values equal to 'value' (equivalent to PERCENTRANK.INC)
        return float(stats.percentileofscore(data_list, value, kind="weak") / 100.0)

    def safe_pct_change(
        self, current_val: float, past_val: float, default: float = 0.0
    ) -> float:
        """Calculates percentage change safely avoiding ZeroDivisionError or NaN RuntimeWarnings."""
        if past_val is None or pd.isna(past_val) or past_val <= 0:
            return default
        if current_val is None or pd.isna(current_val):
            return default

        return float((current_val - past_val) / past_val * 100)

    # ----------------------------------------------------------------------
    # Technical Indicators & Statistical Calculations
    # ----------------------------------------------------------------------
    def calculate_ppo_score(
        self, macd01, macd02, macd03
    ) -> float:
        """Calculates PPO score based on 3-day MACD histogram trendline slope."""
        if not (macd01 and macd02 and macd03):
            return 0.0

        if macd01 is None or macd02 is None or macd03 is None:
            return 0.0

        # Linear regression slope over 3 histogram points
        slope, _, _, _, _ = stats.linregress([1, 2, 3], [macd03, macd02, macd01])
        degrees = self.slope_to_degrees(slope)

        if degrees >= 45:
            return 5.0
        elif degrees <= -45:
            return 0.0
        return (slope + 1.0) * 50.0


    def calculate_sctr_base(self, stats_dict: Dict[str, Any]) -> None:
        """Calculates base StockCharts Technical Rank (SCTR) before normalization."""
        close = stats_dict["close"]

        # Long-term indicators (60%)
        if stats_dict["sma200"] > 0:
            stats_dict["sma200pref"] = (
                (close - stats_dict["sma200"]) / stats_dict["sma200"] * 100
            )
            stats_dict["ema200pref"] = (
                (close - stats_dict["ema200"]) / stats_dict["ema200"] * 100
            )

        if not math.isnan(stats_dict.get("roc125", 0)):
            stats_dict["roc125sctr"] = stats_dict["roc125"]

        # Medium-term indicators (30%)
        if stats_dict["sma50"] > 0:
            stats_dict["sma50pref"] = (
                (close - stats_dict["sma50"]) / stats_dict["sma50"] * 100
            )
            stats_dict["ema50pref"] = (
                (close - stats_dict["ema50"]) / stats_dict["ema50"] * 100
            )

        if not math.isnan(stats_dict.get("roc20", 0)):
            stats_dict["roc20sctr"] = stats_dict["roc20"]

        # Short-term indicators (10%)
        if not math.isnan(stats_dict.get("rsi14", 0)):
            stats_dict["rsi14sctr"] = stats_dict["rsi14"]

        stats_dict["ppo01sctr"] = self.calculate_ppo_score(
            stats_dict["macd01"], stats_dict["macd02"], stats_dict["macd03"]
        )

        stats_dict["sctr"] = (
            0.60 * (stats_dict["ema200pref"] + stats_dict["roc125sctr"])
            + 0.30 * (stats_dict["ema50pref"] + stats_dict["roc20sctr"])
            + 0.10 * (stats_dict["ppo01sctr"] + stats_dict["rsi14sctr"])
        )


    def calculate_technical_indicators(
        self, price_history: List[Dict], stats_dict: Dict[str, Any]
    ) -> None:
        """Computes technical indicators using vectorized Pandas series."""
        df = pd.DataFrame(price_history).iloc[::-1].reset_index(drop=True)
        n = len(df)

        closes = df["close"]
        highs = df["high"]
        lows = df["low"]
        turnovers = closes * df["volume"]

        # Moving Averages & Turnovers
        if n >= 3:
            stats_dict["sma03"] = closes.rolling(3).mean().iloc[-1]
        if n >= 5:
            stats_dict["sma05"] = closes.rolling(5).mean().iloc[-1]
            stats_dict["adr05"] = (highs - lows).rolling(5).mean().iloc[-1]
        if n >= 10:
            stats_dict["sma10"] = closes.rolling(10).mean().iloc[-1]
            stats_dict["sma10turnover"] = turnovers.rolling(10).mean().iloc[-1]
        if n >= 20:
            stats_dict["sma20"] = closes.rolling(20).mean().iloc[-1]
            stats_dict["sma20turnover"] = turnovers.rolling(20).mean().iloc[-1]
            stats_dict["adr20"] = (highs - lows).rolling(20).mean().iloc[-1]
            stats_dict["roc20"] = self.safe_pct_change(closes.iloc[-1], closes.iloc[-20])
        if n >= 50:
            stats_dict["sma50"] = closes.rolling(50).mean().iloc[-1]
            stats_dict["ema50"] = closes.ewm(span=50, adjust=False).mean().iloc[-1]
            stats_dict["sma50turnover"] = turnovers.rolling(50).mean().iloc[-1]
        if n >= 100:
            stats_dict["sma100"] = closes.rolling(100).mean().iloc[-1]
        if n >= 125:
            stats_dict["roc125"] = self.safe_pct_change(closes.iloc[-1], closes.iloc[-125])
        if n >= 150:
            stats_dict["sma150"] = closes.rolling(150).mean().iloc[-1]
        if n >= 200:
            stats_dict["sma200"] = closes.rolling(200).mean().iloc[-1]
            stats_dict["ema200"] = (
                closes.ewm(span=200, adjust=False).mean().iloc[-1]
            )

        # RSI 14
        if n >= 14:
            delta = closes.diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            stats_dict["rsi14"] = (100 - (100 / (1 + rs))).iloc[-1]

        # MACD (12, 26, 9)
        if n >= 26:
            ema12 = closes.ewm(span=12, adjust=False).mean()
            ema26 = closes.ewm(span=26, adjust=False).mean()
            macd = ema12 - ema26
            signal = macd.ewm(span=9, adjust=False).mean()
            hist = macd - signal

            for i, key in enumerate(["macd01", "macd02", "macd03"], start=1):
                if n >= 25 + i:
                    stats_dict[key] = hist.iloc[-i];
        
        # Latest Quote Price Updates
        latest = price_history[0]
        stats_dict.update({
            "open": latest["open"],
            "high": latest["high"],
            "low": latest["low"],
            "close": latest["close"],
            "volume": latest["volume"],
        })

        if len(price_history) > 1:
            prev = price_history[1]

            # Convert values to float safely, defaulting to 0.0 if None/NaN
            latest_close = float(latest.get("close") or 0.0)
            prev_close = float(prev.get("close") or 0.0)

            stats_dict.update({
                "prev_open": float(prev.get("open") or 0.0),
                "prev_high": float(prev.get("high") or 0.0),
                "prev_low": float(prev.get("low") or 0.0),
                "prev_close": prev_close,
                "prev_volume": float(prev.get("volume") or 0.0),
            })

            # Safe percentage change calculation avoiding division by zero
            if prev_close > 0:
                stats_dict["chg_pct_1d"] = (
                    (latest_close - prev_close) / prev_close
                ) * 100
            else:
                stats_dict["chg_pct_1d"] = 0.0

        # Above SMA flags
        c = stats_dict["close"]
        for period in [5, 10, 20, 50, 100, 150, 200]:
            sma_val = stats_dict.get(f"sma{period:02d}" if period < 100 else f"sma{period}", 0)
            stats_dict[f"above_{period}d_sma"] = 1 if sma_val and c >= sma_val else 0

        # Lookback changes (5d, 10d, 20d, 50d, 100d)
        c = float(stats_dict.get("close") or 0.0)

        for days in [5, 10, 20, 50, 100]:
            if n >= days:
                past_row = price_history[days - 1]
                past_close = float(past_row.get("close") or 0.0)

                if past_close > 0:
                    stats_dict[f"chg_pct_{days}d"] = (
                        (c - past_close) / past_close
                    ) * 100
                else:
                    stats_dict[f"chg_pct_{days}d"] = 0.0
            else:
                stats_dict[f"chg_pct_{days}d"] = 0.0


    def calculate_volume_profile(self, price_history: List[Dict], stats_dict: Dict):
        subset = price_history[:150]
        if not subset:
            return

        # Extract prices, converting None/missing values safely to 0.0
        highs = np.array([float(x.get("high") or 0.0) for x in subset])
        lows = np.array([float(x.get("low") or 0.0) for x in subset])
        volumes = np.array([float(x.get("volume") or 0.0) for x in subset])

        # Filter out NaN or Infinite values
        valid_mask = np.isfinite(highs) & np.isfinite(lows) & np.isfinite(volumes)
        if not np.any(valid_mask):
            return

        highs, lows, volumes = (
            highs[valid_mask],
            lows[valid_mask],
            volumes[valid_mask],
        )

        if len(highs) == 0:
            return

        min_p, max_p = lows.min(), highs.max()

        # Ensure range is valid and finite
        if (
            np.isnan(min_p)
            or np.isnan(max_p)
            or min_p == max_p
            or min_p <= 0
            or not np.isfinite(min_p)
            or not np.isfinite(max_p)
        ):
            return

        # Vectorized binning using numpy histogram
        counts, bin_edges = np.histogram(
            (highs + lows) / 2, bins=100, weights=volumes
        )

        # Prevent argmax error on empty bins
        if len(counts) == 0 or counts.sum() == 0:
            return

        max_bin_idx = np.argmax(counts)

        stats_dict.update(
            {
                "vp_high": float(bin_edges[max_bin_idx + 1]),
                "vp_low": float(bin_edges[max_bin_idx]),
                "vp_bullish": float(counts[max_bin_idx] / 2),
                "vp_bearish": float(counts[max_bin_idx] / 2),
            }
        )

    def calculateSlope(
        self, 
        price_history: List[Dict],
        stats_dict: Dict[str, Any],
        stats_history: List[Dict],
        period: int = 20,
        target_key_hist: str = "sma020",
        target_key_curr: str = "sma20",
    ) -> float:
        """Calculates SMA angle degrees based on linear regression slope."""
        sliced = stats_history[: period - 1]
        dataPoint = [[ps["dt"], ps.get(target_key_hist, 0)] for ps in sliced]
        dataPoint.insert(0, [stats_dict["dt"], stats_dict.get(target_key_curr, 0)])

        # Extract y-values and clean invalid/missing numbers
        raw_y = [x[1] for x in reversed(dataPoint)]
        clean_y = [
            float(y)
            for y in raw_y
            if y is not None and not np.isnan(float(y)) and not np.isinf(float(y))
        ]

        if len(clean_y) < 2:
            return 0.0

        slope, _, _, _, _ = stats.linregress(range(len(clean_y)), clean_y)
        return self.slope_to_degrees(slope)


    # ----------------------------------------------------------------------
    # Core Processing Engine
    # ----------------------------------------------------------------------
    def calculate_statistics(
        self, 
        symbol: str, 
        query_date: str, 
        warning_list: List[Dict]
    ) -> Optional[Dict[str, Any]]:
        """Calculates all technical metrics and statistics for a single symbol."""

        # price history row
        historySql = "SELECT * FROM DAILY_STOCK_PRICE WHERE symbol = ? AND dt <= ? ORDER BY dt DESC LIMIT 200"
        params = [symbol, query_date]
        history_rows = self.dbHelper.fetchAllRows(historySql, params)

        statsSql = "SELECT * FROM DAILY_STOCK_STATS WHERE symbol = ? AND dt < ? ORDER BY dt DESC LIMIT 200"
        params = [symbol, query_date]
        stats_rows = self.dbHelper.fetchAllRows(statsSql, params)

        price_history = [dict(r) for r in history_rows]
        stats_history = [dict(r) for r in stats_rows]

        if not price_history:
            return None

        # Default template dictionary
        stats_dict = {
            "symbol": symbol,
            "dt": query_date,
            "start_dt": price_history[-1]["dt"],
            "histDay": len(price_history),
            "sma200": 0,
            "sma150": 0,
            "sma100": 0,
            "sma50": 0,
            "sma20": 0,
            "sma10": 0,
            "sma05": 0,
            "sma03": 0,
            "ema200": 0,
            "ema50": 0,
            "roc20": 0,
            "roc125": 0,
            "rsi14": 0,
            "macd01": 0,
            "macd02": 0,
            "macd03": 0,
            "ema200pref": 0,
            "sma200pref": 0,
            "ema50pref": 0,
            "sma50pref": 0,
            "rsi14sctr": 0,
            "ppo01sctr": 0,
            "roc125sctr": 0,
            "roc20sctr": 0,
            "sctr": 0,
            "normalise_rs": 0,
            "ema500pref": 0,
            "rs": 0,
            # ... existing keys ...
            # Add defaults for slope and RS metrics
            "priceOverSMA20": 0.0,
            "slopeSMA20": 0.0,
            "slopeSMA50": 0.0,
            "slopeSMA150": 0.0,
            "slopeAdr05": 0.0,
            "slopeAdr20": 0.0,
            "rs_priceOverSMA20": 0.0,
            "rs_slopeSMA20": 0.0,
            "rs_slopeSMA50": 0.0,
            "rs_slopeSMA150": 0.0,            
        }

        try:
            self.calculate_technical_indicators(price_history, stats_dict)
            self.calculate_sctr_base(stats_dict)
            self.calculate_volume_profile(price_history, stats_dict)
        except Exception as e:
            logging.error(f"❌ [{symbol}] 未知錯誤: {e} / {symbol}")
            sys.exit(1)

        if stats_history:
            # Relative Strength calculation
            slope20 = self.calculateSlope(
                price_history, stats_dict, stats_history, 20, "sma020", "sma20"
            )
            slope50 = self.calculateSlope(
                price_history, stats_dict, stats_history, 50, "sma050", "sma50"
            )
            slope150 = self.calculateSlope(
                price_history, stats_dict, stats_history, 150, "sma150", "sma150"
            )
            slopeAdr05 = self.calculateSlope(
                price_history, stats_dict, stats_history, 5, "adr05", "adr05"
            )
            slopeAdr20 = self.calculateSlope(
                price_history, stats_dict, stats_history, 20, "adr20", "adr20"
            )

            stats_dict["slopeSMA20"] = slope20
            stats_dict["slopeSMA50"] = slope50
            stats_dict["slopeSMA150"] = slope150
            stats_dict["slopeAdr05"] = slopeAdr05
            stats_dict["slopeAdr20"] = slopeAdr20        

            price_over_sma20 = (
                (stats_dict["close"] / stats_dict["sma20"] * 100)
                if stats_dict["sma20"] > 0
                else 0.0
            )

            if stats_dict["sma20"] == 0 or price_over_sma20 > 300:
                warning_list.append({
                    "symbol": symbol,
                    "priceOverSMA20": f"{price_over_sma20:.2f}",
                    "close": stats_dict["close"],
                    "slopeSMA20": f"{slope20:.2f}",
                    "condition": (
                        "sma20 is zero"
                        if stats_dict["sma20"] == 0
                        else "priceOverSMA20 > 300"
                    ),
                })
                price_over_sma20 = 0.0

            stats_dict["priceOverSMA20"] = price_over_sma20

        return stats_dict


    def normalize_relative_strength(
        self,
        query_date: str, 
        price_stats_list: List[Dict[str, Any]]
    ) -> None:
        """Computes cross-sectional percentile rankings across all symbols for a date."""
        if not price_stats_list:
            return

        # Extract lists for percent ranking
        price_over_sma20 = [p.get("priceOverSMA20", 0) for p in price_stats_list]
        slope_sma20 = [p.get("slopeSMA20", 0) for p in price_stats_list]
        slope_sma50 = [p.get("slopeSMA50", 0) for p in price_stats_list]
        slope_sma150 = [p.get("slopeSMA150", 0) for p in price_stats_list]

        long_term = [
            p.get("ema200pref", 0) + p.get("roc125sctr", 0) for p in price_stats_list
        ]
        medium_term = [
            p.get("ema50pref", 0) + p.get("roc20sctr", 0) for p in price_stats_list
        ]
        short_term = [
            p.get("ppo01sctr", 0) + p.get("rsi14sctr", 0) for p in price_stats_list
        ]

        start_time = time.perf_counter()

        # Step 1: Initial Percentile Ranking
        rs_norm_list, sctr_list = [], []
        for ps in price_stats_list:
            ps["rs_priceOverSMA20"] = (
                self.percent_rank_inc(price_over_sma20, ps.get("priceOverSMA20", 0)) * 100
            )
            ps["rs_slopeSMA20"] = (
                self.percent_rank_inc(slope_sma20, ps.get("slopeSMA20", 0)) * 100
            )
            ps["rs_slopeSMA50"] = (
                self.percent_rank_inc(slope_sma50, ps.get("slopeSMA50", 0)) * 100
            )
            ps["rs_slopeSMA150"] = (
                self.percent_rank_inc(slope_sma150, ps.get("slopeSMA150", 0)) * 100
            )

            ps["normalise_rs"] = (
                0.05 * ps["rs_priceOverSMA20"]
                + 0.05 * ps["rs_slopeSMA20"]
                + 0.40 * ps["rs_slopeSMA50"]
                + 0.50 * ps["rs_slopeSMA150"]
            )

            lt_rank = (
                self.percent_rank_inc(
                    long_term, ps.get("ema200pref", 0) + ps.get("roc125sctr", 0)
                )
                * 100
            )
            mt_rank = (
                self.percent_rank_inc(
                    medium_term, ps.get("ema50pref", 0) + ps.get("roc20sctr", 0)
                )
                * 100
            )
            st_rank = (
                self.percent_rank_inc(
                    short_term, ps.get("ppo01sctr", 0) + ps.get("rsi14sctr", 0)
                )
                * 100
            )

            ps["sctr"] = 0.60 * lt_rank + 0.30 * mt_rank + 0.10 * st_rank

            rs_norm_list.append(ps["normalise_rs"])
            sctr_list.append(ps["sctr"])

        # Step 2: Final Rank Normalization
        for ps in price_stats_list:
            ps["normalise_rs_v2"] = (
                self.percent_rank_inc(rs_norm_list, ps["normalise_rs"]) * 100
            )
            ps["sctr"] = self.percent_rank_inc(sctr_list, ps["sctr"]) * 100

        duration = (time.perf_counter() - start_time) * 1000
        logging.info(
            f"Normalization completed for [{query_date}] in {duration:.2f} ms"
        )

    def process_single_date(
        self, query_date: str, query_symbol: Optional[str] = None
    ) -> int:
        """Processes statistics calculation for a specific date and symbol."""
        sql = f"""
            SELECT 
              P.symbol 
            FROM 
              DAILY_STOCK_PRICE P, 
              STOCK S 
            WHERE
              P.symbol = S.symbol 
              AND P.dt = ?
              {self.whereClause}
        """
        params = [query_date]

        if query_symbol:
            sql += " AND P.symbol = ?"
            params.append(query_symbol)

        sql += " ORDER BY P.symbol ASC"

        rows = self.dbHelper.fetchAllRows(sql, params)
        symbols = [r["symbol"] for r in rows]

        warning_list = []
        price_stats_list = []

        total_symbols = len(symbols)
        logging.info(f"Starting processing for {total_symbols} symbols on {query_date}")

        # Print progress every N symbols (e.g., every 100 symbols)
        log_interval = 100

        for idx, sym in enumerate(symbols, start=1):
            stats_dict = self.calculate_statistics(sym, query_date, warning_list)
            if stats_dict:
                price_stats_list.append(stats_dict)

            # Progress log
            if idx % log_interval == 0 or idx == total_symbols:
                pct = (idx / total_symbols) * 100
                logging.info(
                    f"Progress [{query_date}]: {idx}/{total_symbols} ({pct:.1f}%) processed"
                )

        if warning_list:
            logging.warning(
                f"Warnings for [{query_date}]:\n{pd.DataFrame(warning_list).to_string()}"
            )

        if not price_stats_list:
            return 0

        self.normalize_relative_strength(query_date, price_stats_list)
        updated = self.dbHelper.insertOrReplacePriceStats(price_stats_list)
        logging.info(f"Price Stats row : {len(price_stats_list)} / DB Updated : {updated}")
        
        df = pd.DataFrame(price_stats_list)
        logging.info("\n" + df.iloc[:5, :15].to_string())
        logging.info(
            "\nData Statistics Summary:\n"
            + df.describe().T.to_string(float_format="{:.2f}".format)
        )

        return len(price_stats_list)

    def process_data_local(self, queryDate, querySymbol) -> None:
        """Main task execution flow."""

        if queryDate:
            self.process_single_date(queryDate, querySymbol or None)
        else:
            sql = """
                SELECT dt FROM ( 
                    SELECT dt 
                      FROM 
                    DAILY_STOCK_PRICE 
                      GROUP BY dt 
                      ORDER BY dt DESC 
                    LIMIT 200 
                ) EXCEPT 
                    SELECT dt 
                      FROM 
                    DAILY_STOCK_STATS 
                      GROUP BY dt
                ORDER BY dt
            """

            rows = self.dbHelper.fetchAllRows(sql)
            dates = [r["dt"] for r in rows]

            for dt in dates:
                logging.info(f"Start Processing date: {dt}")
                with helper.time_it(f"process_single_date({dt})"):
                    count = self.process_single_date(dt)
                logging.info(f"Completed date: {dt}. Processed records: {count}")

        logging.info("Data processing run completed successfully.")