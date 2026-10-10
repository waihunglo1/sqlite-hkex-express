import yfinance as yf


def get_latest_trading_date_yf(ticker_symbol: str = "0005.HK") -> str:
    """Fetches the latest trading session date from Yahoo Finance for a given ticker."""
    ticker = yf.Ticker(ticker_symbol)

    # Fetch 5 days of history to cover weekends and multi-day holidays
    df = ticker.history(period="5d")

    if df.empty:
        raise ValueError(
            f"No data returned from Yahoo Finance for ticker '{ticker_symbol}'."
        )

    # Extract the timestamp of the last available row
    last_trading_dt = df.index[-1]

    # Format as YYYYMMDD string (e.g., '20261009')
    return last_trading_dt.strftime("%Y%m%d")


# Usage
if __name__ == "__main__":
    # HKEX stock (e.g., HSBC 0005.HK)
    hk_trading_date = get_latest_trading_date_yf("^HSI")
    print(f"Latest HKEX Trading Date: {hk_trading_date}")

    hk_trading_date = get_latest_trading_date_yf("2800.HK")
    print(f"Latest HKEX Trading Date: {hk_trading_date}")    

    # US Market (e.g., SPY or AAPL)
    us_trading_date = get_latest_trading_date_yf("^GSPC")
    print(f"Latest US Trading Date:   {us_trading_date}")