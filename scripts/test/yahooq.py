from yahooquery import Ticker

symbols = ["AAPL", "MSFT", "NVDA"]
tickers = Ticker(symbols)

# Get 5 days of data for all assets
df = tickers.history(period="10d", interval="1d")

# This returns a multi-index DataFrame. You can view it like this:
print(df[["close", "volume"]])

# Pro Tip: Flatten the multi-index to a standard table if you prefer
flat_df = df.reset_index()
print(flat_df.head())

# Instantiate Ticker with a list of symbols
symbols = ["AAPL", "SPY", "AAPL240119C00150000", "0700.HK","QQQ"]
tickers = Ticker(symbols)

# Fetch quote type details
quote_types = tickers.quote_type

for symbol, info in quote_types.items():
    if isinstance(info, dict):
        quote_type = info.get("quoteType")  # e.g., 'EQUITY', 'ETF', 'OPTION'
        short_name = info.get("shortName")
        print(f"Symbol: {symbol:<20} | Type: {quote_type:<10} | Name: {short_name}")
    else:
        print(f"Error fetching {symbol}: {info}")