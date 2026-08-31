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