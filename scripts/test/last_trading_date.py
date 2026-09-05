from datetime import datetime, timedelta
import yfinance as yf

# Download a short window of recent market data for a major ticker
ticker = yf.Ticker("^GSPC") # S&P 500 Index
recent_data = ticker.history(period="5d")

# Extract the date of the very last row in the DataFrame
last_trading_date = recent_data.index[-1].strftime('%Y-%m-%d')

print(f"The last US trading date was: {last_trading_date}")