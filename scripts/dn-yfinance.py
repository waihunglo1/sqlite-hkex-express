import pandas as pd
import yfinance as yf

# Define converters to force the first 3 columns (by index 0, 1, 2) to string
df = pd.read_excel(
    "/Users/user/Downloads/ListOfSecurities_c.xlsx",
    skiprows=3,
    usecols=[0, 1, 2],
    converters={0: str, 1: str, 2: str}
)

# Clean and format the first column without lambda
clean_codes = df.iloc[:, 0].str.split('.').str[0].str.strip().str.lstrip('0')

# Pad with leading zeros to 4 digits and add .HK
df.iloc[:, 0] = clean_codes.str.zfill(4) + '.HK'
print(df)
ticker_list = df.iloc[:, 0].tolist()

# Split ticker_list into batches of 100 items
data = []
batch_size = 100
for i in range(0, len(ticker_list), batch_size):
    batch = ticker_list[i : i + batch_size]

    # Call yfinance for the current batch
    tickers_obj = yf.Tickers(batch)

    # Process your batch here (e.g., fetch market cap, industry)
    for ticker in batch:
        try:
            info = tickers_obj.tickers[ticker].info
            # Extract your data safely
            data.append(
                {
                    "Ticker": ticker,
                    "Name": info.get("longName", "N/A"),
                    "Industry": info.get("industry", "N/A"),
                    "Market Cap": info.get("marketCap", "N/A"),
                }
            )
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

# Convert to DataFrame for easy viewing or exporting
df = pd.DataFrame(data)
print(df)