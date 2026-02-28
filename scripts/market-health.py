import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# Define sectors and top 10 tickers based on market cap as of February 2026
sectors = {
    'Information Technology': ['NVDA', 'AAPL', 'MSFT', 'AVGO', 'TSM', 'ORCL', 'AMD', 'ADBE', 'CRM', 'QCOM'],
    'Consumer Discretionary': ['AMZN', 'TSLA', 'HD', 'MCD', 'LOW', 'BKNG', 'TJX', 'NKE', 'SBUX', 'CMG'],
    'Communication Services': ['GOOGL', 'META', 'NFLX', 'TMUS', 'VZ', 'DIS', 'CMCSA', 'T', 'SPOT', 'EA'],
    'Industrials': ['CAT', 'GE', 'RTX', 'GEV', 'UNP', 'UBER', 'HON', 'ETN', 'UPS', 'DE'],
    'Consumer Staples': ['WMT', 'COST', 'PG', 'KO', 'PEP', 'PM', 'TGT', 'MDLZ', 'CL', 'MO'],
    'Energy': ['XOM', 'CVX', 'COP', 'EOG', 'SLB', 'MPC', 'VLO', 'OXY', 'HES', 'WMB'],
    'Utilities': ['NEE', 'CEG', 'SO', 'DUK', 'SRE', 'AEP', 'D', 'EXC', 'XEL', 'ETR'],
    'Materials': ['LIN', 'SCCO', 'NEM', 'SHW', 'FCX', 'ECL', 'APD', 'CTVA', 'DOW', 'NUE'],
    'Financials': ['JPM', 'V', 'MA', 'BAC', 'WFC', 'GS', 'MS', 'AXP', 'SPGI', 'SCHW'],
    'Health Care': ['LLY', 'UNH', 'JNJ', 'ABBV', 'MRK', 'TMO', 'ABT', 'DHR', 'AMGN', 'PFE'],
    'Real Estate': ['WELL', 'PLD', 'EQIX', 'AMT', 'SPG', 'DLR', 'O', 'PSA', 'RKT', 'VICI']
}

def get_data(ticker):
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=400)).strftime('%Y-%m-%d')
    df = yf.download(ticker, start=start_date, end=end_date)
    if df.empty:
        return None
    return df

def calculate_indicators(df):
    if len(df) < 210:
        return None
    # Moving Averages
    df['sma50'] = df['Close'].rolling(50).mean()
    df['sma200'] = df['Close'].rolling(200).mean()
    bullish_cross = df['sma50'].iloc[-1] > df['sma200'].iloc[-1]
    # Volatility
    df['log_ret'] = np.log(df['Close'] / df['Close'].shift(1))
    vol = df['log_ret'].std() * np.sqrt(252)
    # RSI
    delta = df['Close'].diff(1)
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    last_rsi = rsi.iloc[-1]
    # ADX
    df['tr'] = np.maximum(df['High'] - df['Low'], np.maximum(abs(df['High'] - df['Close'].shift()), abs(df['Low'] - df['Close'].shift())))
    df['dm_plus'] = np.where((df['High'] - df['High'].shift()) > (df['Low'].shift() - df['Low']), df['High'] - df['High'].shift(), 0)
    df['dm_minus'] = np.where((df['Low'].shift() - df['Low']) > (df['High'] - df['High'].shift()), df['Low'].shift() - df['Low'], 0)
    tr_smooth = df['tr'].ewm(span=14, adjust=False).mean()
    dm_plus_smooth = df['dm_plus'].ewm(span=14, adjust=False).mean()
    dm_minus_smooth = df['dm_minus'].ewm(span=14, adjust=False).mean()
    di_plus = 100 * dm_plus_smooth / tr_smooth
    di_minus = 100 * dm_minus_smooth / tr_smooth
    dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
    adx = dx.ewm(span=14, adjust=False).mean()
    last_adx = adx.iloc[-1]
    # Chaikin Money Flow (CMF)
    mfv = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / (df['High'] - df['Low']) * df['Volume']
    mfv = mfv.fillna(0)
    cmf = mfv.rolling(21).sum() / df['Volume'].rolling(21).sum()
    last_cmf = cmf.iloc[-1]
    # 30-day return
    last_30_return = (df['Close'].iloc[-1] / df['Close'].iloc[-30]) - 1 if len(df) >= 30 else 0
    # Volume ratio
    avg_vol_10 = df['Volume'].iloc[-10:].mean()
    avg_vol_50 = df['Volume'].iloc[-50:-10].mean() if len(df) > 50 else avg_vol_10
    vol_ratio = avg_vol_10 / avg_vol_50
    return {
        'bullish_cross': bullish_cross,
        'vol': vol,
        'rsi': last_rsi,
        'adx': last_adx,
        'cmf': last_cmf,
        'return_30d': last_30_return,
        'vol_ratio': vol_ratio
    }

results = {}
for sector, tickers in sectors.items():
    sector_data = []
    for ticker in tickers:
        df = get_data(ticker)
        if df is None:
            print(f"No data for {ticker}")
            continue
        ind = calculate_indicators(df)
        if ind is None:
            print(f"Insufficient data for {ticker}")
            continue
        sector_data.append(ind)
    if sector_data:
        df_sector = pd.DataFrame(sector_data)
        avg_adx = df_sector['adx'].mean()
        avg_rsi = df_sector['rsi'].mean()
        avg_vol = df_sector['vol'].mean()
        pct_bullish = df_sector['bullish_cross'].mean() * 100
        avg_cmf = df_sector['cmf'].mean()
        avg_return = df_sector['return_30d'].mean()
        avg_vol_ratio = df_sector['vol_ratio'].mean()
        results[sector] = {
            'Average ADX': avg_adx,
            'Average RSI': avg_rsi,
            'Average Volatility': avg_vol,
            'Pct Bullish MA Cross': pct_bullish,
            'Average CMF': avg_cmf,
            'Average 30d Return': avg_return,
            'Average Vol Ratio': avg_vol_ratio
        }

df_results = pd.DataFrame.from_dict(results, orient='index')
# Calculate a trend score for ranking sectors
df_results['Trend Score'] = (
    (df_results['Average ADX'] / 10) +
    (df_results['Average CMF'].clip(lower=0) * 10) +
    (df_results['Average 30d Return'] * 10) +
    df_results['Average Vol Ratio'] +
    (df_results['Pct Bullish MA Cross'] / 10) +
    (df_results['Average RSI'] > 50).astype(int)
)

# Sort by trend score descending
df_sorted = df_results.sort_values('Trend Score', ascending=False)

print("Sector Trend Analysis Report")
print("===========================")
print("This report identifies trending sectors based on aggregated technical indicators from top 10 companies in each sector.")
print("Higher Trend Score indicates stronger trend strength, positive money flow, and bullish signals.")
print("\nRanked Sectors by Trend Score:\n")
print(df_sorted[['Trend Score']])

print("\nDetailed Sector Metrics:\n")
print(df_results.to_string())

# Identify the hottest sector
hot_sector = df_sorted.index[0]
print(f"\nThe hottest trending sector currently is: {hot_sector}")
print(f"With Trend Score: {df_sorted['Trend Score'].iloc[0]:.2f}")
print(f"Key Metrics for {hot_sector}:")
print(df_sorted.iloc[0])