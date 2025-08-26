import yfinance as yf
import pandas as pd

def fetch_crypto_data(symbol, start, end):
    df = yf.download(symbol, start=start, end=end)
    df.reset_index(inplace=True)
    df['symbol'] = symbol
    return df
