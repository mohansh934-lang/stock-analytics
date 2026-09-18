import requests
import pandas as pd
import time
import os

API_KEY = "J2B2UR052DDKFJAR"   

symbols = ["AAPL", "MSFT", "GOOG", "TSLA"]


save_path = "data/raw_data/stocks.csv"
os.makedirs(os.path.dirname(save_path), exist_ok=True)

print("API se data maang raha hoon...")

all_data = []

for symbol in symbols:
    print(f"\nFetching {symbol}...")
    
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    
    response = requests.get(url)
    data = response.json()
    
    if "Time Series (Daily)" not in data:
        print(f"Error fetching data for {symbol}:", data.get('Note', 'Unknown error'))
        continue
        
    ts = data["Time Series (Daily)"]

    df = pd.DataFrame(ts).T
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df = df.astype(float)

    df.index = pd.to_datetime(df.index)
    df.index.name = 'date'

    df = df.sort_index()
    df['symbol'] = symbol

    all_data.append(df)
    time.sleep(12)  # Alpha Vantage free tier allows 5 requests per minute

#combine all data into a single DataFrame
new_df = pd.concat(all_data)

#agar file hai to alredy merge kar do
if os.path.exists(save_path):
    old_df = pd.read_csv(save_path, parse_dates=['date'])
    old_df['date'] = pd.to_datetime(old_df['date'])
    old_df.set_index('date', inplace=True)
    
    combined = pd.concat([old_df, new_df])
    combined = combined[~combined.index.duplicated(keep='last')]
    combined.to_csv(save_path)
    print(f"\nData appended! Total unique records: {len(combined)}")
else:
    new_df.to_csv(save_path)
    print(f"\nData saved! Total records: {len(new_df)}")
