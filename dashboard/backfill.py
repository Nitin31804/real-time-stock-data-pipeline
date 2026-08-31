import os
import time
import requests
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
# Fallback symbols if env var is missing during execution
STOCK_SYMBOLS = [s.strip() for s in os.getenv("STOCK_SYMBOLS", "AAPL,GOOGL,MSFT,AMZN,TSLA,NVDA,META,NFLX,AMD,INTC,JPM,V,WMT,JNJ,PG,MA,UNH,HD,BAC,DIS").split(",")]

def main():
    print(f"Connecting to Postgres at {POSTGRES_HOST}/{POSTGRES_DB}...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("Wiping existing mock data...")
    cur.execute("TRUNCATE stock_ohlcv_1m CASCADE;")
    cur.execute("TRUNCATE raw_stock_ticks CASCADE;")
    conn.commit()

    print(f"Fetching historical data for {len(STOCK_SYMBOLS)} symbols using Yahoo Finance raw API...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for symbol in STOCK_SYMBOLS:
        print(f"Fetching {symbol}...", end=" ", flush=True)
        yf_symbol = symbol.replace(".", "-")
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_symbol}?range=5d&interval=1m"
        
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                res = data.get("chart", {}).get("result", [])
                if res:
                    timestamps = res[0].get("timestamp", [])
                    quote = res[0].get("indicators", {}).get("quote", [{}])[0]
                    opens = quote.get("open", [])
                    highs = quote.get("high", [])
                    lows = quote.get("low", [])
                    closes = quote.get("close", [])
                    volumes = quote.get("volume", [])
                    
                    records = []
                    for i in range(len(timestamps)):
                        # Skip nulls
                        if closes[i] is None: continue
                        
                        bucket = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
                        records.append((
                            symbol, bucket, 
                            float(opens[i]), float(highs[i]), float(lows[i]), float(closes[i]), 
                            int(volumes[i]), float(closes[i]), 1
                        ))
                    
                    if records:
                        query = """
                            INSERT INTO stock_ohlcv_1m (symbol, bucket_start, open, high, low, close, volume, vwap, trade_count)
                            VALUES %s
                            ON CONFLICT (symbol, bucket_start) DO NOTHING;
                        """
                        execute_values(cur, query, records)
                        conn.commit()
                        print(f"Inserted {len(records)} candles.")
                    else:
                        print("No records to insert.")
                else:
                    print("No results in JSON.")
            else:
                print(f"HTTP {resp.status_code}")
        except Exception as e:
            print(f"Failed to fetch {symbol}: {e}")
            
        time.sleep(1)

    cur.close()
    conn.close()
    print("Backfill complete.")

if __name__ == "__main__":
    main()
