import requests
import csv
import time
from datetime import datetime, timedelta, timezone

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000  # Binance max limit per request

# --- Configurable parameters (hardcoded for now) ---
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVAL = "1h"
MONTHS_BACK = 6

def get_klines(symbol, interval, start_time, end_time):
    """Fetch klines from Binance API with error handling and retries."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": MAX_LIMIT
    }
    for attempt in range(5):
        try:
            response = requests.get(BINANCE_API_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching {symbol} [{interval}] ({start_time} - {end_time}): {e}")
            time.sleep(2 ** attempt)
    print(f"Failed to fetch data for {symbol} after retries.")
    return []

def write_klines_to_csv(symbol, interval, klines, write_header=False):
    filename = f"{symbol}_{interval}_klines.csv"
    mode = "w" if write_header else "a"
    with open(filename, mode, newline="") as file:
        writer = csv.writer(file)
        if write_header:
            writer.writerow([
                "Open time", "Open", "High", "Low", "Close", "Volume", "Close time",
                "Quote asset volume", "Number of trades", "Taker buy base asset volume",
                "Taker buy quote asset volume", "Ignore"
            ])
        for row in klines:
            writer.writerow(row)

def main():
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=MONTHS_BACK * 30)
    interval_minutes = 60  # For "1h"
    interval_delta = timedelta(minutes=interval_minutes)
    max_span = interval_delta * MAX_LIMIT

    for symbol in SYMBOLS:
        print(f"Downloading {symbol} {INTERVAL} data from {start_time} to {end_time}")
        current_start = start_time
        first_chunk = True
        chunk_count = 0
        while current_start < end_time:
            current_end = min(current_start + max_span, end_time)
            print(f"  Fetching chunk {chunk_count + 1}: {current_start} to {current_end} ...", end="")
            klines = get_klines(symbol, INTERVAL, current_start, current_end)
            if not klines:
                print(" no data or error, stopping.")
                break
            write_klines_to_csv(symbol, INTERVAL, klines, write_header=first_chunk)
            print(f" got {len(klines)} rows.")
            first_chunk = False
            chunk_count += 1
            # Advance to next window (last kline's close time + 1 ms)
            last_open_time = int(klines[-1][0]) / 1000  # Open time in seconds
            current_start = datetime.fromtimestamp(last_open_time, tz=timezone.utc) + interval_delta
            time.sleep(0.5)  # Be nice to the API
        print(f"Finished {symbol}: {chunk_count} chunks downloaded.\n")

if __name__ == "__main__":
    main()
