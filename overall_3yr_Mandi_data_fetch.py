import requests
import os
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import date, timedelta
from requests.exceptions import ConnectionError, Timeout
from urllib3.exceptions import ProtocolError
from tqdm import tqdm

# Load API key from .env
load_dotenv()
apikey = os.getenv('api-key')

base_url = "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24"
format_type = "json"
limit = 1000

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9"
}

# Date range
# start_date = "18/10/2022"
# end_date = "09/07/2025"

start_date = "11/07/2025"
end_date = "09/06/2026"

start_day, start_month, start_year = map(int, start_date.split("/"))
end_day, end_month, end_year = map(int, end_date.split("/"))

# Generate date strings
def generate_date_strings(start_d, start_m, start_y, end_d, end_m, end_y):
    start = date(start_y, start_m, start_d)
    end = date(end_y, end_m, end_d)
    while start <= end:
        yield start.strftime("%d/%m/%Y")
        start += timedelta(days=1)

# Request with retry logic
def fetch_data_with_retries(url, max_retries=5, sleep_time=5):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ Status {response.status_code} for URL: {url}")
        except (ConnectionError, Timeout, ProtocolError) as e:
            print(f"🔁 Retry {attempt+1}/{max_retries} after error: {str(e)}")
            time.sleep(sleep_time * (attempt + 1))
    return None

# Fetching loop
skipped_dates = []

for date_str in tqdm(list(generate_date_strings(start_day, start_month, start_year, end_day, end_month, end_year)), desc="📅 Fetching by date"):
    offset = 0
    all_records = []
    print(f"\n📅 Fetching for: {date_str}")
    time.sleep(2)

    while True:
        url = f"{base_url}?format={format_type}&api-key={apikey}&limit={limit}&offset={offset}&filters[Arrival_Date]={date_str}"
        data = fetch_data_with_retries(url)

        if data is None:
            print(f"❌ Skipping {date_str} due to repeated connection errors.")
            skipped_dates.append(date_str)
            break

        records = data.get("records", [])
        if not records:
            break

        all_records.extend(records)
        offset += limit

        if len(records) < limit:
            break

    # Save to CSV
    if all_records:
        df = pd.DataFrame(all_records)
        safe_date = date_str.replace("/", "-")
        df.to_csv(f"testing/api_fetched_data/all_food_prices{safe_date}.csv", index=False)
        print(f"✅ Saved {len(df)} records for {date_str}")
    else:
        print(f"⚠️ No data for {date_str}")

# Optional: Save skipped dates
if skipped_dates:
    with open("testing/api_fetched_data/skipped_dates.txt", "w") as f:
        for d in skipped_dates:
            f.write(d + "\n")
    print(f"\n❗ Skipped {len(skipped_dates)} dates. Logged to 'skipped_dates.txt'")
