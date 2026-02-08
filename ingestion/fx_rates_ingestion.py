import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv("config/.env")

# Create DB connection safely
url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
)

engine = create_engine(url)

# FX API (no auth)
API_URL = "https://open.er-api.com/v6/latest/USD"

params = {
    "base": "USD"
}

response = requests.get(API_URL, params=params)
response.raise_for_status()

data = response.json()

from datetime import date

if data.get("result") != "success":
    raise Exception(f"API error: {data}")

rates = data["rates"]
rate_date = date.today().isoformat()

records = []

for quote_currency, rate in rates.items():
    records.append({
        "base_currency": "USD",
        "quote_currency": quote_currency,
        "rate": rate,
        "rate_date": rate_date
    })

df = pd.DataFrame(records)

# Load into raw table
df.to_sql(
    "raw_fx_rates",
    engine,
    if_exists="append",
    index=False
)

print(f"✅ FX rates ingested for {rate_date}")