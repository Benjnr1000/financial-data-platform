import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import os
from sqlalchemy.dialects.postgresql import insert

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

from sqlalchemy import text
from datetime import date

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT MAX(rate_date) FROM raw_fx_rates WHERE base_currency = 'USD'")
    ).fetchone()

latest_date = result[0]
today = date.today()

if latest_date == today:
    print("ℹ️ FX rates already ingested for today. Skipping.")
    exit()
    
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

# -----------------------------
# Data quality checks
# -----------------------------
if df["rate"].isnull().any():
    raise ValueError("Null FX rate detected")

if (df["rate"] <= 0).any():
    raise ValueError("Invalid FX rate detected")

# Remove duplicates just in case
df.drop_duplicates(
    subset=["base_currency", "quote_currency", "rate_date"],
    inplace=True
)

# -----------------------------
# Load into raw table
# -----------------------------
from sqlalchemy import Table, MetaData

metadata = MetaData()
raw_fx_rates = Table(
    "raw_fx_rates",
    metadata,
    autoload_with=engine
)

with engine.begin() as conn:
    stmt = insert(raw_fx_rates).values(df.to_dict(orient="records"))
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["base_currency", "quote_currency", "rate_date"]
    )
    conn.execute(stmt)

print(f"✅ FX rates ingested for {rate_date}")