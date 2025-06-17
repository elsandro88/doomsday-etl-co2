# etl/etl_energy_mix.py

import os
import requests
import psycopg2
from datetime import datetime

print("⚡ ETL Energy Mix (LIVE) avviato")

# Config
ZONE = "IT"  # Puoi cambiarlo in "DE", "FR", "ES", ecc.
API_KEY = os.environ["ELECTRICITYMAP_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]

# Fetch
headers = {"auth-token": API_KEY}
url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={ZONE}"

response = requests.get(url, headers=headers)
response.raise_for_status()

data = response.json()
mix = data["data"]["energyMix"]

fossil = mix.get("fossil", 0)
renewable = mix.get("renewable", 0)
other = mix.get("other", 0)
timestamp = datetime.utcnow()

# Store
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("""
    INSERT INTO energy_mix_live (zone, fossil, renewable, other, updated_at)
    VALUES (%s, %s, %s, %s, %s)
""", (ZONE, fossil, renewable, other, timestamp))

conn.commit()
cur.close()
conn.close()

print(f"✅ ETL completato: {ZONE} – Fossile: {fossil}% | Rinnovabili: {renewable}%")
