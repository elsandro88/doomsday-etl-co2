



import pandas as pd
import requests
import psycopg2
import os
from io import StringIO

print("🔍 Connecting to:", os.environ["DATABASE_URL"])
# Scarica i dati CO2
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
response = requests.get(url)
csv_raw = StringIO(response.text)

df = pd.read_csv(csv_raw, comment='#', header=None,
                 names=["year", "month", "decimal_date", "average", "interpolated", "trend", "days_missing"])

# Conversione sicura numerica
cols = ["average", "interpolated", "trend", "days_missing"]
df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
df = df[df["average"] > 0]

# 🔐 Usa DATABASE_URL (connessione semplificata)
DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)

sql = """
    INSERT INTO climate_co2_data (year, month, average_ppm, interpolated_ppm, trend_ppm, days_missing)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

cur = conn.cursor()
for _, r in df.iterrows():
    cur.execute(sql, (
        int(r["year"]), int(r["month"]), float(r["average"]),
        float(r["interpolated"]), float(r["trend"]), int(r["days_missing"])
    ))

conn.commit()
cur.close()
conn.close()
print("✅ Supabase updated with CO2 data")

