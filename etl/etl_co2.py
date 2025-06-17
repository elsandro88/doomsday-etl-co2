

import pandas as pd
import requests
import psycopg2
import os
from io import StringIO

print("🔍 Connecting to:", os.environ["DB_HOST"])

# Scarica il CSV CO2 da NOAA
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
response = requests.get(url)
csv_raw = StringIO(response.text)

# Leggi e prepara il DataFrame
df = pd.read_csv(csv_raw, comment='#', header=None,
                 names=["year", "month", "decimal_date", "average", "interpolated", "trend", "days_missing"])

# Forza conversione numerica per evitare errori
cols = ["average", "interpolated", "trend", "days_missing"]
df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
df = df[df["average"] > 0]

# Connessione a Supabase tramite Transaction Pooler (porta 6543)
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASS"],
    port=int(os.environ.get("DB_PORT", 6543)),  # fallback: 6543
    sslmode="require"
)

# Inserimento nel database
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
