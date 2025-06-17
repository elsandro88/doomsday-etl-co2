import pandas as pd
import requests
import psycopg2
import os
from io import StringIO

# Scarica il file NOAA
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
response = requests.get(url)
csv_raw = StringIO(response.text)

# Leggi il file CSV, salta i commenti
df = pd.read_csv(csv_raw, comment='#', header=None,
                 names=["year", "month", "decimal_date", "average", "interpolated", "trend", "days_missing"])

# 🔧 Forza conversione numerica (fix del bug)
cols_to_numeric = ["average", "interpolated", "trend", "days_missing"]
df[cols_to_numeric] = df[cols_to_numeric].apply(pd.to_numeric, errors="coerce")

# Filtra solo righe valide (average > 0)
df = df[df["average"] > 0]

# Connessione a Supabase PostgreSQL (via GitHub secrets)
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASS"],
    port=5432
)

# Inserimento righe (senza duplicati)
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
print("✅ Supabase updated")
