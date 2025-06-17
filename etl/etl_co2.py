import pandas as pd
import requests
import psycopg2
import os
from io import StringIO

# 🔽 Scarica il file CO₂ NOAA
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
response = requests.get(url)
csv_raw = StringIO(response.text)

# 🔢 Leggi e assegna nomi colonne
df = pd.read_csv(csv_raw, comment='#', header=None)
df.columns = ["year", "month", "decimal_date", "average", "interpolated", "trend", "days_missing", "uncorrected_avg"]


# 🧹 Conversione sicura dei tipi
cols = ["year", "month", "average", "interpolated", "trend", "days_missing"]
df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
df = df[df["average"] > 0]

# 🔐 Connessione a Supabase con DATABASE_URL
DATABASE_URL = os.environ["DATABASE_URL"]
print("🔍 Connecting to:", DATABASE_URL[:35] + "...")  # stampa parziale per debug

conn = psycopg2.connect(DATABASE_URL)

# 🧾 Query SQL di inserimento
sql = """
    INSERT INTO climate_co2_data (year, month, average_ppm, interpolated_ppm, trend_ppm, days_missing)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

# 🚀 Loop e insert
cur = conn.cursor()
for _, r in df.iterrows():
    cur.execute(sql, (
        int(r["year"]),
        int(r["month"]),
        float(r["average"]),
        float(r["interpolated"]),
        float(r["trend"]),
        int(r["days_missing"])
    ))

conn.commit()
cur.close()
conn.close()

print("✅ Supabase updated with CO2 data")
