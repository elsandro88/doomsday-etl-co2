# etl/etl_sea_level.py
import requests
import pandas as pd
import psycopg2
import os
from io import StringIO

print("🌊 ETL Sea Level avviato")

# Dataset NASA Sea Level in CSV
url = "https://climate.nasa.gov/system/internal_resources/details/original/1986_Global_Sea_Level_Data_CSV.csv"
response = requests.get(url)
response.raise_for_status()

df = pd.read_csv(StringIO(response.text))
df.columns = df.columns.str.strip()

# Estrazione anno/mese da decimal year
df["year"] = df["Time"].astype(int)
df["month"] = ((df["Time"] % 1) * 12 + 1).astype(int)
df["level_change_mm"] = df["GMSL"]  # Global Mean Sea Level

# Connessione a Supabase/PostgreSQL
DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Inserimento dati
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO sea_level_data (year, month, level_change_mm)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (int(row["year"]), int(row["month"]), float(row["level_change_mm"])))

conn.commit()
cur.close()
conn.close()
print("✅ Dati livello mare caricati")
