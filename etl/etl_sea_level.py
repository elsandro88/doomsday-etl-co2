# etl/etl_sea_level.py
import requests
import pandas as pd
import psycopg2
import os
from io import StringIO

print("🌊 ETL Sea Level avviato")

# 📦 Fonte NOAA/CSIRO stabile
url = "https://www.cmar.csiro.au/sealevel/sl_data_monthly.csv"
response = requests.get(url)
response.raise_for_status()

# 📄 Parsing
df = pd.read_csv(StringIO(response.text), skiprows=1, names=["date", "level_change_mm"])
df.dropna(inplace=True)

# Estrai anno e mese
df["year"] = df["date"].str.slice(0, 4).astype(int)
df["month"] = df["date"].str.slice(5, 7).astype(int)
df["level_change_mm"] = pd.to_numeric(df["level_change_mm"], errors="coerce")
df.dropna(subset=["level_change_mm"], inplace=True)

# 🔗 Connessione Supabase
DATABASE_URL = os.environ["DATABASE_URL"]
print("🔌 Connessione al database...")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# 🚀 Caricamento dati
print("📤 Inserimento dati livello mare...")
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO sea_level_data (year, month, level_change_mm)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (row["year"], row["month"], row["level_change_mm"]))

conn.commit()
cur.close()
conn.close()

print("✅ ETL completato – Dati livello mare caricati")
