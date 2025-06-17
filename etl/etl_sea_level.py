# etl/etl_sea_level.py
import requests
import pandas as pd
import psycopg2
import os
from io import StringIO

print("🌊 ETL Sea Level avviato")

# ✅ NOAA/NCEI fonte stabile
url = "https://www.ncei.noaa.gov/access/global-sea-level-rise/data/2022_rel/GMSL_TPJAOS_5.1_1993_2021.csv"
response = requests.get(url)
response.raise_for_status()

df = pd.read_csv(StringIO(response.text), skiprows=1, names=["decimal_year", "level_mm", "uncertainty_mm"])
df.dropna(inplace=True)

# Estrai anno e mese da 'decimal_year'
df["year"] = df["decimal_year"].astype(int)
df["month"] = ((df["decimal_year"] - df["year"]) * 12 + 1).round().astype(int)

df["level_mm"] = pd.to_numeric(df["level_mm"], errors="coerce")
df.dropna(subset=["level_mm"], inplace=True)

# 🔌 Connessione a Supabase
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
    """, (row["year"], row["month"], row["level_mm"]))

conn.commit()
cur.close()
conn.close()

print("✅ ETL completato – Dati livello mare aggiornati")
