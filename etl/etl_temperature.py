# etl/etl_temperature.py
import requests
import pandas as pd
import psycopg2
import os
from io import StringIO

print("🌡️ ETL Temperature avviato")

# Dataset NASA GISTEMP mensile (anomalie in °C rispetto a 1951–1980)
url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"
response = requests.get(url)
response.raise_for_status()

# Carica CSV saltando la prima riga descrittiva
df = pd.read_csv(StringIO(response.text), skiprows=1)

# Reshape da wide a long (un record per anno/mese)
df = df.melt(id_vars=["Year"], var_name="month", value_name="anomaly_celsius")
df.dropna(inplace=True)

# Rimuovi valori non numerici (es. '***')
df["anomaly_celsius"] = pd.to_numeric(df["anomaly_celsius"], errors="coerce")
df.dropna(subset=["anomaly_celsius"], inplace=True)

# Mappa mesi in numeri
month_map = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5,
             "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
df["month"] = df["month"].str.strip().str[:3].map(month_map)
df = df.dropna(subset=["month"])  # rimuove eventuali mesi non mappati

# Connessione a Supabase (PostgreSQL)
DATABASE_URL = os.environ["DATABASE_URL"]
print("🔗 Connessione al DB...")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Inserimento dati
print("📤 Inizio inserimento dati...")
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO temperature_data (year, month, anomaly_celsius)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (int(row["Year"]), int(row["month"]), float(row["anomaly_celsius"])))

conn.commit()
cur.close()
conn.close()

print("✅ ETL completato – Dati temperatura caricati")
