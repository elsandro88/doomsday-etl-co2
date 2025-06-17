# etl/etl_temperature.py
import requests
import pandas as pd
import psycopg2
import os
from io import StringIO

print("🌡️ ETL Temperature avviato")

# URL ufficiale dei dati mensili GISTEMP (NASA)
url = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"
response = requests.get(url)
response.raise_for_status()

# Parsing CSV, saltando header non tabellare
df = pd.read_csv(StringIO(response.text), skiprows=1)

# Pulizia: melting del DataFrame per ottenere righe singole per mese
df = df.melt(id_vars=["Year"], var_name="month", value_name="anomaly_celsius")
df.dropna(inplace=True)
df["anomaly_celsius"] = pd.to_numeric(df["anomaly_celsius"], errors="coerce")
df.dropna(subset=["anomaly_celsius"], inplace=True)
df["month"] = df["month"].str.strip().str[:3]

# Mapping da stringa a numero
month_map = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5,
             "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
df["month"] = df["month"].map(month_map)

# Connessione Supabase/Postgres
DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Inserimento dati
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO temperature_data (year, month, anomaly_celsius)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (int(row["Year"]), int(row["month"]), float(row["anomaly_celsius"])))

conn.commit()
cur.close()
conn.close()
print("✅ Dati temperatura caricati")
