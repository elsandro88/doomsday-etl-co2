import os
import requests
import psycopg2
from datetime import datetime

print("⚡ ETL Energy Mix (LIVE) avviato")

# Esegui chiamata all'API ElectricityMap
url = "https://api.electricitymap.org/v3/carbon-intensity/latest?zone=IT"
headers = {"auth-token": os.environ["ELECTRICITYMAP_API_KEY"]}

print("🔍 Chiamata API...")
response = requests.get(url, headers=headers)
print(f"📡 Status code: {response.status_code}")
print(f"🧾 Response text:\n{response.text}")

# Converte la risposta in JSON
data = response.json()

# Verifica se contiene la chiave "data"
if "data" not in data or "energyMix" not in data["data"]:
    raise ValueError("❌ 'data' o 'energyMix' mancante nella risposta dell'API.")

mix = data["data"]["energyMix"]
updated_at = data["datetime"]

# Connessione a Supabase (PostgreSQL)
print("🔌 Connessione al database...")
DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Crea tabella se non esiste
cur.execute("""
    CREATE TABLE IF NOT EXISTS energy_mix (
        id SERIAL PRIMARY KEY,
        source VARCHAR,
        percentage FLOAT,
        updated_at TIMESTAMP
    );
""")

# Inserisce i dati dell'energy mix
print("💾 Inserimento dati...")
for source, percentage in mix.items():
    cur.execute("""
        INSERT INTO energy_mix (source, percentage, updated_at)
        VALUES (%s, %s, %s);
    """, (source, percentage, updated_at))

conn.commit()
cur.close()
conn.close()
print("✅ ETL Energy Mix completato con successo.")
