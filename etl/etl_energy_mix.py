import os
import requests
import psycopg2

print("⚡ ETL Energy Mix (LIVE) avviato")

# Endpoint corretto: ottieni il mix energetico
url = "https://api.electricitymap.org/v3/power-mix/latest?zone=IT"
headers = {"auth-token": os.environ["ELECTRICITYMAP_API_KEY"]}

print("🔍 Chiamata API...")
response = requests.get(url, headers=headers)
print(f"📡 Status code: {response.status_code}")
print(f"🧾 Response text:\n{response.text}")

data = response.json()

# Verifica presenza chiavi
if "data" not in data or "energyMix" not in data["data"]:
    raise ValueError("❌ 'data' o 'energyMix' mancante nella risposta dell'API.")

mix = data["data"]["energyMix"]
updated_at = data["datetime"]

# Connessione al database
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

# Inserisce i dati del mix energetico
print("💾 Inserimento dati nel database...")
for source, percentage in mix.items():
    cur.execute("""
        INSERT INTO energy_mix (source, percentage, updated_at)
        VALUES (%s, %s, %s);
    """, (source, percentage, updated_at))

conn.commit()
cur.close()
conn.close()

print("✅ ETL Energy Mix completato con successo.")
