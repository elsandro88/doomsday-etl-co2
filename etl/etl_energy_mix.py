print("⚡ ETL Energy Mix (LIVE) avviato")
print("🔍 Chiamata API...")

response = requests.get(
    "https://api.electricitymap.org/v3/carbon-intensity/latest?zone=IT",
    headers={"auth-token": os.environ["ELECTRICITYMAP_API_KEY"]}
)

print("🧾 Risposta API:")
print(response.status_code)
print(response.text)  # 👈 aggiungi questa riga!

data = response.json()
mix = data["data"]["energyMix"]
