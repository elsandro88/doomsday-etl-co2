import pandas as pd
import requests
import psycopg2
import os
from io import StringIO

# URL NOAA
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
response = requests.get(url)
df = pd.read_csv(StringIO(response.text), comment='#', header=None,
                 names=["year", "month", "decimal_date", "average", "interpolated", "trend", "days_missing"])
df = df[df["average"] > 0]

# Supabase credentials from environment
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASS"],
    port=5432
)

sql = """INSERT INTO climate_co2_data (year, month, average_ppm, interpolated_ppm, trend_ppm, days_missing)
         VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

cur = conn.cursor()
for _, r in df.iterrows():
    cur.execute(sql, (r["year"], r["month"], r["average"], r["interpolated"], r["trend"], r["days_missing"]))
conn.commit()
cur.close()
conn.close()
print("✅ Supabase updated")
