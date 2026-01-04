# export_supabase_table.py
import csv
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLE = "autoscout_car_adverts"
OUT = f"{TABLE}.csv"
BATCH = 10000

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json",
}

last_id = None
total = 0
fieldnames = None

with open(OUT, "w", newline="", encoding="utf-8") as f:
    writer = None

    while True:
        params = {
            "select": "*",
            "order": "car_id.asc",
            "limit": str(BATCH),
        }
        if last_id:
            params["car_id"] = f"gt.{last_id}"

        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{TABLE}",
            headers=headers,
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break

        if writer is None:
            fieldnames = list(rows[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

        writer.writerows(rows)
        total += len(rows)
        last_id = rows[-1]["car_id"]
        print(f"exported {total} rows; last_id={last_id}")
        time.sleep(0.2)

print(f"done: {total} rows -> {OUT}")
