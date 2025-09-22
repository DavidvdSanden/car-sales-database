import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from supabase import create_client, Client
import time
import logging
import os
from dotenv import load_dotenv
from datetime import datetime
import re
import subprocess
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

# -------------------------
# CONSTANTS
# -------------------------
USE_VPN = False
PROCESS_ALL = True
PAGE_LIMIT = 20
REFRESH_RATE = 10
BATCH_SIZE = 500
POSTCODE_PATTERN = r'^\d{4}[A-Z]{2}$'

# -------------------------
# NETWORK HELPERS
# -------------------------


def fetch_page(url, params, max_retries=3, timeout=30):
    """Fetch a URL with retries and return response.text or None on failure."""
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.ReadTimeout:
        logging.warning(f"Read timeout for URL: {url} | params: {params}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    return None


# -------------------------
# UTILITY FUNCTIONS
# -------------------------

def setup_logging():
    """Configure logging with a timestamped file."""
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    logging.basicConfig(
        filename=f"../logging/script_log_{timestamp}.log",
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def connect_vpn():
    """Optionally connect to VPN (if USE_VPN is enabled)."""
    if USE_VPN:
        command = r'cd "C:\Program Files\NordVPN" && nordvpn -c -g "Netherlands"'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        time.sleep(10)
        if result.returncode == 0:
            logging.info("Successful VPN connection.")
        else:
            logging.error(f"VPN connection failed. Return code: {result.returncode}")
            sys.exit()
    else:
        logging.info("No VPN activated intentionally.")


def is_valid_format(s, pattern):
    """Check if string matches given regex pattern."""
    return bool(re.fullmatch(pattern, s))


def get_supabase_client():
    """Create and return Supabase client from .env."""
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    return create_client(supabase_url, supabase_key)


def fetch_existing_car_ids(supabase, table_name):
    """Fetch existing car IDs from Supabase."""
    logging.info("Fetching existing car IDs from database...")
    response = supabase.table(table_name).select("car_id").execute()
    car_ids = {d['car_id'] for d in response.data}
    logging.info(f"Found {len(car_ids)} existing car IDs.")
    return car_ids


def insert_batch_to_db(supabase, table_name, cars_to_insert):
    """Insert a batch of cars into Supabase."""
    if cars_to_insert:
        logging.info(f"Inserting {len(cars_to_insert)} cars into database...")
        supabase.table(table_name).upsert(cars_to_insert, ignore_duplicates=True).execute()


def remove_duplicates(supabase, table_name, chunk_size=1000):
    """Remove duplicate car_id entries from database."""
    response = supabase.table(table_name).select("id, car_id, make, listing_price").execute()
    df_full = pd.DataFrame(response.data)
    id_to_remove = df_full.loc[df_full.duplicated(subset=['car_id'], keep="first"), 'id'].values

    if len(id_to_remove) == 0:
        logging.info('No duplicates found in database.')
        return

    logging.info(f"Removing {len(id_to_remove)} duplicate entries in database.")
    for i in range(0, len(id_to_remove), chunk_size):
        chunk = id_to_remove[i:min(i + chunk_size, len(id_to_remove))]
        supabase.table(table_name).delete().in_("id", chunk).execute()


# -------------------------
# MAIN SCRAPING LOGIC
# -------------------------

def scrape_cars(supabase, table_name):
    """Main scraping loop over price, km, and page ranges."""
    price_ranges: np.ndarray = np.array(
        [0, 500, 650, 700, 750, 850, 1000, 1100, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3250, 3500, 4000, 4500,
         5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 10500, 11000, 11500, 12000, 12500, 13000,
         13500, 14000, 14500, 15000, 15500, 16000, 16500, 17000, 17500, 18000, 18500, 19000, 19500, 20000, 20500, 21000,
         21500, 22000, 22500, 23000, 24000, 24500, 25000, 26000, 27000, 28000, 28500, 29000, 30000, 31000, 32000, 33000,
         34000, 35000, 36000, 37000, 38000, 39000, 40000, 41000, 42000, 43000, 44000, 45000, 46000, 47000, 48000, 49000,
         50000, 52000, 54000, 56000, 58000, 60000, 62000, 64000, 66000, 68000, 70000, 75000, 80000, 85000, 90000, 95000,
         100000, 150000, 1e9])
    km_ranges: np.ndarray = np.array(
        [0, 1, 2, 5, 7, 10, 12, 15, 20, 50, 100, 200, 500, 1000, 2000, 3000, 5000, 10000, 15000, 20000, 25000, 30000,
         35000, 40000, 45000, 50000, 55000, 60000, 70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000, 145000,
         150000, 155000, 160000, 170000, 180000, 190000, 200000, 210000, 220000, 230000, 240000, 260000, 280000, 300000,
         350000, 400000, 1e9])

    base_url = "https://www.autoscout24.nl/lst"
    params = {
        "atype": "C",
        "cy": "NL",
        "damaged_listing": "exclude",
        "desc": "1",
        "powertype": "kw",
        "sort": "age",
        "source": "homepage_search-mask",
        "ustate": "N,U",
        "kmfrom": 0,
        "kmto": 1000,
        "pricefrom": 0,
        "priceto": 10000,
        "page": 1
    }

    car_ids_in_database = fetch_existing_car_ids(supabase, table_name)
    car_ids_in_upsert = set()
    cars_to_insert = []
    count_added = 0

    for price_index, price in enumerate(price_ranges[:-1]):
        params['pricefrom'] = round(price_ranges[price_index])
        params['priceto'] = round(price_ranges[price_index + 1])
        logging.info(f"Evaluating price range {params['pricefrom']}-{params['priceto']} "
                     f"({round(price_index / len(price_ranges) * 100, 2)}%)")

        if price_index % REFRESH_RATE == 0:
            car_ids_in_database = fetch_existing_car_ids(supabase, table_name)

        for km_index, km in enumerate(km_ranges[:-1]):
            params['kmfrom'] = round(km_ranges[km_index])
            params['kmto'] = round(km_ranges[km_index + 1])
            page_limit_reached = True

            for page_index in range(PAGE_LIMIT):
                params['page'] = page_index + 1
                html = fetch_page(base_url, params)
                if html is None:
                    logging.warning(f"Skipping page due to repeated failures: {params}")
                    continue

                soup = BeautifulSoup(html, "html.parser")
                car_listings = soup.find_all("article", class_="cldt-summary-full-item")
                if not car_listings:
                    page_limit_reached = False
                    break

                for car in car_listings:
                    car_id = car.get("id")
                    if car_id not in car_ids_in_upsert and ((car_id not in car_ids_in_database) or PROCESS_ALL):
                        try:
                            data_mileage = float(car.get("data-mileage"))
                        except (ValueError, TypeError):
                            data_mileage = -1

                        try:
                            listing_price = float(car.get("data-price"))
                        except (ValueError, TypeError):
                            listing_price = -1

                        raw_postcode = car.get("data-listing-zip-code")
                        try:
                            postcode = raw_postcode[0:4] + raw_postcode[-2:].upper()
                            if not is_valid_format(postcode, POSTCODE_PATTERN):
                                postcode = None
                        except:
                            postcode = None

                        transmission = car.find("span", {"data-testid": "VehicleDetails-transmission"})
                        fuel = car.find("span", {"data-testid": "VehicleDetails-gas_pump"})
                        power = car.find("span", {"data-testid": "VehicleDetails-speedometer"})

                        transmission_text = transmission.get_text(strip=True) if transmission else None
                        fuel_text = fuel.get_text(strip=True) if fuel else None
                        power_text = power.get_text(strip=True) if power else None

                        kw_value, pk_value = None, None
                        if power_text:
                            match = re.search(r"(\d+)\s*kW.*\((\d+)\s*PK\)", power_text)
                            if match:
                                kw_value = float(match.group(1))
                                pk_value = float(match.group(2))

                        title_element = car.find("span", class_="ListItem_title_bold__iQJRq")
                        model_text = title_element.get_text(strip=True) if title_element else None
                        version_element = car.find("span", class_="ListItem_version__5EWfi")
                        version_text = version_element.get_text(strip=True) if version_element else None

                        actieradius_element = car.find("span", attrs={"aria-label": "actieradius"})
                        actieradius_text = actieradius_element.get_text(strip=True) if actieradius_element else None

                        ranges = [float(num) for num in
                                  re.findall(r"\d+(?:\.\d+)?", actieradius_text)] if actieradius_text else []

                        general_range = ranges[0] if len(ranges) > 0 else None
                        urban_range = ranges[1] if len(ranges) > 1 else None

                        car_info = {
                            "car_id": car_id,
                            "make": car.get("data-make"),
                            "model": car.get("data-model"),
                            "first_registration": car.get("data-first-registration"),
                            "fuel_type": car.get("data-fuel-type"),
                            "mileage": data_mileage,
                            "post_code_raw": raw_postcode,
                            "post_code": postcode,
                            "listing_price": listing_price,
                            "transmission": transmission_text,
                            "fuel_text": fuel_text,
                            "power_text": power_text,
                            "power_kw": kw_value,
                            "power_pk": pk_value,
                            "model_text": model_text,
                            "model_type": version_text,
                            "range_raw": actieradius_text,
                            "range_general": general_range,
                            "range_urban": urban_range
                        }
                        cars_to_insert.append(car_info)
                        car_ids_in_database.add(car_id)
                        car_ids_in_upsert.add(car_id)

                        if len(cars_to_insert) >= BATCH_SIZE:
                            insert_batch_to_db(supabase, table_name, cars_to_insert)
                            count_added += len(cars_to_insert)
                            cars_to_insert.clear()
                            car_ids_in_upsert.clear()

                time.sleep(random.uniform(0.01, 0.05))

            if page_limit_reached:
                logging.info(f"Reached page limit for price {params['pricefrom']}-{params['priceto']} "
                             f"and mileage {params['kmfrom']}-{params['kmto']}")

    if cars_to_insert:
        insert_batch_to_db(supabase, table_name, cars_to_insert)
        count_added += len(cars_to_insert)

    logging.info(f"Total cars added: {count_added}")


# -------------------------
# ENTRY POINT
# -------------------------

def main():
    setup_logging()
    logging.info("Script started.")
    connect_vpn()

    table_name = "autoscout_car_adverts"
    supabase = get_supabase_client()

    if PROCESS_ALL:
        logging.warning("Re-processing all data.")

    scrape_cars(supabase, table_name)
    remove_duplicates(supabase, table_name)
    logging.info("Script finished successfully.")


if __name__ == "__main__":
    main()
