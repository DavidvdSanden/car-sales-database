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
import cProfile
import pstats
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from time import monotonic

# -------------------------
# CONFIGURATION / TOGGLES
# -------------------------
USE_VPN = False
PROCESS_ALL = False
PAGE_LIMIT = 20
DB_REFRESH_RATE = 10
BATCH_SIZE = 500
POSTCODE_PATTERN = r'^\d{4}[A-Z]{2}$'

ENABLE_MULTITHREADING = True
MAX_WORKERS = 8

ENABLE_RATE_LIMITING = True
RATE_LIMIT_LOGGING = 1000
REQUESTS_PER_SECOND = 5
RANDOM_DELAY_RANGE = (0.01, 0.1)

# --- Adaptive Throttling ---
ADAPTIVE_THROTTLE_ENABLED = True
THROTTLE_CHECK_INTERVAL = 20        # requests before checking 429 ratio
THROTTLE_429_THRESHOLD = 0.1        # 10% of requests = slowdown trigger
THROTTLE_REDUCTION_FACTOR = 0.8     # reduce RPS by 20%
THROTTLE_MIN_RPS = 1.0              # min 1 request/sec
THROTTLE_DELAY_INCREASE = 1.5       # increase delay range by 50%
MAX_TOTAL_429 = 50                 # stop script after this many 429s total

# -------------------------
# NETWORK HELPERS
# -------------------------
_last_request_time = monotonic()
_rate_limit_lock = threading.Lock()

_request_count = 0
_start_window = monotonic()

_throttle_lock = threading.Lock()
_429_count = 0
_total_request_attempts = 0
_total_429_global = 0


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

    global _429_count, _total_request_attempts, _total_429_global

    try:
        response = session.get(url, params=params, timeout=timeout)
        _total_request_attempts += 1
        if response.status_code == 429:
            _429_count += 1
            _total_429_global += 1
            logging.warning(f"HTTP 429 Too Many Requests for URL: {url}")
            # Stop script if threshold reached
            if _total_429_global >= MAX_TOTAL_429:
                logging.critical(f"Exceeded MAX_TOTAL_429 ({MAX_TOTAL_429}). Stopping script immediately.")
                sys.exit("Too many 429 errors — stopping script.")
        response.raise_for_status()
        return response.text
    except requests.exceptions.ReadTimeout:
        logging.warning(f"Read timeout for URL: {url} | params: {params}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    return None


def adjust_rate_limit_if_needed():
    """Automatically reduce request rate and increase delay when too many 429s occur."""
    global _429_count, _total_request_attempts, REQUESTS_PER_SECOND, RANDOM_DELAY_RANGE

    with _throttle_lock:
        if not ADAPTIVE_THROTTLE_ENABLED or _total_request_attempts < THROTTLE_CHECK_INTERVAL:
            return

        ratio_429 = _429_count / _total_request_attempts
        if ratio_429 >= THROTTLE_429_THRESHOLD:
            old_rps = REQUESTS_PER_SECOND
            REQUESTS_PER_SECOND = max(THROTTLE_MIN_RPS, REQUESTS_PER_SECOND * THROTTLE_REDUCTION_FACTOR)
            old_delay = RANDOM_DELAY_RANGE
            RANDOM_DELAY_RANGE = (
                old_delay[0] * THROTTLE_DELAY_INCREASE,
                old_delay[1] * THROTTLE_DELAY_INCREASE
            )
            logging.warning(
                f"Too many 429s ({ratio_429:.1%}). "
                f"Reducing RPS from {old_rps:.2f} to {REQUESTS_PER_SECOND:.2f}, "
                f"increasing delay to {RANDOM_DELAY_RANGE}."
            )
        _429_count = 0
        _total_request_attempts = 0


def rate_limited_fetch_page(url, params, max_retries=3, timeout=30):
    """Wrapper for fetch_page that enforces a global rate limit and random delay."""
    global _last_request_time, _request_count, _start_window

    # --- Add jitter ---
    if RANDOM_DELAY_RANGE:
        time.sleep(random.uniform(*RANDOM_DELAY_RANGE))

    if ENABLE_RATE_LIMITING:
        with _rate_limit_lock:
            now = monotonic()
            min_interval = 1.0 / REQUESTS_PER_SECOND
            elapsed = now - _last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            _last_request_time = monotonic()

    # --- Fetch the page ---
    result = fetch_page(url, params, max_retries=max_retries, timeout=timeout)
    adjust_rate_limit_if_needed()

    # --- Log actual requests/sec every 10 requests ---
    with _rate_limit_lock:
        _request_count += 1
        if _request_count % RATE_LIMIT_LOGGING == 0:
            now = monotonic()
            elapsed_window = now - _start_window
            rps = _request_count / elapsed_window if elapsed_window > 0 else 0
            logging.info(f"Effective request rate: {rps:.2f} req/sec over last {elapsed_window:.1f}s")
            _request_count = 0
            _start_window = now

    return result


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
# PAGE PROCESSING FUNCTION
# -------------------------
def process_page(base_url, params, car_ids_in_database, car_ids_in_upsert):
    """Fetch and parse a single page, return list of car_info dicts."""
    html = rate_limited_fetch_page(base_url, params)
    if html is None:
        logging.warning(f"Skipping page due to repeated failures: {params}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    car_listings = soup.find_all("article", class_="cldt-summary-full-item")
    if not car_listings:
        return []

    results = []
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

            ranges = [float(num) for num in re.findall(r"\d+(?:\.\d+)?", actieradius_text)] if actieradius_text else []
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
            results.append(car_info)

    return results


# -------------------------
# MAIN SCRAPING LOGIC
# -------------------------
def scrape_cars(supabase, table_name):
    """Main scraping loop over price, km, and page ranges with thread-safe locks."""
    price_ranges: np.ndarray = np.array(
        [0, 500, 650, 700, 750, 850, 1000, 1100, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3250, 3500, 4000, 4500,
         5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 10500, 11000, 11500, 12000, 12500, 13000,
         13500, 14000, 14500, 15000, 15500, 16000, 16500, 17000, 17500, 18000, 18500, 19000, 19500, 20000, 20500, 21000,
         21500, 22000, 22500, 23000, 24000, 24500, 25000, 26000, 27000, 28000, 28500, 29000, 30000, 31000, 32000, 33000,
         34000, 35000, 36000, 36500, 37000, 38000, 39000, 40000, 41000, 42000, 43000, 43500, 44000, 44500, 45000, 46000,
         47000, 48000, 49000, 50000, 51000, 52000, 53000, 54000, 55000, 56000, 57000, 58000, 59000, 60000, 61000, 62000,
         64000, 66000, 68000, 70000, 75000, 80000, 85000, 90000, 95000, 100000, 125000, 150000, 1e9])
    km_ranges: np.ndarray = np.array(
        [0, 1, 2, 5, 7, 8, 10, 11, 12, 15, 20, 50, 100, 200, 500, 1000, 2000, 3000, 5000, 10000, 15000, 20000, 25000,
         30000, 35000, 40000, 45000, 50000, 55000, 60000, 70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000,
         145000, 150000, 155000, 160000, 170000, 180000, 190000, 200000, 210000, 220000, 230000, 240000, 260000, 280000,
         300000, 350000, 400000, 1e9])

    base_url = "https://www.autoscout24.nl/lst"
    params = {
        "atype": "C",
        "cy": "NL",
        "damaged_listing": "exclude",
        "desc": "1",
        "powertype": "kw",
        "sort": "age",
        "source": "homepage_search-mask",
        "ustate": "N,U"
    }

    car_ids_in_database = fetch_existing_car_ids(supabase, table_name)
    car_ids_in_upsert = set()
    cars_to_insert = []
    count_added = 0

    # --- Lock for thread-safe modifications ---
    ids_lock = threading.Lock()

    for price_index in range(len(price_ranges) - 1):
        params['pricefrom'] = round(price_ranges[price_index])
        params['priceto'] = round(price_ranges[price_index + 1])
        logging.info(f"Evaluating price range {params['pricefrom']}-{params['priceto']} "
                     f"({round(price_index / len(price_ranges) * 100, 2)}%)")

        if price_index % DB_REFRESH_RATE == 0:
            car_ids_in_database = fetch_existing_car_ids(supabase, table_name)

        for km_index in range(len(km_ranges) - 1):
            params['kmfrom'] = round(km_ranges[km_index])
            params['kmto'] = round(km_ranges[km_index + 1])

            if ENABLE_MULTITHREADING:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = []
                    for page_index in range(PAGE_LIMIT):
                        page_params = params.copy()
                        page_params['page'] = page_index + 1
                        futures.append(executor.submit(process_page, base_url, page_params,
                                                       car_ids_in_database, car_ids_in_upsert))

                    for future in as_completed(futures):
                        page_results = future.result()
                        with ids_lock:  # <-- protect shared data
                            for car_info in page_results:
                                cars_to_insert.append(car_info)
                                car_ids_in_database.add(car_info['car_id'])
                                car_ids_in_upsert.add(car_info['car_id'])

                                if len(cars_to_insert) >= BATCH_SIZE:
                                    insert_batch_to_db(supabase, table_name, cars_to_insert)
                                    count_added += len(cars_to_insert)
                                    cars_to_insert.clear()
                                    car_ids_in_upsert.clear()
            else:
                for page_index in range(PAGE_LIMIT):
                    page_params = params.copy()
                    page_params['page'] = page_index + 1
                    page_results = process_page(base_url, page_params,
                                                car_ids_in_database, car_ids_in_upsert)
                    with ids_lock:  # <-- protect shared data
                        for car_info in page_results:
                            cars_to_insert.append(car_info)
                            car_ids_in_database.add(car_info['car_id'])
                            car_ids_in_upsert.add(car_info['car_id'])

                            if len(cars_to_insert) >= BATCH_SIZE:
                                insert_batch_to_db(supabase, table_name, cars_to_insert)
                                count_added += len(cars_to_insert)
                                cars_to_insert.clear()
                                car_ids_in_upsert.clear()

    # Insert remaining cars
    with ids_lock:
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


if __name__ == '__main__':
    pr = cProfile.Profile()
    pr.enable()
    try:
        main()
    except Exception as e:
        logging.exception("Script crashed")
        raise
    finally:
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s)
        ps.strip_dirs()
        ps.sort_stats("cumtime")
        ps.print_stats(20)
        logging.info("Profiling results:\n%s", s.getvalue())
