import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from supabase import create_client
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
import faulthandler

# -------------------------
# CONFIGURATION / TOGGLES
# -------------------------
USE_VPN = False
PROCESS_ALL = False
PAGE_LIMIT = 20
DB_REFRESH_RATE = 10
BATCH_SIZE = 200
BATCH_SIZE_POSTCODES = 50
POSTCODE_PATTERN = r'^\d{4}[A-Z]{2}$'
BASE_URL_POST_CODE_API = "https://openpostcode.nl/api/address"
POST_CODE_BATCH_SIZE = 100
MAX_DUPLICATES_REMOVAL = 1000

ENABLE_MULTITHREADING = True
MAX_WORKERS = 16

ENABLE_RATE_LIMITING = True
RATE_LIMIT_LOGGING = 500
REQUESTS_PER_SECOND = 5
RANDOM_DELAY_RANGE = (0.01, 0.05)

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

# Global shared session with retry + backoff
_session = requests.Session()

# Define retry strategy
_retry_strategy = Retry(
    total=5,                # Retry up to 5 times
    backoff_factor=1,       # Wait 1s, then 2s, 4s, etc.
    status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
    allowed_methods=["GET", "POST"]              # Safe methods to retry
)

# Attach the adapter to both HTTP and HTTPS
_adapter = HTTPAdapter(max_retries=_retry_strategy, pool_connections=100, pool_maxsize=100)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)
faulthandler.enable()


# -------------------------
# UTILITY FUNCTIONS
# -------------------------
def fetch_page(url, params, timeout=30, session=_session):
    """Fetch a URL with retries and return response.text or None on failure."""

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
    result = fetch_page(url, params, timeout=timeout)
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


def setup_logging():
    """Configure logging with a timestamped file."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(current_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    log_path = os.path.join(log_dir, f"script_log_{timestamp}.log")
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


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


def fetch_existing_car_ids(table_name):
    """Fetch existing car IDs from Supabase."""
    supabase = get_supabase_client()
    logging.info("Fetching existing car IDs from database...")
    response = supabase.table(table_name).select("car_id").execute()
    car_ids = {d['car_id'] for d in response.data}
    logging.info(f"Found {len(car_ids)} existing car IDs.")
    return car_ids


def insert_batch_to_db(table_name, cars_to_insert):
    """Insert a batch of cars into Supabase."""
    supabase = get_supabase_client()
    if cars_to_insert:
        logging.info(f"Inserting {len(cars_to_insert)} cars into database...")
        supabase.table(table_name).upsert(cars_to_insert, ignore_duplicates=True).execute()


def fetch_all_rows_in_batches(
    table_name: str,
    key_column: str,
    columns: str = "*",
    batch_size: int = 5000,
    max_batches: int | None = None
):
    """
    Fetch all rows from a Supabase table in batches to avoid timeouts.

    Args:
        table_name: Name of the table to query
        key_column: String indicating the key indexing column on which table will be ordered
        columns: Comma-separated column names or "*" for all
        batch_size: Number of rows per batch
        max_batches: Optional limit (for testing or large tables)

    Returns:
        List of dicts containing all rows fetched.
    """
    supabase = get_supabase_client()
    all_rows = []
    offset = 0
    batch_count = 0
    last_key = None

    while True:
        try:

            query = supabase.table(table_name).select(columns).order(key_column, desc=False).limit(batch_size)
            if last_key is not None:
                query = query.gt(key_column, last_key)

            response = query.execute()
            data = response.data

            if not data:
                break

            all_rows.extend(data)
            last_key = data[-1][key_column]  # last key fetched

            offset += batch_size
            batch_count += 1
            logging.info(f"Fetched {len(data)} rows (total {len(all_rows)}).")

            # Optional: stop early if max_batches is set
            if max_batches and batch_count >= max_batches:
                logging.info(f"Reached max_batches ({max_batches}), stopping early.")
                break

        except Exception as e:
            logging.error(f"Error fetching batch starting at {offset}: {e}")
            time.sleep(2)
            break

    return all_rows


def remove_duplicates(table_name, chunk_size=1000, max_removals=MAX_DUPLICATES_REMOVAL):
    """Remove duplicate car_id entries from database."""
    response = fetch_all_rows_in_batches(table_name, "car_id", "id, car_id, make, listing_price", batch_size=20000)
    df_full = pd.DataFrame(response)
    car_id_to_remove = df_full.loc[df_full.duplicated(subset=['car_id'], keep="first"), 'car_id'].values
    logging.info(f"New method has: {len(car_id_to_remove)} duplicate entries in database.")

    if len(car_id_to_remove) == 0:
        logging.info('No duplicates found in database.')
        return

    supabase = get_supabase_client()
    logging.info(f"Removing {len(car_id_to_remove)} duplicate entries in database.")
    if len(car_id_to_remove) > max_removals:
        logging.warning(f"More duplicates detected than threshold. Limiting removal to {max_removals}")
    for i in range(0, min(max_removals, len(car_id_to_remove)), chunk_size):
        chunk = car_id_to_remove[i:min(i + chunk_size, len(car_id_to_remove))]
        supabase.table(table_name).delete().in_("car_id", chunk).execute()


def fetch_and_insert_postcodes():
    """Fetch missing postcode info from openpostcode.nl API and insert into database."""
    car_adverts_table = "autoscout_car_adverts"
    postcodes_table = "postcode_info_nl"
    supabase = get_supabase_client()
    global _total_429_global

    logging.info("Starting postcode enrichment job...")

    # --- Fetch existing postcodes ---
    response = fetch_all_rows_in_batches(car_adverts_table, "car_id", "car_id, post_code", batch_size=50000)
    df_full = pd.DataFrame(response).dropna(subset=['post_code'])
    postcodes_in_car_database = set(df_full['post_code'])
    response = fetch_all_rows_in_batches(postcodes_table, "post_code", "post_code, latitude", batch_size=50000)
    df_full = pd.DataFrame(response).dropna(subset=['latitude'])
    postcodes_in_database = set(df_full['post_code'])
    postcodes_not_in_database = postcodes_in_car_database.difference(postcodes_in_database)
    postcodes_to_insert = []
    count_added = 0
    total_to_process = len(postcodes_not_in_database)
    logging.info(f"Found {total_to_process} new postcodes to process.")

    for idx, code in enumerate(postcodes_not_in_database):
        if code in postcodes_in_database or not code:
            continue
        if _total_429_global >= MAX_TOTAL_429:
            logging.critical("Max 429 limit reached during postcode processing. Stopping.")
            break

        params = {"postcode": code, "huisnummer": 1}

        response = requests.get(BASE_URL_POST_CODE_API, params=params)
        if response.status_code == 500:
            logging.info(f"Response code 500 received for post code: {code}")
            continue
        elif response.status_code == 429:
            logging.info(f"Response code 429 received for post code: {code}")
            continue
        elif all(k in response.json() for k in ("latitude", "longitude")):
            lat = response.json()['latitude']
            lon = response.json()['longitude']
            straat = response.json()['straat']
            buurt = response.json()['buurt']
            wijk = response.json()['wijk']
            woonplaats = response.json()['woonplaats']
            gemeente = response.json()['gemeente']
            provincie = response.json()['provincie']
            huisnummer = response.json()['huisnummer']

        elif response.json()['error'] == 'Huisnummer not found':
            params = {
                "postcode": code,
                "huisnummer": response.json()['suggestions'][0]
            }
            response = requests.get(BASE_URL_POST_CODE_API, params=params)
            if response.status_code == 500:
                logging.info(f"Response code 500 received for post code: {code}")
                continue
            elif response.status_code == 429:
                logging.info(f"Response code 429 received for post code: {code}")
                continue
            else:
                lat = response.json()['latitude']
                lon = response.json()['longitude']
                straat = response.json()['straat']
                buurt = response.json()['buurt']
                wijk = response.json()['wijk']
                woonplaats = response.json()['woonplaats']
                gemeente = response.json()['gemeente']
                provincie = response.json()['provincie']
                huisnummer = response.json()['huisnummer']
        else:
            lat = None
            lon = None
            straat = None
            buurt = None
            wijk = None
            woonplaats = None
            gemeente = None
            provincie = None
            huisnummer = None

        postcode_info = {
            "post_code": code,
            "huisnummer": huisnummer,
            "straat": straat,
            "buurt": buurt,
            "wijk": wijk,
            "woonplaats": woonplaats,
            "gemeente": gemeente,
            "provincie": provincie,
            "longitude": lon,
            "latitude": lat,
        }
        postcodes_to_insert.append(postcode_info)
        postcodes_in_database.add(code)
        time.sleep(random.uniform(0.01, 0.05))
        if len(postcodes_to_insert) >= BATCH_SIZE_POSTCODES:
            logging.info(f"Inserting {len(postcodes_to_insert)} postcodes to the database...")
            supabase.table(postcodes_table).upsert(postcodes_to_insert).execute()
            count_added += len(postcodes_to_insert)
            postcodes_to_insert = []

    if postcodes_to_insert:
        logging.info(f"Inserting final {len(postcodes_to_insert)} postcodes to the database...")
        supabase.table(postcodes_table).upsert(postcodes_to_insert).execute()
        count_added += len(postcodes_to_insert)

    logging.info(f"Postcode enrichment completed. Total inserted: {count_added}")


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
        return -1

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

def scrape_km_range(base_url, params, price_from, price_to, km_from, km_to,
                    car_ids_in_database, car_ids_in_upsert):
    """Scrape all pages for a given (price, km) pair."""
    local_cars = []
    local_ids = set()

    for page_index in range(PAGE_LIMIT):
        page_params = params.copy()
        page_params.update({
            "pricefrom": round(price_from),
            "priceto": round(price_to),
            "kmfrom": round(km_from),
            "kmto": round(km_to),
            "page": page_index + 1
        })

        page_results = process_page(base_url, page_params, car_ids_in_database, car_ids_in_upsert)

        if page_results == -1:
            # Reached end of pages containing cars
            break
        else:
            local_cars.extend(page_results)
            local_ids.update([car["car_id"] for car in page_results])

        if page_index + 1 == PAGE_LIMIT:
            logging.info(f"Reached page limit for price {page_params['pricefrom']}-{page_params['priceto']} "
                         f"and mileage {page_params['kmfrom']}-{page_params['kmto']}")

    return local_cars, local_ids


def scrape_cars(table_name):
    """Main scraping loop over price, km, and page ranges with thread-safe locks."""
    price_ranges: np.ndarray = np.array(
        [0, 500, 650, 700, 750, 850, 1000, 1100, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3250, 3500, 4000, 4500,
         5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 10500, 11000, 11500, 12000, 12500, 13000,
         13500, 14000, 14500, 15000, 15500, 16000, 16500, 17000, 17500, 18000, 18500, 19000, 19500, 19750, 20000, 20500,
         21000, 21500, 22000, 22500, 23000, 23500, 24000, 24500, 25000, 26000, 27000, 28000, 28500, 29000, 30000, 31000, 32000,
         33000, 34000, 34500, 35000, 35500, 36000, 36500, 37000, 37500, 38000, 39000, 39500, 40000, 41000, 42000, 43000, 43500, 44000, 44500, 45000,
         46000, 47000, 48000, 49000, 50000, 51000, 52000, 53000, 54000, 55000, 56000, 57000, 58000, 59000, 60000, 61000,
         62000, 64000, 66000, 68000, 70000, 75000, 80000, 85000, 90000, 95000, 100000, 125000, 150000, 1e9])
        # [150000, 1e9])
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

    car_rows = fetch_all_rows_in_batches(table_name, "car_id", "car_id", batch_size=50000)
    car_ids_in_database = {row["car_id"] for row in car_rows if "car_id" in row}
    car_ids_in_upsert = set()
    cars_to_insert = []
    count_added = 0
    if ENABLE_MULTITHREADING:
        logging.info("Starting multithreaded scraping...")

    # --- Lock for thread-safe modifications ---
    ids_lock = threading.Lock()

    # --- Main nested loop (threading at KM level) ---
    for price_index in range(len(price_ranges) - 1):
        price_from = int(price_ranges[price_index])
        price_to = int(price_ranges[price_index + 1])
        logging.info(f"Evaluating price range {price_from}-{price_to} "
                     f"({round((price_index + 1) / len(price_ranges) * 100, 2)}%)")

        if price_index % DB_REFRESH_RATE == 0:
            car_rows = fetch_all_rows_in_batches(table_name, "car_id", "car_id", batch_size=50000)
            car_ids_in_database = {row["car_id"] for row in car_rows if "car_id" in row}

        if ENABLE_MULTITHREADING:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(
                        scrape_km_range, base_url, params, price_from, price_to,
                        km_ranges[i], km_ranges[i + 1],
                        car_ids_in_database, car_ids_in_upsert
                    )
                    for i in range(len(km_ranges) - 1)
                ]

                for future in as_completed(futures):
                    try:
                        km_cars, _ = future.result()
                        with ids_lock:
                            for car in km_cars:
                                car_id = car["car_id"]
                                if car_id not in car_ids_in_database:
                                    cars_to_insert.append(car)
                                    car_ids_in_database.add(car_id)
                                    car_ids_in_upsert.add(car_id)
                    except Exception as e:
                        logging.error(f"Thread error: {e}")

            with ids_lock:
                if len(cars_to_insert) >= BATCH_SIZE:
                    insert_batch_to_db(table_name, cars_to_insert)
                    count_added += len(cars_to_insert)
                    cars_to_insert.clear()
                    car_ids_in_upsert.clear()
                    # logging.info(f"Inserted {count_added} total cars")
        else:
            # Sequential fallback
            for i in range(len(km_ranges) - 1):
                km_cars, _ = scrape_km_range(
                    base_url, params, price_from, price_to,
                    km_ranges[i], km_ranges[i + 1],
                    car_ids_in_database, car_ids_in_upsert
                )
                for car in km_cars:
                    car_id = car["car_id"]
                    if car_id not in car_ids_in_database:
                        cars_to_insert.append(car)
                        car_ids_in_database.add(car_id)
                        car_ids_in_upsert.add(car_id)

            with ids_lock:
                if len(cars_to_insert) >= BATCH_SIZE:
                    insert_batch_to_db(table_name, cars_to_insert)
                    count_added += len(cars_to_insert)
                    cars_to_insert.clear()
                    car_ids_in_upsert.clear()

    # Final insert
    with ids_lock:
        if cars_to_insert:
            insert_batch_to_db(table_name, cars_to_insert)
            count_added += len(cars_to_insert)
            logging.info(f"Final batch inserted ({len(cars_to_insert)} cars)")

    logging.info(f"Total cars added: {count_added}")
    return count_added


# -------------------------
# ENTRY POINT
# -------------------------
def main():
    setup_logging()
    logging.info("Script started.")
    connect_vpn()

    table_name = "autoscout_car_adverts"

    if PROCESS_ALL:
        logging.warning("Re-processing all data.")
    try:
        scrape_cars(table_name)
        remove_duplicates(table_name)
        fetch_and_insert_postcodes()
    except Exception as e:
        logging.error(f"Error encountered: {e}")
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
