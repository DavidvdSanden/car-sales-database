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


def is_valid_format(s, pattern):
    return bool(re.fullmatch(pattern, s))


def main():
    # Get current date and time
    now = datetime.now()

    # Format it as YYYY-MM-DD_HH-MM-SS
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    # The level is set to INFO, so all messages from INFO and above will be recorded.
    logging.basicConfig(filename=f"../logging/script_log_{timestamp}.log", level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Script started.')

    # Enable VPN
    command = r'cd "C:\Program Files\NordVPN" && nordvpn -c -g "Netherlands"'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    time.sleep(5)
    if result.returncode == 0:
        logging.info(f"Successful connection created with VPN.")
    else:
        logging.info(f"Unsuccessful connection with VPN. Return code is {result.returncode}")
        sys.exit()

    # Process all adverts
    is_processing_all = 1

    # Load variables from .env into the environment
    load_dotenv()

    # Read variables
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    supabase: Client = create_client(supabase_url, supabase_key)
    table_name = "autoscout_car_adverts"

    try:
        response = supabase.table(table_name).select("*").limit(1).execute()
        data = response.data
    except:
        logging.warning('No connection possible to Supabase.')

    price_vec = np.array(
        [0, 500, 650, 700, 750, 850, 1000, 1100, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3250, 3500, 4000, 4500,
         5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 10500, 11000, 11500, 12000, 12500, 13000,
         13500, 14000, 14500, 15000, 15500, 16000, 16500, 17000, 17500, 18000, 18500, 19000, 19500, 20000, 20500, 21000,
         21500, 22000, 22500, 23000, 24000, 24500, 25000, 26000, 27000, 28000, 28500, 29000, 30000, 31000, 32000, 33000,
         34000, 35000, 36000, 37000, 38000, 39000, 40000, 41000, 42000, 43000, 44000, 45000, 46000, 47000, 48000, 49000,
         50000, 52000, 54000, 56000, 58000, 60000, 62000, 64000, 66000, 68000, 70000, 75000, 80000, 85000, 90000, 95000,
         100000, 150000, 1e9])
    km_vec = np.array(
        [0, 1, 2, 5, 10, 15, 20, 50, 100, 200, 500, 1000, 2000, 3000, 5000, 10000, 15000, 20000, 25000, 30000, 35000,
         40000, 45000, 50000, 55000, 60000, 70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000, 145000, 150000,
         155000, 160000, 170000, 180000, 190000, 200000, 210000, 220000, 230000, 240000, 260000, 280000, 300000, 350000,
         400000, 1e9])

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
        "page": 1  # start page
    }

    count_added = 0
    cars_to_insert = []
    car_ids_in_upsert = set()
    batch_size = 500
    page_limit_autoscout = 20
    refresh_rate_cars_in_database = 10

    logging.info("Fetching existing car IDs from the database...")
    response = supabase.table(table_name).select("car_id").execute()
    car_ids_in_database = {d['car_id'] for d in response.data}
    logging.info(f"Found {len(car_ids_in_database)} existing car IDs.")
    pattern = r'^\d{4}[A-Z]{2}$'

    # Main loops over price range
    for k, price in enumerate(price_vec[:-1]):
        params['pricefrom'] = round(price_vec[k])
        params['priceto'] = round(price_vec[k + 1])
        logging.info(
            f"Evaluating price range {params['pricefrom']}-{params['priceto']}. "
            f"Approx {round(k / np.shape(price_vec)[0] * 100, 2)}%")

        # Refresh all cars in database
        if k % refresh_rate_cars_in_database == 0:
            response = supabase.table(table_name).select("car_id").execute()
            car_ids_in_database = {d['car_id'] for d in response.data}

        # Middle loop over mileage range
        for j, km in enumerate(km_vec[:-1]):
            params['kmfrom'] = round(km_vec[j])
            params['kmto'] = round(km_vec[j + 1])

            # Flag to check if the page loop completes fully
            page_limit_reached = True

            # Innermost loop over pages
            for i in range(page_limit_autoscout):
                params['page'] = i + 1
                html = requests.get(base_url, params=params).text
                soup = BeautifulSoup(html, "html.parser")
                car_listings = soup.find_all("article", class_="cldt-summary-full-item")

                # If no listings are found, the loop breaks early.
                if not car_listings:
                    page_limit_reached = False
                    break

                # Loop over all extracted cars
                for car in car_listings:

                    # Check if car is already in database
                    car_id = car.get("id")
                    if car_id not in car_ids_in_upsert and ((car_id not in car_ids_in_database) or is_processing_all):

                        # Extract correct mileage
                        try:
                            data_mileage = float(car.get("data-mileage"))
                        except (ValueError, TypeError):
                            data_mileage = -1

                        # Extract correct listing price
                        try:
                            listing_price = float(car.get("data-price"))
                        except (ValueError, TypeError):
                            listing_price = -1

                        # Extract postcode
                        raw_postcode = car.get("data-listing-zip-code")
                        try:
                            postcode = raw_postcode[0:4] + raw_postcode[-2:].upper()
                            if not is_valid_format(postcode, pattern):
                                postcode = None
                        except:
                            postcode = None

                        # Extract the desired details by their data-testid attributes
                        transmission = car.find("span", {"data-testid": "VehicleDetails-transmission"})
                        fuel = car.find("span", {"data-testid": "VehicleDetails-gas_pump"})
                        power = car.find("span", {"data-testid": "VehicleDetails-speedometer"})

                        # Get the text values, stripping whitespace
                        transmission_text = transmission.get_text(strip=True) if transmission else None
                        fuel_text = fuel.get_text(strip=True) if fuel else None
                        power_text = power.get_text(strip=True) if power else None

                        kw_value = None
                        pk_value = None

                        if power_text:
                            # Extract numbers: first one before 'kW', second inside parentheses
                            match = re.search(r"(\d+)\s*kW.*\((\d+)\s*PK\)", power_text)
                            if match:
                                kw_value = float(match.group(1))
                                pk_value = float(match.group(2))

                        # Find car and model specifics
                        title_element = car.find("span", class_="ListItem_title_bold__iQJRq")
                        model_text = title_element.get_text(strip=True) if title_element else None
                        version_element = car.find("span", class_="ListItem_version__5EWfi")
                        version_text = version_element.get_text(strip=True) if version_element else None

                        # Find the actieradius span by aria-label
                        actieradius_element = car.find("span", attrs={"aria-label": "actieradius"})
                        actieradius_text = actieradius_element.get_text(strip=True) if actieradius_element else None

                        # Extract both numeric values as floats
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

                        # Update database in batches
                        if len(cars_to_insert) >= batch_size:
                            logging.info(f"Inserting {len(cars_to_insert)} cars to the database...")
                            supabase.table(table_name).upsert(cars_to_insert, ignore_duplicates=True).execute()
                            count_added += len(cars_to_insert)
                            cars_to_insert = []
                            car_ids_in_upsert = set()

                time.sleep(0.01)

            # Check and log if the page limit was reached for this mileage-price combination
            if page_limit_reached:
                logging.info(
                    f"Reached page limit for price: {params['pricefrom']}-{params['priceto']} and mileage: "
                    f"{params['kmfrom']}-{params['kmto']}")

    # Insert any remaining cars after all loops have finished
    if cars_to_insert:
        logging.info(f"Inserting final {len(cars_to_insert)} cars to the database.")
        supabase.table(table_name).upsert(cars_to_insert).execute()
        count_added += len(cars_to_insert)

    logging.info(f"\nTotal cars added to the database: {count_added}")

    # Removal of duplicates
    response = supabase.table(table_name).select("id, car_id, make, listing_price").execute()
    car_ids_in_database = response.data
    df_full = pd.DataFrame(car_ids_in_database)
    id_to_remove = df_full.loc[(df_full.duplicated(subset=['car_id'], keep="first")), 'id'].values
    chunk_size = 1000
    if len(id_to_remove) == 0:
        logging.info('No duplicates found in database.')
    else:
        logging.info(f"Removing {len(id_to_remove)} duplicate entries in database.")

        for i in range(0, len(id_to_remove), chunk_size):
            chunk = id_to_remove[i:min(i + chunk_size, len(id_to_remove))]
            response = (
                supabase.table(table_name)
                .delete()
                .in_("id", chunk)
                .execute()
            )

    # Retrieve postcode information
    # logging.info('Calculating latitude and longitude for missing fields.')
    #  TO DO: add retrieve any missing postcode information

    # Script finished
    logging.info('Script finished successfully.')


if __name__ == '__main__':
    main()
