import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from supabase import create_client, Client
import time
import logging
import os
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from datetime import datetime
import re


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
    batch_size = 500
    page_limit_autoscout = 20
    refresh_rate_cars_in_database = 10

    logging.info("Fetching existing car IDs from the database...")
    response = supabase.table(table_name).select("car_id").execute()
    car_ids_in_database = {d['car_id'] for d in response.data}
    logging.info(f"Found {len(car_ids_in_database)} existing car IDs.")
    pattern = r'^\d{4}[A-Z]{2}$'

    # Initialize a rich Console object
    console = Console()

    # --- Main Loops ---
    # Custom Progress display
    with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
    ) as progress:
        # Outer loop over price range
        task_price = progress.add_task(
            "[green]Processing price ranges...", total=len(price_vec[:-1] - 1)
        )

        for k, price in enumerate(price_vec[:-1]):
            params['pricefrom'] = round(price_vec[k])
            params['priceto'] = round(price_vec[k + 1])
            logging.info(f"Evaluating price range {params['pricefrom']}-{params['priceto']}.")

            if k % refresh_rate_cars_in_database == 0:
                response = supabase.table(table_name).select("car_id").execute()
                car_ids_in_database = {d['car_id'] for d in response.data}

            # Inner loop over mileage
            task_mileage = progress.add_task(
                f"[cyan]  Processing mileage {round(km_vec[0])}-{round(km_vec[-1])}...",
                total=len(km_vec[:-1] - 1)
            )

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

                    if not car_listings:
                        # If no listings are found, the loop breaks early.
                        page_limit_reached = False
                        break

                    for car in car_listings:

                        # Check if car is already in database
                        car_id = car.get("id")
                        if car_id not in car_ids_in_database:

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
                            }
                            cars_to_insert.append(car_info)
                            car_ids_in_database.add(car_id)

                            if len(cars_to_insert) >= batch_size:
                                console.log(f"Inserting {len(cars_to_insert)} cars to the database...")
                                logging.info(f"Inserting {len(cars_to_insert)} cars to the database...")
                                supabase.table(table_name).insert(cars_to_insert).execute()
                                count_added += len(cars_to_insert)
                                cars_to_insert = []

                    time.sleep(0.01)

                # Check and log if the page limit was reached for this mileage-price combination
                if page_limit_reached:
                    logging.info(
                        f"Reached page limit for price: {params['pricefrom']}-{params['priceto']} and mileage: "
                        f"{params['kmfrom']}-{params['kmto']}")

                # Update the mileage task for each mileage range
                progress.update(task_mileage, advance=1)

            # Mark the mileage task as complete and remove it
            progress.remove_task(task_mileage)

            # Update the price task for each price range
            progress.update(task_price, advance=1)

        # Mark the price task as complete and remove it
        progress.remove_task(task_price)

    # Insert any remaining cars after all loops have finished
    if cars_to_insert:
        logging.info(f"Inserting final {len(cars_to_insert)} cars to the database.")
        supabase.table(table_name).insert(cars_to_insert).execute()
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

    # Latitude / longitude information
    # logging.info('Calculating latitude and longitude for missing fields.')
    #  TO DO: add lat/lon calcs

    # Script finished
    logging.info('Script finished successfully.')


if __name__ == '__main__':
    main()
