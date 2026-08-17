"""
One-off backfill for rows whose transmission/fuel_text/power_text/model_text/
model_type/range_* fields are NULL because they were scraped while
process_page()'s extraction was broken (see scraper.py history around
2026-04-22, and the transmission/range fields which never worked at all).

Re-crawls the live AutoScout24 listings (same price/km range crawl as
scrape_cars) and, for any car_id that matches a currently-NULL row, UPDATEs
just the affected columns instead of inserting a new row. Only listings still
live on the site can be recovered -- cars already sold/delisted are gone.
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import psycopg2.extras

import scraper

TABLE_NAME = "autoscout_car_adverts"
# Column name -> Postgres type, used to cast the untyped VALUES literals below.
# psycopg2's execute_values sends the VALUES(...) rows without type info; Postgres
# infers a column's type from the literals it sees, and a column that happens to be
# all-NULL in a given batch (e.g. range_general for a batch with no EVs) silently
# infers as `text`, which then fails to match a `numeric` target column.
BACKFILL_COLUMN_TYPES = {
    "transmission": "text",
    "fuel_text": "text",
    "power_text": "text",
    "power_kw": "numeric",
    "power_pk": "numeric",
    "model_text": "text",
    "model_type": "text",
    "range_raw": "text",
    "range_general": "numeric",
    "range_urban": "numeric",
}
BACKFILL_COLUMNS = list(BACKFILL_COLUMN_TYPES)
UPDATE_BATCH_SIZE = 500


def fetch_target_car_ids():
    """Return the set of car_ids that still have at least one target field NULL."""
    where_clause = " OR ".join(f"{col} IS NULL" for col in BACKFILL_COLUMNS)
    conn = scraper.get_postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT car_id FROM {TABLE_NAME} WHERE {where_clause}")
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def update_batch(conn, cars_to_update):
    """Batch-update BACKFILL_COLUMNS for the given car dicts, keyed by car_id."""
    if not cars_to_update:
        return
    columns = ["car_id"] + BACKFILL_COLUMNS
    values = [[car.get(col) for col in columns] for car in cars_to_update]
    set_clause = ", ".join(
        f"{col} = v.{col}::{BACKFILL_COLUMN_TYPES[col]}" for col in BACKFILL_COLUMNS
    )
    value_cols = ", ".join(columns)
    sql = (
        f"UPDATE {TABLE_NAME} AS t SET {set_clause} "
        f"FROM (VALUES %s) AS v({value_cols}) "
        f"WHERE t.car_id = v.car_id::uuid"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
    conn.commit()


def backfill():
    scraper.setup_logging()
    logging.info("Backfill script started.")

    backend = scraper.get_db_backend()
    if backend != "postgres":
        logging.error(f"Backfill script only supports the postgres backend, got: {backend}")
        return

    target_ids = fetch_target_car_ids()
    logging.info(f"Found {len(target_ids)} rows with missing fields to backfill.")
    if not target_ids:
        return

    ranges_file = scraper._ranges_file_path()
    price_list, km_list, _ = scraper.load_ranges_from_file(ranges_file)
    if not price_list or not km_list:
        logging.error(
            f"Could not load price/km ranges from {ranges_file}. This file is "
            "kept fine-grained by the daily scraper's auto-adjust logic to avoid "
            "hitting PAGE_LIMIT in busy buckets; refusing to fall back to a "
            "coarser hardcoded grid, since that could silently miss listings."
        )
        return
    price_ranges = np.array(price_list)
    km_ranges = np.array(km_list)

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
    }

    conn = scraper.get_postgres_conn()
    pending_updates = []
    updated_ids = set()
    ids_lock = threading.Lock()

    def flush_pending():
        if pending_updates:
            update_batch(conn, pending_updates)
            pending_updates.clear()

    try:
        for price_index in range(len(price_ranges) - 1):
            price_from = int(price_ranges[price_index])
            price_to = int(price_ranges[price_index + 1])
            logging.info(
                f"Backfill: evaluating price range {price_from}-{price_to} "
                f"({round((price_index + 1) / len(price_ranges) * 100, 2)}%)"
            )

            with ThreadPoolExecutor(max_workers=scraper.MAX_WORKERS) as executor:
                futures = [
                    executor.submit(
                        scraper.scrape_km_range,
                        base_url,
                        params,
                        price_from,
                        price_to,
                        km_ranges[i],
                        km_ranges[i + 1],
                        set(),  # treat every listing as "new" so it gets returned
                        set(),
                    )
                    for i in range(len(km_ranges) - 1)
                ]

                for future in as_completed(futures):
                    try:
                        km_cars, _, _, _, _, _, _ = future.result()
                    except Exception as e:
                        logging.error(f"Backfill thread error: {e}")
                        continue

                    with ids_lock:
                        for car in km_cars:
                            if car["car_id"] in target_ids:
                                pending_updates.append(car)
                                updated_ids.add(car["car_id"])

                        if len(pending_updates) >= UPDATE_BATCH_SIZE:
                            flush_pending()

        with ids_lock:
            flush_pending()
    finally:
        conn.close()

    missing_ids = target_ids - updated_ids
    logging.info(
        f"Backfill finished. Updated {len(updated_ids)}/{len(target_ids)} rows. "
        f"{len(missing_ids)} rows were not found in the current live listings "
        f"(likely sold/delisted) and remain unchanged."
    )
    print(
        f"Backfill finished. Updated {len(updated_ids)}/{len(target_ids)} rows. "
        f"{len(missing_ids)} rows could not be recovered (no longer live)."
    )


if __name__ == "__main__":
    backfill()
