# AutoScout24 Scraper

This project contains a data pipeline that collects car advertisements from [AutoScout24.nl](https://www.autoscout24.nl), processes the data, and stores it in a Supabase database. The data is linked to a PowerBI dashboard displaying information such as vehicle make, model, location, price, etc.

<img width="1199" height="674" alt="image" src="https://github.com/user-attachments/assets/8aa18a29-7e34-47c9-849f-f6d1aafb1714" />


## Features

* **Data Scraping:** Iterates through price and mileage ranges, fetching and parsing:
  * Price
  * Mileage
  * Postal code
  * Fuel type
  * Transmission
  * Power (kW & HP)
  * Model & version
  * Range (if available)
    
* **Database Integration:**
  * Connects to Supabase using `SUPABASE_URL` and `SUPABASE_KEY` from `.env`
  * Upserts (inserts or updates) new ads
  * Removes duplicates based on `car_id`
    
* **Logging:** All steps and warnings are logged in a timestamped log file.
* **VPN Connection (Optional):** Automatically connects to NordVPN (Netherlands server) before scraping starts.

## Requirements

### Software

* Python 3.9+
* Supabase account and tables named appropriately
* NordVPN CLI installed and properly configured (Optional)

### Python Packages

Install dependencies:

```bash
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_service_key
```

## Usage

Run the script:

```bash
python main.py
```

The script will:

1. Connect to NordVPN
2. Load Supabase credentials
3. Iterate over price and mileage ranges
4. Scrape ads and insert them into the database in batches
5. Remove duplicate entries

## Logging

Log files are stored in `../logging/` and are automatically named like:

```
script_log_2025-09-21_14-30-12.log
```

## Customization

You can modify the following parameters in the code:

* **`price_vec` and `km_vec`**: price and mileage ranges
* **`page_limit_autoscout`**: number of pages per query
* **`batch_size`**: batch size for database upserts
* **`refresh_rate_cars_in_database`**: how often existing car IDs are refreshed

## Dashboard

* **General overview**: Overall statistics on cars in database, top-selling brands, etc.
* **Location**: Geospatial overview of where cars are sold within NL.
* **Car Info**: Detailed information on specified model and make.
* **Price trends**: Price trends for specified model and make over selected time period.
* **Price**: Price comparisons between specified model and make.
* **Database**: Overall database statistics on average prices, mileage, etc.

## Future Improvements

* Add more robust error handling for network issues
* Implement parallel scraping to improve speed

---

_**Note:** Respect AutoScout24's terms of service and avoid excessive requests to prevent being blocked._
