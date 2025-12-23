# 🚗 Car Sales Database — AutoScout24 Scraper & Price Prediction

This project builds a complete **data pipeline**, **machine learning model**, and **Power BI dashboard** to analyze car advertisements from [AutoScout24.nl](https://www.autoscout24.nl).  

<img width="1448" height="812" alt="image" src="https://github.com/user-attachments/assets/ec721c20-0262-480a-bb55-09f218e5368a" />

It automatically scrapes listings, processes and stores them, and powers an interactive Power BI dashboard with insights such as:
- Price and mileage trends  
- Brand and model distribution  
- Predicted price trends for selected vehicles  

---

## 📁 Project Structure

```bash
car-sales-database/
│
├── app/ # (Future) deployment components (e.g. API or Streamlit app)
│
├── data_pipeline/ # Data scraping and preprocessing
│ ├── logs/ # Runtime logs
│ ├── notebooks/ # Development and EDA notebooks
│ └── scraper.py # Main AutoScout24 scraper
│
├── ml_model/ # Machine learning models and training scripts
│ └── notebooks/ # Model experimentation (linear regression, xG-style models, etc.)
│
├── pbi_dashboards/ # Power BI dashboards
│ └── main_dashboard.pbix # Linked to Supabase data and predictive model output
│
├── images/ # Plots, model output, or screenshots for documentation
│
├── .env # Supabase credentials and environment variables
├── .gitignore
├── requirements.txt
└── README.md
```

---

## ⚙️ Features

### **1. Data Scraping**
The pipeline iterates over **price** and **mileage** ranges to collect data from AutoScout24, extracting:
- Price  
- Mileage  
- Postal code  
- Fuel type  
- Transmission  
- Power (kW / HP)  
- Model & version  
- Range (if available)

> 🧩 *Optional:* Automatically connects to a NordVPN Netherlands server before scraping for stable access.

---

### **2. Database Integration**
- Connects to **Supabase** using credentials from `.env`
- Performs **upserts** (insert/update) to avoid duplicates  
- Removes redundant entries based on `car_id`

---

### **3. Machine Learning Model**
Inside `ml_model/`, a **Linear Regression pipeline** predicts car prices using both numerical and categorical features (`make`, `model`, `year`, etc.).

Features include:
- Automated preprocessing (OneHotEncoding & TargetEncoding)
- Model evaluation (MAE, RMSE, R²)
- Trained model persisted via `joblib`
- Ready for integration with Power BI or API endpoints

Example prediction usage:
```python
import joblib, pandas as pd

model = joblib.load("ml_model/car_price_model.pkl")

new_car = pd.DataFrame([{
    'make': 'Toyota',
    'model': 'Corolla',
    'year': 2018,
    'mileage': 30000,
    'engine_size': 1.8
}])

pred = model.predict(new_car)
print(f"Predicted price: €{pred[0]:,.0f}")
```

---

### **4. Power BI Dashboard**
Located in `pbi_dashboards/main_dashboard.pbix`.

**Dashboard pages:**
- **Overview:** Market summary and top-selling brands  
- **Location:** Distribution of listings across NL  
- **Car Info:** Details by make and model  
- **Price Trends:** Price evolution and predictions for selected vehicles  
- **Database:** Overall statistics on average prices, mileage, etc.  

---

## 🧰 Requirements

### **Software**
- Python 3.11+
- Supabase account (with appropriate tables)
- Optional: NordVPN CLI for network stability

### **Python Packages**
Install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 🔑 Environment Variables
Create a .env file in the project root:
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_service_key
```

If using VPN:
```bash
NORDVPN_USERNAME=your_username
NORDVPN_PASSWORD=your_password
```

## ▶️ Usage
#### 1. Run Scraper
``` python
python data_pipeline/scraper.py
```

This will:
- Connect to NordVPN (optional)
- Fetch and parse car ads
- Insert data into Supabase
- Log all activity in data_pipeline/logs/

#### 2. Train the Model

Train or retrain using your dataset:
```python
python ml_model/train_model.py
```

Model output:
- ml_model/car_price_model.pkl

#### 3. Visualize in Power BI

1. Open pbi_dashboards/main_dashboard.pbix
2. Refresh connections to Supabase → visualize latest data and predictions.

---

## 🧾 Logging

Logs are automatically saved in:
- data_pipeline/logs/

Format:
- scraper_log_2025-10-11_14-30-12.log

---

## 🚀 Future Improvements

- Expand ML models (e.g. gradient boosting, xG-like estimators for performance analysis)
- Automate Power BI refresh via Supabase webhook
- Add Streamlit or FastAPI interface for on-demand price predictions

---

## ⚠️ Disclaimer

_This project is for educational purposes only._

_Respect AutoScout24’s Terms of Service — avoid excessive requests to prevent IP blocking._


