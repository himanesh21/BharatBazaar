# BharatBazaar – Automated ETL & Commodity Price Intelligence (Python + PostgreSQL)

This project is a robust Python-based ETL (Extract, Transform, Load) pipeline designed to process Indian agricultural market price data. It ingests daily CSV reports, cleans the data, normalizes commodity names using Generative AI (Google Gemini), and loads the result into a **PostgreSQL Star Schema** for analytics.

## 🚀 Key Features

* **AI-Powered Normalization:** Uses Google's **Gemini 1.5 Pro** model to translate and standardize regional commodity names (e.g., "Bhindi" → "Okra") into common English names.
* **Smart Caching:** Implements a JSON-based caching mechanism (`commodity_mapping.json`) to minimize API calls and speed up processing for previously seen commodities.
* **Star Schema Architecture:** Transforms flat CSV data into a relational data warehouse structure with Fact and Dimension tables.
* **Data Quality & Cleaning:**
    * **Outlier Detection:** Automatically removes price anomalies using the Interquartile Range (IQR) method.
    * **Logical Validation:** Ensures strict adherence to `Min Price <= Modal Price <= Max Price`.
    * **Deduplication:** Handles duplicate records and ensures referential integrity.
* **Idempotent Ingestion:** Uses "Upsert" logic (Update/Insert) for dimension tables to prevent duplicate entities while capturing new data.

## 🛠️ Tech Stack

* **Language:** Python 3.8+
* **Database:** PostgreSQL
* **ETL & Data Manipulation:** Pandas, SQLAlchemy
* **AI Integration:** Google Generative AI (Gemini API)
* **Utilities:** `python-dotenv`, `tqdm` (Progress bars)

## 🗄️ Database Schema (Star Schema)

The pipeline organizes data into the following structure:

### Fact Table
| Table Name | Description | Key Columns |
| :--- | :--- | :--- |
| **`fact_market_prices`** | Stores daily price metrics. | `date_id`, `market_id`, `commodity_id`, `min_price`, `max_price`, `modal_price` |

### Dimension Tables
| Table Name | Description | Attributes |
| :--- | :--- | :--- |
| **`state_dim`** | Geographical state data. | `state_id`, `state` |
| **`market_dim`** | Specific market locations. | `market_id`, `market`, `district`, `state_id` |
| **`commodity_dim`** | Product details. | `commodity_id`, `commodity`, `variety`, `grade` |
| **`date_dim`** | Date attributes for time-series analysis. | `date_id`, `arrival_date`, `day`, `month`, `year`, `weekday` |

## ⚙️ Setup & Installation

### 1. Prerequisites
* Python installed on your machine.
* PostgreSQL installed and running locally.
* A Google Gemini API Key (Get it from Google AI Studio).

### 2. Install Dependencies
Run the following command to install the required libraries:

```bash
pip install pandas sqlalchemy psycopg2-binary python-dotenv tqdm google-generativeai requests
```

### 4. Environment Variables
Create a .env file in the root directory:

```bash

api-key=YOUR_DATA_SOURCE_KEY  # (Optional, if you expand the script to fetch data)
gemini=YOUR_GOOGLE_GEMINI_API_KEY
```


# 🧠 How the AI Logic Works
* The script extracts the base commodity name (e.g., "Tomato (Hybrid)").

* It checks commodity_mapping.json.

* If found: It uses the cached translation.

* If not found: It sends a prompt to Gemini 1.5 Pro:

* "Translate the Indian agricultural commodity name 'X' into its most common English name..."

* The result is saved to the JSON file and applied to the dataframe.


## ⚠️ Notes
* File Naming: The script expects input files to be named exactly all_food_prices_DD-MM-YYYY.csv.

* Outliers: The script removes data that falls outside 1.5 * IQR (Interquartile Range). If your data contains legitimate extreme price spikes, adjust the filtering logic in the ETL_and_SQL_ingestion function.
