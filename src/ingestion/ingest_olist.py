import os
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import kaggle
from dotenv import load_dotenv
from pathlib import Path

# ─── LOGGING SETUP ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/ingestion.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── LOAD ENVIRONMENT VARIABLES ──────────────────────────────────────────────
load_dotenv()

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE")

# ─── DATASET CONFIG ───────────────────────────────────────────────────────────
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
RAW_DATA_PATH  = Path("data/raw")

# All 9 Olist CSV files and their target Snowflake table names
OLIST_FILES = {
    "olist_orders_dataset.csv":                  "orders",
    "olist_order_items_dataset.csv":             "order_items",
    "olist_order_payments_dataset.csv":          "order_payments",
    "olist_order_reviews_dataset.csv":           "order_reviews",
    "olist_customers_dataset.csv":               "customers",
    "olist_products_dataset.csv":                "products",
    "olist_sellers_dataset.csv":                 "sellers",
    "olist_geolocation_dataset.csv":             "geolocation",
    "product_category_name_translation.csv":     "product_category_translation",
}

# ─── STEP 1: DOWNLOAD DATA FROM KAGGLE ───────────────────────────────────────
def download_olist_data():
    log.info("Starting Kaggle dataset download...")
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

    kaggle.api.authenticate()

    kaggle.api.dataset_download_files(
    KAGGLE_DATASET,
    path=str(RAW_DATA_PATH),
    unzip=True
    )
    log.info(f"Download complete. Files saved to {RAW_DATA_PATH}")


# ─── STEP 2: CONNECT TO SNOWFLAKE ────────────────────────────────────────────
def get_snowflake_connection():
    log.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )
    log.info("Snowflake connection established.")
    return conn


# ─── STEP 3: LOAD CSV TO SNOWFLAKE BRONZE ────────────────────────────────────
def load_file_to_snowflake(conn, csv_filename, table_name):
    file_path = RAW_DATA_PATH / csv_filename

    if not file_path.exists():
        log.warning(f"File not found: {file_path} — skipping.")
        return

    log.info(f"Loading {csv_filename} -> BRONZE.{table_name.upper()}...")

    # Read CSV into pandas dataframe
    df = pd.read_csv(file_path, dtype=str)  # dtype=str keeps everything as text in bronze

    # Uppercase all column names — Snowflake convention
    df.columns = [col.upper() for col in df.columns]

    # Write to Snowflake
    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name.upper(),
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=True,
        overwrite=True
    )

    if success:
        log.info(f"Loaded {num_rows} rows into BRONZE.{table_name.upper()}")
    else:
        log.error(f"Failed to load {csv_filename}")


# ─── MAIN PIPELINE ────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("OLIST BRONZE INGESTION PIPELINE STARTED")
    log.info("=" * 60)

    # Step 1 — Download data
    download_olist_data()

    # Step 2 — Connect to Snowflake
    conn = get_snowflake_connection()

    # Step 3 — Load each file
    for csv_file, table_name in OLIST_FILES.items():
        load_file_to_snowflake(conn, csv_file, table_name)

    conn.close()
    log.info("=" * 60)
    log.info("BRONZE INGESTION PIPELINE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()