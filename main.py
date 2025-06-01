from utils.extract import extract_all_products
from utils.transform import transform_data
from utils.load import save_to_csv, save_to_postgresql, save_to_google_sheets
from utils.config import CSV_FILE_PATH, POSTGRES_TABLE_NAME, GOOGLE_SHEET_NAME
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_etl_pipeline():
    """Runs the full ETL pipeline."""
    logging.info("Starting ETL Pipeline...")

    # 1. Extract
    logging.info("--- Extract Phase ---")
    raw_product_data = extract_all_products()
    if raw_product_data.empty:
        logging.error("Extraction failed or returned no data. ETL pipeline cannot continue.")
        return
    logging.info(f"Successfully extracted {len(raw_product_data)} raw product entries.")

    # 2. Transform
    logging.info("--- Transform Phase ---")
    cleaned_product_data = transform_data(raw_product_data)
    if cleaned_product_data.empty:
        logging.error("Transformation failed or resulted in no data. ETL pipeline cannot continue.")
        return
    logging.info(f"Successfully transformed data. {len(cleaned_product_data)} products ready for loading.")
    print(cleaned_product_data.head())
    print(cleaned_product_data.info())


    # 3. Load
    logging.info("--- Load Phase ---")
    # Load to CSV
    csv_success = save_to_csv(cleaned_product_data, CSV_FILE_PATH)
    if csv_success:
        logging.info(f"Data loaded to CSV: {CSV_FILE_PATH}")
    else:
        logging.warning("Failed to load data to CSV.")

    # Load to PostgreSQL
    pg_success = save_to_postgresql(cleaned_product_data, POSTGRES_TABLE_NAME)
    if pg_success:
        logging.info(f"Data loaded to PostgreSQL table: {POSTGRES_TABLE_NAME}")
    else:
        logging.warning("Failed to load data to PostgreSQL.")

    # Load to Google Sheets
    gs_success = save_to_google_sheets(cleaned_product_data) 
    if gs_success:
        logging.info(f"Data loaded to Google Sheets (details in config/.env).")
    else:
        logging.warning("Failed to load data to Google Sheets.")

    logging.info("ETL Pipeline finished.")

if __name__ == "__main__":
    run_etl_pipeline()