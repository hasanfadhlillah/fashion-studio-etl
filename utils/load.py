import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from .config import (
    CSV_FILE_PATH, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME,
    POSTGRES_TABLE_NAME, GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEET_NAME, GOOGLE_SHEET_ID
)
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_csv(df, file_path=CSV_FILE_PATH):
    """Saves DataFrame to a CSV file."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping CSV save.")
        return False
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info(f"Data successfully saved to CSV: {file_path}")
        return True
    except IOError as e:
        logging.error(f"Error saving data to CSV {file_path}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving to CSV {file_path}: {e}")
        return False

def save_to_postgresql(df, table_name=POSTGRES_TABLE_NAME):
    """Saves DataFrame to a PostgreSQL table."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping PostgreSQL save.")
        return False
    
    connection_string_from_env = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    logging.info(f"INFO: Using connection string from .env: postgresql://{DB_USER}:*****@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    connection_string_to_use = connection_string_from_env

    try:
        engine = create_engine(connection_string_to_use)
        with engine.connect() as connection:
            create_table_query = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Title TEXT,
                Price BIGINT,
                Rating FLOAT,
                Colors INTEGER,
                Size TEXT,
                Gender TEXT,
                Timestamp TIMESTAMP WITHOUT TIME ZONE,
                PRIMARY KEY (Title, Size, Gender, Colors) 
            );
            """)
            connection.execute(create_table_query)
            connection.commit()

            df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data successfully saved to PostgreSQL table: {table_name}")
        return True
    except FileNotFoundError:
        logging.error(f"PostgreSQL connection details might be missing or incorrect. Ensure .env is set up.")
        return False
    except ConnectionRefusedError as e:
        logging.error(f"PostgreSQL connection refused. Is the server running and accessible? {e}")
        return False
    except Exception as e: 
        logging.error(f"Error saving data to PostgreSQL table {table_name}: {e}")
        return False

def save_to_google_sheets(df):
    if df.empty:
        logging.warning("DataFrame is empty. Skipping Google Sheets save.")
        return False

    if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
        logging.error(f"Google Sheets credentials file not found: {GOOGLE_SHEETS_CREDENTIALS_FILE}")
        logging.error("Please ensure 'google-sheets-api.json' is in the project root directory.")
        return False
        
    try:
        logging.info(f"gspread module imported: {gspread} (Version: {gspread.__version__})")
        logging.info(f"Attempting to init client with: gspread.service_account(filename='{GOOGLE_SHEETS_CREDENTIALS_FILE}')")
        
        client = gspread.service_account(filename=GOOGLE_SHEETS_CREDENTIALS_FILE)
        logging.info(f"Type of client object: {type(client)}")
        spreadsheet = None
        service_account_email = client.auth.service_account_email # Dapatkan email SA di awal

        if GOOGLE_SHEET_ID:
            sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"
            logging.info(f"Attempting to open Google Sheet by URL (using ID): {sheet_url}")
            try:
                spreadsheet = client.open_by_url(sheet_url)
                logging.info(f"Successfully opened Google Sheet '{spreadsheet.title}' using URL (with ID).")
            except gspread.exceptions.APIError as e:
                logging.error(f"API error opening Google Sheet by URL (with ID '{GOOGLE_SHEET_ID}'): {e}. This is likely an authentication/permission issue (e.g., JWT, sheet not shared, APIs not enabled).")
                logging.error(f"Ensure the sheet is shared with '{service_account_email}' as Editor and Google Drive/Sheets APIs are enabled.")
                return False 
            except gspread.exceptions.SpreadsheetNotFound:
                logging.error(f"Google Sheet with ID '{GOOGLE_SHEET_ID}' not found via URL. Ensure the ID is correct and the sheet exists.")
                return False
            except Exception as e_url:
                logging.error(f"Unexpected error opening Google Sheet by URL (with ID '{GOOGLE_SHEET_ID}'): {e_url}")
                return False
        
        elif GOOGLE_SHEET_NAME: # Hanya jika GOOGLE_SHEET_ID tidak ada atau kosong
            logging.info(f"GOOGLE_SHEET_ID not found or not used. Attempting to open/create Google Sheet by name: {GOOGLE_SHEET_NAME}")
            try:
                spreadsheet = client.open(GOOGLE_SHEET_NAME)
                logging.info(f"Successfully opened Google Sheet by name: '{GOOGLE_SHEET_NAME}'")
            except gspread.exceptions.SpreadsheetNotFound:
                logging.info(f"Spreadsheet '{GOOGLE_SHEET_NAME}' not found by name. Attempting to create new one with this name.")
                try:
                    spreadsheet = client.create(GOOGLE_SHEET_NAME)
                    logging.info(f"Spreadsheet '{GOOGLE_SHEET_NAME}' created. URL: {spreadsheet.url}")
                    logging.info(f"Ensure it's shared with {service_account_email} as Editor.")
                except Exception as e_create:
                    logging.error(f"Error creating sheet by name '{GOOGLE_SHEET_NAME}': {e_create}")
                    return False
            except gspread.exceptions.APIError as e: # Tangkap APIError juga untuk open by name
                logging.error(f"API error opening Google Sheet by name '{GOOGLE_SHEET_NAME}': {e}. This is likely an authentication/permission issue (e.g., JWT, sheet not shared, APIs not enabled).")
                logging.error(f"Ensure the sheet (if it exists) is shared with '{service_account_email}' as Editor and Google Drive/Sheets APIs are enabled.")
                return False
            except Exception as e_open_name:
                logging.error(f"Error opening sheet by name '{GOOGLE_SHEET_NAME}': {e_open_name}")
                return False
        
        if not spreadsheet:
            logging.error(f"Could not open or create Google Sheet. Please verify GOOGLE_SHEET_ID/GOOGLE_SHEET_NAME in .env, ensure sharing permissions are correct with '{service_account_email}', and that Google Drive/Sheets APIs are enabled in your GCP project.")
            return False

        worksheet_title = "Products Data"
        try:
            worksheet = spreadsheet.worksheet(worksheet_title)
            logging.info(f"Using existing worksheet: '{worksheet_title}'")
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f"Worksheet '{worksheet_title}' not found. Creating new one.")
            worksheet = spreadsheet.add_worksheet(title=worksheet_title, rows="1", cols="1")
        
        worksheet.clear()
        
        df_gsp = df.copy()
        if 'Timestamp' in df_gsp.columns:
            df_gsp['Timestamp'] = df_gsp['Timestamp'].astype(str)

        worksheet.update([df_gsp.columns.values.tolist()] + df_gsp.values.tolist())
        logging.info(f"Data successfully saved to Google Sheet: '{spreadsheet.title}', Worksheet: '{worksheet_title}'")
        logging.info(f"Sheet URL: {spreadsheet.url}")
        return True
        
    except Exception as e:
        logging.error(f"An unexpected error occurred during Google Sheets operation: {e}")
        return False