import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from google.oauth2.service_account import Credentials
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
        
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            # Create table if it doesn't exist (simple version, consider migrations for complex apps)
            create_table_query = text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Title TEXT,
                Price BIGINT,
                Rating FLOAT,
                Colors INTEGER,
                Size TEXT,
                Gender TEXT,
                Timestamp TIMESTAMP WITHOUT TIME ZONE,
                PRIMARY KEY (Title, Size, Gender, Colors) -- Example composite key
            );
            """)
            connection.execute(create_table_query)
            connection.commit() # Commit table creation

            # Save data, replacing if exists based on primary key (or use 'append')
            df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data successfully saved to PostgreSQL table: {table_name}")
        return True
    except FileNotFoundError: # if .env is missing and defaults are used
        logging.error(f"PostgreSQL connection details might be missing or incorrect. Ensure .env is set up.")
        return False
    except ConnectionRefusedError as e:
        logging.error(f"PostgreSQL connection refused. Is the server running and accessible? {e}")
        return False
    except Exception as e: # Catches sqlalchemy.exc.OperationalError, etc.
        logging.error(f"Error saving data to PostgreSQL table {table_name}: {e}")
        return False


def save_to_google_sheets(df): # Hapus parameter sheet_name, akan menggunakan dari config
    if df.empty:
        logging.warning("DataFrame is empty. Skipping Google Sheets save.")
        return False

    if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
        logging.error(f"Google Sheets credentials file not found: {GOOGLE_SHEETS_CREDENTIALS_FILE}")
        logging.error("Please ensure 'google-sheets-api.json' is in the project root directory.")
        return False
        
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = None

        # 1. Coba buka dengan ID jika ada
        if GOOGLE_SHEET_ID:
            try:
                logging.info(f"Attempting to open Google Sheet by ID: {GOOGLE_SHEET_ID}")
                spreadsheet = client.open_by_id(GOOGLE_SHEET_ID)
                logging.info(f"Successfully opened Google Sheet '{spreadsheet.title}' using ID.")
            except gspread.exceptions.SpreadsheetNotFound:
                logging.warning(f"Google Sheet with ID '{GOOGLE_SHEET_ID}' not found.")
                # Jika ID diberikan tapi tidak ditemukan, kita bisa memilih untuk gagal atau fallback ke nama.
                # Untuk skenario ini, jika ID ada di .env, kita ingin sheet spesifik itu.
                logging.error(f"If GOOGLE_SHEET_ID is specified in .env, the sheet must exist and be shared with {creds.service_account_email}.")
                return False
            except gspread.exceptions.APIError as e:
                logging.error(f"API error opening Google Sheet by ID '{GOOGLE_SHEET_ID}': {e}. Check permissions or ID validity.")
                return False # Gagal jika ada error API saat buka dengan ID

        # 2. Jika tidak ada ID, atau gagal (dan tidak return False di atas), coba dengan NAMA
        if not spreadsheet and GOOGLE_SHEET_NAME:
            try:
                logging.info(f"Attempting to open/create Google Sheet by name: {GOOGLE_SHEET_NAME}")
                spreadsheet = client.open(GOOGLE_SHEET_NAME)
                logging.info(f"Successfully opened Google Sheet by name: '{GOOGLE_SHEET_NAME}'")
            except gspread.exceptions.SpreadsheetNotFound:
                logging.info(f"Spreadsheet '{GOOGLE_SHEET_NAME}' not found by name. Creating new one with this name.")
                spreadsheet = client.create(GOOGLE_SHEET_NAME)
                # Otomatis service account menjadi owner, tapi ingatkan untuk share jika perlu
                logging.info(f"Spreadsheet '{GOOGLE_SHEET_NAME}' created. URL: {spreadsheet.url}")
                logging.info(f"Ensure it's shared appropriately if others need access besides {creds.service_account_email}.")
        
        if not spreadsheet:
            logging.error("Could not open or create Google Sheet. Please check GOOGLE_SHEET_ID/GOOGLE_SHEET_NAME in .env and ensure sharing permissions are correct with '{creds.service_account_email}'.")
            return False

        worksheet_title = "Products Data" # Anda bisa buat ini konfigurabel juga jika mau
        try:
            worksheet = spreadsheet.worksheet(worksheet_title)
        except gspread.exceptions.WorksheetNotFound:
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
        logging.error(f"An unexpected error occurred while saving to Google Sheets: {e}")
        return False