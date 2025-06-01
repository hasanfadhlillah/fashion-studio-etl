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
        
    # ---- AWAL BLOK HARDCODE UNTUK DEBUGGING ----
    temp_db_user = "fashionetladmin"
    temp_db_password = "cobaLAGI789" 
    temp_db_host = "localhost"
    temp_db_port = "5432"
    temp_db_name = "fashion_products"
    # Gunakan connection_string dari nilai hardcode ini:
    connection_string_for_debug = f"postgresql://{temp_db_user}:{temp_db_password}@{temp_db_host}:{temp_db_port}/{temp_db_name}"
    logging.info(f"DEBUG: Using hardcoded connection string: postgresql://{temp_db_user}:*****@{temp_db_host}:{temp_db_port}/{temp_db_name}")
    # ---- AKHIR BLOK HARDCODE UNTUK DEBUGGING ----

    # Gunakan connection_string_for_debug jika ingin tes hardcode
    # ATAU gunakan connection_string dari .env jika tes hardcode selesai
    # connection_string_to_use = connection_string_for_debug # Untuk tes hardcode
    
    # Baris asli yang menggunakan .env (pastikan ini di-uncomment setelah tes hardcode)
    connection_string_from_env = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    logging.info(f"INFO: Using connection string from .env: postgresql://{DB_USER}:*****@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    connection_string_to_use = connection_string_from_env


    # Log nilai yang benar-benar akan digunakan:
    # logging.info(f"Attempting PostgreSQL connection with (from .env):")
    # logging.info(f"   DB_USER: '{DB_USER}'")
    # logging.info(f"   DB_PASSWORD: '{DB_PASSWORD[:2]}...{DB_PASSWORD[-2:]if len(DB_PASSWORD)>4 else ''}' (Length: {len(DB_PASSWORD)})")
    # logging.info(f"   DB_HOST: '{DB_HOST}'")
    # logging.info(f"   DB_PORT: '{DB_PORT}'")
    # logging.info(f"   DB_NAME: '{DB_NAME}'")
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
        
    try: # TRY UTAMA UNTUK SELURUH OPERASI GOOGLE SHEETS
        # Menggunakan gspread.service_account untuk inisialisasi client
        logging.info(f"gspread module imported: {gspread}") # DEBUG
        logging.info(f"Attempting to init client with: gspread.service_account(filename='{GOOGLE_SHEETS_CREDENTIALS_FILE}')") # DEBUG

        client = gspread.service_account(filename=GOOGLE_SHEETS_CREDENTIALS_FILE)
        logging.info(f"Type of client object: {type(client)}") # DEBUG
        logging.info(f"Does client have 'open_by_id'?: {hasattr(client, 'open_by_id')}") # DEBUG
        spreadsheet = None

        # 1. Coba buka dengan ID jika ada
        if GOOGLE_SHEET_ID:
            try:
                logging.info(f"Attempting to open Google Sheet by ID: {GOOGLE_SHEET_ID}")
                spreadsheet = client.open_by_id(GOOGLE_SHEET_ID)
                logging.info(f"Successfully opened Google Sheet '{spreadsheet.title}' using ID.")
            except gspread.exceptions.SpreadsheetNotFound:
                logging.warning(f"Google Sheet with ID '{GOOGLE_SHEET_ID}' not found.")
                service_account_email = client.auth.service_account_email
                logging.error(f"If GOOGLE_SHEET_ID is specified in .env, the sheet must exist and be shared with {service_account_email} as Editor.")
                return False # Gagal jika ID ada tapi sheet tidak ditemukan (asumsi ID harus valid jika diberikan)
            except gspread.exceptions.APIError as e:
                logging.error(f"API error opening Google Sheet by ID '{GOOGLE_SHEET_ID}': {e}. Check permissions or ID validity.")
                return False

        # 2. Jika tidak ada ID (atau jika ID gagal dan kita ingin fallback), coba dengan NAMA
        # Kondisi "elif" memastikan ini hanya berjalan jika ID tidak ada ATAU spreadsheet belum ditemukan via ID
        if not spreadsheet and GOOGLE_SHEET_NAME: 
            try:
                logging.info(f"Attempting to open/create Google Sheet by name: {GOOGLE_SHEET_NAME}")
                spreadsheet = client.open(GOOGLE_SHEET_NAME)
                logging.info(f"Successfully opened Google Sheet by name: '{GOOGLE_SHEET_NAME}'")
            except gspread.exceptions.SpreadsheetNotFound:
                logging.info(f"Spreadsheet '{GOOGLE_SHEET_NAME}' not found by name. Creating new one with this name.")
                spreadsheet = client.create(GOOGLE_SHEET_NAME)
                service_account_email = client.auth.service_account_email
                logging.info(f"Spreadsheet '{GOOGLE_SHEET_NAME}' created. URL: {spreadsheet.url}")
                logging.info(f"Ensure it's shared appropriately with {service_account_email} as Editor if others need access or if it wasn't auto-shared correctly.")
        
        if not spreadsheet:
            # Pesan error jika setelah semua upaya, spreadsheet tetap tidak bisa diakses/dibuat
            service_account_email_msg = ""
            try:
                if 'client' in locals() and hasattr(client, 'auth') and hasattr(client.auth, 'service_account_email'):
                    service_account_email_msg = f"with service account '{client.auth.service_account_email}'"
            except Exception:
                pass 
            logging.error(f"Could not open or create Google Sheet. Please check GOOGLE_SHEET_ID/GOOGLE_SHEET_NAME in .env and ensure sharing permissions are correct {service_account_email_msg}.")
            return False

        worksheet_title = "Products Data" # Anda bisa buat ini konfigurabel juga jika mau
        try:
            worksheet = spreadsheet.worksheet(worksheet_title)
            logging.info(f"Using existing worksheet: '{worksheet_title}'")
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f"Worksheet '{worksheet_title}' not found. Creating new one.")
            worksheet = spreadsheet.add_worksheet(title=worksheet_title, rows="1", cols="1") # Start small
        
        worksheet.clear()
        
        df_gsp = df.copy()
        if 'Timestamp' in df_gsp.columns:
            df_gsp['Timestamp'] = df_gsp['Timestamp'].astype(str) # Konversi Timestamp ke string

        worksheet.update([df_gsp.columns.values.tolist()] + df_gsp.values.tolist())
        logging.info(f"Data successfully saved to Google Sheet: '{spreadsheet.title}', Worksheet: '{worksheet_title}'")
        logging.info(f"Sheet URL: {spreadsheet.url}")
        return True
        
    except Exception as e: # Menangkap semua exception lain selama proses Google Sheets
        logging.error(f"An unexpected error occurred while saving to Google Sheets: {e}")
        return False