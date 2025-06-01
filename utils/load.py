import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from google.oauth2.service_account import Credentials
from .config import (
    CSV_FILE_PATH, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME,
    POSTGRES_TABLE_NAME, GOOGLE_SHEETS_CREDENTIALS_FILE, GOOGLE_SHEET_NAME
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
            # This is a very basic schema, adjust types as needed for production
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
            # For robust upsert, a more complex SQL merge/on_conflict_do_update is needed.
            # Here, 'replace' will drop and recreate, which might not be ideal.
            # 'append' is safer if duplicates are handled upstream or by DB constraints.
            # Let's use append and rely on the primary key for uniqueness if set up.
            # Or, to ensure fresh data each run and simple 'replace':
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


def save_to_google_sheets(df, sheet_name=GOOGLE_SHEET_NAME):
    """Saves DataFrame to a Google Sheet."""
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

        try:
            spreadsheet = client.open(sheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            logging.info(f"Spreadsheet '{sheet_name}' not found. Creating new one.")
            spreadsheet = client.create(sheet_name)
            # Share with yourself or others if needed - replace with your email
            # spreadsheet.share('your-email@example.com', perm_type='user', role='writer')
            logging.info(f"Please share '{sheet_name}' with desired users, including the service account email if it's not the owner.")


        worksheet_title = "Products Data"
        try:
            worksheet = spreadsheet.worksheet(worksheet_title)
            logging.info(f"Found existing worksheet: '{worksheet_title}'")
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f"Worksheet '{worksheet_title}' not found. Creating new one.")
            worksheet = spreadsheet.add_worksheet(title=worksheet_title, rows="1", cols="1") # Start small

        worksheet.clear() # Clear existing data
        
        # Convert Timestamp to string for Google Sheets compatibility
        df_gsp = df.copy()
        if 'Timestamp' in df_gsp.columns:
            df_gsp['Timestamp'] = df_gsp['Timestamp'].astype(str)

        worksheet.update([df_gsp.columns.values.tolist()] + df_gsp.values.tolist())
        logging.info(f"Data successfully saved to Google Sheet: '{sheet_name}', Worksheet: '{worksheet_title}'")
        logging.info(f"Sheet URL: {spreadsheet.url}")
        return True
    except FileNotFoundError: # For credentials file
         logging.error(f"Credentials file '{GOOGLE_SHEETS_CREDENTIALS_FILE}' not found.")
         return False
    except Exception as e:
        logging.error(f"Error saving data to Google Sheets '{sheet_name}': {e}")
        return False