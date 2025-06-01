import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://fashion-studio.dicoding.dev"
MAX_PAGES = 50 # Sesuai permintaan, 50 halaman
USD_TO_IDR_EXCHANGE_RATE = 16000.0

# PostgreSQL Configuration
DB_USER = os.getenv("DB_USER", "default_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "default_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fashion_db")
POSTGRES_TABLE_NAME = "products"

# Google Sheets Configuration
# Pastikan file google-sheets-api.json ada di root direktori proyek
GOOGLE_SHEETS_CREDENTIALS_FILE = "google-sheets-api.json"
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Dicoding Fashion Products")
# GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID") # Alternatif jika menggunakan ID

CSV_FILE_PATH = "products.csv"