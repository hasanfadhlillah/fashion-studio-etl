import pytest
import pandas as pd
import logging
from unittest.mock import patch, MagicMock # mock_open tidak diperlukan lagi untuk CSV dasar
from sqlalchemy.exc import OperationalError
import gspread # Untuk gspread.exceptions

# Impor fungsi dan konstanta yang akan diuji/digunakan
from utils.load import save_to_csv, save_to_postgresql, save_to_google_sheets
from utils.config import (
    CSV_FILE_PATH, POSTGRES_TABLE_NAME, 
    GOOGLE_SHEETS_CREDENTIALS_FILE, GOOGLE_SHEET_NAME, GOOGLE_SHEET_ID
)
from datetime import datetime
import os # Untuk mock os.path.exists

@pytest.fixture
def sample_clean_df():
    """Provides a sample cleaned DataFrame for testing loading."""
    now_dt = datetime.now()
    data = {
        'Title': ["Cleaned Product A", "Cleaned Product B"],
        'Price': [160000, 320000], 
        'Rating': [4.5, 3.8],
        'Colors': [3, 1],
        'Size': ["M", "L"],
        'Gender': ["Men", "Women"],
        'Timestamp': [pd.to_datetime(now_dt), pd.to_datetime(now_dt)] # Pastikan datetime
    }
    df = pd.DataFrame(data)
    # Pastikan tipe data
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Price'] = df['Price'].astype('int64')
    df['Rating'] = df['Rating'].astype('float64')
    df['Colors'] = df['Colors'].astype('int64')
    return df

@pytest.fixture
def empty_df():
    return pd.DataFrame()

# --- Test save_to_csv ---
@patch('pandas.DataFrame.to_csv')
def test_save_to_csv_success(mock_to_csv, sample_clean_df):
    assert save_to_csv(sample_clean_df, "test.csv") is True
    mock_to_csv.assert_called_once_with("test.csv", index=False, encoding='utf-8')

@patch('pandas.DataFrame.to_csv', side_effect=IOError("Disk full"))
def test_save_to_csv_io_error(mock_to_csv, sample_clean_df, caplog):
    with caplog.at_level(logging.ERROR):
        assert save_to_csv(sample_clean_df, "test.csv") is False
    assert "Error saving data to CSV test.csv: Disk full" in caplog.text

def test_save_to_csv_empty_df(empty_df, caplog):
    with caplog.at_level(logging.WARNING):
        assert save_to_csv(empty_df, "test.csv") is False
    assert "DataFrame is empty. Skipping CSV save." in caplog.text

# --- Test save_to_postgresql ---
@patch('utils.load.create_engine')
def test_save_to_postgresql_success(mock_create_engine, sample_clean_df):
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine
    # Simulasikan context manager untuk with engine.connect()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection 

    mock_df_to_sql = MagicMock()
    # Patch metode to_sql pada objek DataFrame (semua instance DataFrame)
    with patch.object(pd.DataFrame, 'to_sql', mock_df_to_sql):
        assert save_to_postgresql(sample_clean_df, "test_table") is True
    
    mock_create_engine.assert_called_once() 
    assert mock_connection.execute.call_count > 0 # CREATE TABLE IF NOT EXISTS dipanggil
    mock_df_to_sql.assert_called_once_with("test_table", mock_engine, if_exists='replace', index=False)

@patch('utils.load.create_engine', side_effect=OperationalError("connection failed", "params", "orig_error"))
def test_save_to_postgresql_connection_error(mock_create_engine, sample_clean_df, caplog):
    with caplog.at_level(logging.ERROR):
        assert save_to_postgresql(sample_clean_df, "test_table") is False
    # Pesan error SQLAlchemy bisa kompleks, cek bagian pentingnya
    assert "Error saving data to PostgreSQL table test_table" in caplog.text
    assert "connection failed" in caplog.text 

def test_save_to_postgresql_empty_df(empty_df, caplog):
    with caplog.at_level(logging.WARNING):
        assert save_to_postgresql(empty_df, "test_table") is False
    assert "DataFrame is empty. Skipping PostgreSQL save." in caplog.text

# --- Test save_to_google_sheets ---
# Patch utama adalah gspread.service_account yang dipanggil di utils.load
@patch('utils.load.gspread.service_account') 
@patch('utils.load.os.path.exists', return_value=True) # Asumsikan file creds JSON ada
def test_save_to_google_sheets_success_with_id(mock_os_exists, mock_gspread_service_account, sample_clean_df, monkeypatch):
    mock_client = MagicMock()
    mock_spreadsheet = MagicMock(title="Mocked Sheet Title", url="http://mock.url") # Tambah title & url
    mock_worksheet = MagicMock()

    mock_gspread_service_account.return_value = mock_client
    mock_client.open_by_url.return_value = mock_spreadsheet # Kita pakai open_by_url sekarang jika ID ada
    mock_spreadsheet.worksheet.return_value = mock_worksheet
    mock_spreadsheet.add_worksheet.return_value = mock_worksheet # Untuk kasus worksheet not found

    # Mock GOOGLE_SHEET_ID agar logika ID yang dites
    monkeypatch.setattr('utils.load.GOOGLE_SHEET_ID', "mock_sheet_id_123")
    monkeypatch.setattr('utils.load.GOOGLE_SHEET_NAME', None) # Pastikan logika nama tidak jalan

    assert save_to_google_sheets(sample_clean_df) is True
    
    mock_gspread_service_account.assert_called_once_with(filename=GOOGLE_SHEETS_CREDENTIALS_FILE)
    expected_url = f"https://docs.google.com/spreadsheets/d/mock_sheet_id_123"
    mock_client.open_by_url.assert_called_once_with(expected_url)
    mock_worksheet.clear.assert_called_once()
    mock_worksheet.update.assert_called_once()


@patch('utils.load.os.path.exists', return_value=False) # File creds JSON TIDAK ada
def test_save_to_google_sheets_no_creds_file(mock_os_exists, sample_clean_df, caplog):
    with caplog.at_level(logging.ERROR):
        # Panggil tanpa argumen kedua karena signature fungsi sudah diubah
        assert save_to_google_sheets(sample_clean_df) is False 
    assert f"Google Sheets credentials file not found: {GOOGLE_SHEETS_CREDENTIALS_FILE}" in caplog.text

@patch('utils.load.gspread.service_account', side_effect=Exception("Simulated Auth Error"))
@patch('utils.load.os.path.exists', return_value=True)
def test_save_to_google_sheets_auth_error(mock_os_exists, mock_gspread_service_account, sample_clean_df, caplog):
    with caplog.at_level(logging.ERROR):
        assert save_to_google_sheets(sample_clean_df) is False
    assert "Simulated Auth Error" in caplog.text
    assert "ERROR    root:load.py" in caplog.text # Cek apakah prefix ada
    assert "An unexpected error occurred during Google Sheets operation: Simulated Auth Error" in caplog.text # Lebih cocok dengan pesan penuh tanpa newline

def test_save_to_google_sheets_empty_df(empty_df, caplog):
    with caplog.at_level(logging.WARNING):
        # Panggil tanpa argumen kedua
        assert save_to_google_sheets(empty_df) is False
    assert "DataFrame is empty. Skipping Google Sheets save." in caplog.text

@patch('utils.load.gspread.service_account')
@patch('utils.load.os.path.exists', return_value=True)
def test_save_to_google_sheets_creates_new_sheet_by_name(mock_os_exists, mock_gspread_service_account, sample_clean_df, caplog, monkeypatch):
    mock_client = MagicMock()
    # Mock service account email
    mock_auth_obj = MagicMock()
    mock_auth_obj.service_account_email = "mock_sa_email@example.com"
    mock_client.auth = mock_auth_obj

    mock_gspread_service_account.return_value = mock_client
    
    # Simulasikan SpreadsheetNotFound saat open by name, lalu create berhasil
    mock_client.open.side_effect = gspread.exceptions.SpreadsheetNotFound
    mock_new_spreadsheet = MagicMock(title="Fashion Studio Products", url="http://new.sheet.url")
    mock_client.create.return_value = mock_new_spreadsheet
    
    mock_worksheet = MagicMock()
    mock_new_spreadsheet.worksheet.return_value = mock_worksheet # Atau add_worksheet jika itu yg dipanggil
    mock_new_spreadsheet.add_worksheet.return_value = mock_worksheet


    # Mock GOOGLE_SHEET_ID menjadi None agar logika nama yang dites
    monkeypatch.setattr('utils.load.GOOGLE_SHEET_ID', None) 
    monkeypatch.setattr('utils.load.GOOGLE_SHEET_NAME', "Fashion Studio Products")

    with caplog.at_level(logging.INFO):
        assert save_to_google_sheets(sample_clean_df) is True
    
    print("\n--- CAPTURED LOGS FOR test_save_to_google_sheets_creates_new_sheet_by_name ---")
    print(caplog.text)
    print("--- END OF CAPTURED LOGS ---\n")
    
    mock_client.open.assert_called_once_with("Fashion Studio Products")
    mock_client.create.assert_called_once_with("Fashion Studio Products")
    
    # Pesan log yang diharapkan:
    expected_log_not_found = "Spreadsheet 'Fashion Studio Products' not found by name. Attempting to create new one with this name."
    expected_log_created = "Spreadsheet 'Fashion Studio Products' created. URL: http://new.sheet.url"
    expected_log_shared_info = "Ensure it's shared appropriately as Editor if others need access." # atau bagian dari pesan ini

    assert expected_log_not_found in caplog.text
    assert expected_log_created in caplog.text
    assert expected_log_shared_info in caplog.text