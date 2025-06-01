import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from sqlalchemy.exc import OperationalError
import gspread

from utils.load import save_to_csv, save_to_postgresql, save_to_google_sheets
from utils.config import CSV_FILE_PATH, POSTGRES_TABLE_NAME, GOOGLE_SHEET_NAME, GOOGLE_SHEETS_CREDENTIALS_FILE
from datetime import datetime

@pytest.fixture
def sample_clean_df():
    """Provides a sample cleaned DataFrame for testing loading."""
    now_dt = datetime.now()
    data = {
        'Title': ["Cleaned Product A", "Cleaned Product B"],
        'Price': [160000, 320000], # IDR
        'Rating': [4.5, 3.8],
        'Colors': [3, 1],
        'Size': ["M", "L"],
        'Gender': ["Men", "Women"],
        'Timestamp': [now_dt, now_dt]
    }
    df = pd.DataFrame(data)
    # Ensure dtypes match what's expected after transformation
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
    assert save_to_csv(sample_clean_df, "test.csv") is False
    assert "Error saving data to CSV test.csv: Disk full" in caplog.text

def test_save_to_csv_empty_df(empty_df, caplog):
    assert save_to_csv(empty_df, "test.csv") is False
    assert "DataFrame is empty. Skipping CSV save." in caplog.text


# --- Test save_to_postgresql ---
@patch('utils.load.create_engine')
def test_save_to_postgresql_success(mock_create_engine, sample_clean_df):
    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_engine.connect.return_value.__enter__.return_value = mock_connection # For 'with ... as ...'

    mock_df_to_sql = MagicMock()
    with patch.object(pd.DataFrame, 'to_sql', mock_df_to_sql):
        assert save_to_postgresql(sample_clean_df, "test_table") is True
        mock_create_engine.assert_called_once() # Check if connection string was formed
        mock_connection.execute.assert_called() # Check if CREATE TABLE IF NOT EXISTS was called
        mock_df_to_sql.assert_called_once_with("test_table", mock_engine, if_exists='replace', index=False)

@patch('utils.load.create_engine', side_effect=OperationalError("connection failed", "", ""))
def test_save_to_postgresql_connection_error(mock_create_engine, sample_clean_df, caplog):
    assert save_to_postgresql(sample_clean_df, "test_table") is False
    assert "Error saving data to PostgreSQL table test_table: (sqlalchemy.exc.OperationalError) connection failed" in caplog.text

def test_save_to_postgresql_empty_df(empty_df, caplog):
    assert save_to_postgresql(empty_df, "test_table") is False
    assert "DataFrame is empty. Skipping PostgreSQL save." in caplog.text

# --- Test save_to_google_sheets ---
@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
@patch('utils.load.os.path.exists', return_value=True) # Assume credentials file exists
def test_save_to_google_sheets_success(mock_os_exists, mock_gspread_authorize, mock_creds_from_file, sample_clean_df):
    mock_credentials = MagicMock()
    mock_creds_from_file.return_value = mock_credentials
    
    mock_gc = MagicMock() # Mock gspread client
    mock_gspread_authorize.return_value = mock_gc
    
    mock_spreadsheet = MagicMock()
    mock_gc.open.return_value = mock_spreadsheet
    mock_gc.create.return_value = mock_spreadsheet # For when sheet not found
    
    mock_worksheet = MagicMock()
    mock_spreadsheet.worksheet.return_value = mock_worksheet
    mock_spreadsheet.add_worksheet.return_value = mock_worksheet # For when worksheet not found

    assert save_to_google_sheets(sample_clean_df, "TestSheet") is True
    
    mock_creds_from_file.assert_called_once_with(GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    mock_gspread_authorize.assert_called_once_with(mock_credentials)
    mock_gc.open.assert_called_once_with("TestSheet")
    mock_worksheet.clear.assert_called_once()
    
    # Check if update was called (actual data check is complex with mocks, focus on call)
    assert mock_worksheet.update.called
    
    # Verify Timestamp conversion (example for first data row)
    # args_list will contain a list, where the first element is the list of lists passed to update
    call_args = mock_worksheet.update.call_args[0][0] # Gets the [df.columns.values.tolist()] + df.values.tolist()
    # print(call_args) # for debugging
    first_data_row_sent = call_args[1] # First row of data sent to worksheet.update
    original_timestamp_col_index = sample_clean_df.columns.get_loc('Timestamp')
    
    assert isinstance(first_data_row_sent[original_timestamp_col_index], str)


@patch('utils.load.os.path.exists', return_value=False) # Credentials file does NOT exist
def test_save_to_google_sheets_no_creds_file(mock_os_exists, sample_clean_df, caplog):
    assert save_to_google_sheets(sample_clean_df, "TestSheet") is False
    assert f"Google Sheets credentials file not found: {GOOGLE_SHEETS_CREDENTIALS_FILE}" in caplog.text

@patch('utils.load.Credentials.from_service_account_file', side_effect=Exception("Auth error"))
@patch('utils.load.os.path.exists', return_value=True)
def test_save_to_google_sheets_auth_error(mock_os_exists, mock_creds_from_file, sample_clean_df, caplog):
    assert save_to_google_sheets(sample_clean_df, "TestSheet") is False
    assert "Error saving data to Google Sheets 'TestSheet': Auth error" in caplog.text

def test_save_to_google_sheets_empty_df(empty_df, caplog):
    assert save_to_google_sheets(empty_df, "TestSheet") is False
    assert "DataFrame is empty. Skipping Google Sheets save." in caplog.text

@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
@patch('utils.load.os.path.exists', return_value=True)
def test_save_to_google_sheets_creates_new_sheet(mock_os_exists, mock_gspread_authorize, mock_creds_from_file, sample_clean_df, caplog):
    mock_gc = MagicMock()
    mock_gspread_authorize.return_value = mock_gc
    
    # Simulate SpreadsheetNotFound, then successful creation
    mock_gc.open.side_effect = gspread.exceptions.SpreadsheetNotFound
    mock_new_spreadsheet = MagicMock()
    mock_new_spreadsheet.url = "http://new.sheet.url" # mock the url attribute
    mock_gc.create.return_value = mock_new_spreadsheet
    
    mock_worksheet = MagicMock()
    mock_new_spreadsheet.worksheet.return_value = mock_worksheet # if it tries to get an existing ws
    mock_new_spreadsheet.add_worksheet.return_value = mock_worksheet # if it adds a new ws

    assert save_to_google_sheets(sample_clean_df, "NewNonExistentSheet") is True
    mock_gc.create.assert_called_once_with("NewNonExistentSheet")
    assert "Spreadsheet 'NewNonExistentSheet' not found. Creating new one." in caplog.text
    assert "Sheet URL: http://new.sheet.url" in caplog.text # Verify URL logging