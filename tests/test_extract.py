import pytest
import requests_mock
from utils.extract import fetch_page_content, parse_product_data, extract_all_products
from utils.config import BASE_URL, MAX_PAGES
import pandas as pd
from datetime import datetime

MOCK_HTML_PAGE_1 = """
<html><body>
    <div class="collection-card">
        <h3 class="product-title">Cool T-Shirt</h3>
        <div class="price-container"><span class="price">$25.99</span></div>
        <p>Rating: 4.5 / 5</p>
        <p>3 Colors</p>
        <p>Size: M</p>
        <p>Gender: Men</p>
    </div>
    <div class="collection-card">
        <h3 class="product-title">Awesome Jeans</h3>
        <p class="price">Price Unavailable</p>
        <p>Rating: Invalid Rating / 5</p>
        <p>2 Colors</p>
        <p>Size: L</p>
        <p>Gender: Unisex</p>
    </div>
</body></html>
"""

MOCK_HTML_PAGE_EMPTY = "<html><body></body></html>" # No product cards
MOCK_HTML_PAGE_UNKNOWN = """
<html><body>
    <div class="collection-card">
        <h3 class="product-title">Unknown Product</h3>
        <div class="price-container"><span class="price">$10.00</span></div>
        <p>Rating: 3.0 / 5</p>
        <p>1 Color</p> <p>Size: S</p>
        <p>Gender: Women</p>
    </div>
</body></html>
"""


@pytest.fixture
def mock_requests():
    with requests_mock.Mocker() as m:
        yield m

def test_fetch_page_content_success(mock_requests):
    """Test fetching page content successfully."""
    page_num = 1
    url = f"{BASE_URL}/page/{page_num}"
    mock_requests.get(url, text="<html>Success</html>", status_code=200)
    content = fetch_page_content(page_num)
    assert content == b"<html>Success</html>"

def test_fetch_page_content_failure(mock_requests, caplog):
    """Test fetching page content with a network error."""
    page_num = 1
    url = f"{BASE_URL}/page/{page_num}"
    mock_requests.get(url, status_code=500)
    content = fetch_page_content(page_num)
    assert content is None
    assert f"Error fetching page {page_num}" in caplog.text

def test_parse_product_data_success():
    """Test parsing product data from valid HTML."""
    products = parse_product_data(MOCK_HTML_PAGE_1, 1)
    assert len(products) == 2
    assert products[0]['Title'] == "Cool T-Shirt"
    assert products[0]['Price'] == "$25.99"
    assert products[0]['Rating'] == "Rating: 4.5 / 5"
    assert products[0]['Colors'] == "3 Colors"
    assert products[0]['Size'] == "Size: M"
    assert products[0]['Gender'] == "Gender: Men"
    assert isinstance(products[0]['Timestamp'], datetime)

    assert products[1]['Title'] == "Awesome Jeans"
    assert products[1]['Price'] == "Price Unavailable" # Correctly captured
    assert products[1]['Rating'] == "Rating: Invalid Rating / 5"

def test_parse_product_data_no_cards(caplog):
    """Test parsing when no product cards are found."""
    products = parse_product_data(MOCK_HTML_PAGE_EMPTY, 1)
    assert len(products) == 0
    assert "No product cards found on page 1" in caplog.text

def test_parse_product_data_parsing_error(caplog):
    """Test parsing with malformed HTML (simulated by passing None)."""
    # This simulates a more fundamental parsing issue within BeautifulSoup if HTML is totally unparsable
    # or a different kind of unexpected structure that causes an unhandled exception.
    # Here, we pass None which will cause TypeError inside BeautifulSoup
    products = parse_product_data(None, 1)
    assert len(products) == 0
    assert "Error parsing product data on page 1" in caplog.text

def test_extract_all_products_success(mock_requests, monkeypatch):
    """Test the full extraction process across multiple mocked pages."""
    # Mock MAX_PAGES to a smaller number for faster testing
    monkeypatch.setattr('utils.extract.MAX_PAGES', 2)
    
    url_page1 = f"{BASE_URL}/page/1"
    url_page2 = f"{BASE_URL}/page/2"
    
    mock_requests.get(url_page1, text=MOCK_HTML_PAGE_1, status_code=200)
    mock_requests.get(url_page2, text=MOCK_HTML_PAGE_UNKNOWN, status_code=200)

    df = extract_all_products()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3 # 2 from page 1, 1 from page 2
    assert df.iloc[0]['Title'] == "Cool T-Shirt"
    assert df.iloc[2]['Title'] == "Unknown Product" # Still extracted, transform handles it

def test_extract_all_products_fetch_failure_one_page(mock_requests, caplog, monkeypatch):
    """Test extraction when one page fails to fetch but others succeed."""
    monkeypatch.setattr('utils.extract.MAX_PAGES', 2)
    url_page1 = f"{BASE_URL}/page/1"
    url_page2 = f"{BASE_URL}/page/2"

    mock_requests.get(url_page1, text=MOCK_HTML_PAGE_1, status_code=200)
    mock_requests.get(url_page2, status_code=500) # Page 2 fails

    df = extract_all_products()
    assert len(df) == 2 # Only products from page 1
    assert "Error fetching page 2" in caplog.text
    assert "Skipping page 2 due to fetch error" in caplog.text

def test_extract_all_products_no_data_extracted(mock_requests, caplog, monkeypatch):
    """Test extraction when no data is extracted from any page."""
    monkeypatch.setattr('utils.extract.MAX_PAGES', 1)
    url_page1 = f"{BASE_URL}/page/1"
    mock_requests.get(url_page1, text=MOCK_HTML_PAGE_EMPTY, status_code=200) # Page has no products

    df = extract_all_products()
    assert df.empty
    assert "No products extracted from page 1" in caplog.text
    assert "No data was extracted from any page" in caplog.text

def test_extract_all_products_critical_failure(monkeypatch, caplog):
    """Test overall extraction process failure (e.g., if requests itself fails fundamentally)."""
    # Simulate a critical error by making requests.get raise an unexpected exception
    def mock_requests_get_fails(*args, **kwargs):
        raise Exception("Simulated critical network failure")

    monkeypatch.setattr('utils.extract.requests.get', mock_requests_get_fails)
    monkeypatch.setattr('utils.extract.MAX_PAGES', 1)

    df = extract_all_products()
    assert df.empty
    assert "An critical error occurred during the extraction process" in caplog.text