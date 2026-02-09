"""
Web Unlocker Scraper: Custom website scraping using Bright Data's Web Unlocker.

Unlike the Amazon API which returns structured data, Web Unlocker lets you
scrape ANY website by handling anti-bot measures automatically.

Use cases:
- Scrape competitor websites
- Extract data from sites without a pre-built API
- Custom data extraction with BeautifulSoup/lxml
"""

import os
import requests
import pandas as pd
from typing import List, Dict, Any
from urllib.parse import urlparse

if 'data_loader' not in dir():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


# Bright Data Web Unlocker proxy endpoint
WEB_UNLOCKER_HOST = "brd.superproxy.io"
WEB_UNLOCKER_PORT = 33335


def scrape_url_with_unlocker(
    url: str,
    username: str,
    password: str,
    zone: str = "web_unlocker1",
    country: str = "us",
    timeout: int = 60
) -> Dict[str, Any]:
    """
    Scrape a URL using Bright Data's Web Unlocker.

    The Web Unlocker automatically:
    - Rotates IPs
    - Solves CAPTCHAs
    - Handles anti-bot measures
    - Renders JavaScript if needed

    Args:
        url: The URL to scrape
        username: Bright Data customer ID
        password: Zone password
        zone: Web Unlocker zone name
        country: Target country for IP

    Returns:
        Dict with url, status, html, and metadata
    """
    # Build proxy URL with zone and country
    proxy_url = f"http://{username}-zone-{zone}-country-{country}:{password}@{WEB_UNLOCKER_HOST}:{WEB_UNLOCKER_PORT}"

    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    result = {
        "url": url,
        "domain": urlparse(url).netloc,
        "status_code": None,
        "success": False,
        "html_length": 0,
        "html": None,
        "error": None
    }

    try:
        response = requests.get(
            url,
            proxies=proxies,
            headers=headers,
            timeout=timeout,
            verify=False  # Required for proxy
        )

        result["status_code"] = response.status_code
        result["success"] = response.status_code == 200
        result["html_length"] = len(response.text)
        result["html"] = response.text

    except requests.exceptions.Timeout:
        result["error"] = "Timeout"
    except requests.exceptions.RequestException as e:
        result["error"] = str(e)

    return result


def extract_basic_info(html: str, url: str) -> Dict[str, Any]:
    """
    Extract basic information from HTML.

    For production, you'd use BeautifulSoup or lxml for proper parsing.
    This is a simple example that extracts title and meta description.
    """
    info = {
        "title": None,
        "description": None,
        "h1_count": 0,
        "link_count": 0,
        "image_count": 0
    }

    if not html:
        return info

    # Simple regex-based extraction (use BeautifulSoup in production)
    import re

    # Extract title
    title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
    if title_match:
        info["title"] = title_match.group(1).strip()

    # Extract meta description
    desc_match = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if desc_match:
        info["description"] = desc_match.group(1).strip()

    # Count elements
    info["h1_count"] = len(re.findall(r'<h1[^>]*>', html, re.IGNORECASE))
    info["link_count"] = len(re.findall(r'<a[^>]+href=', html, re.IGNORECASE))
    info["image_count"] = len(re.findall(r'<img[^>]+src=', html, re.IGNORECASE))

    return info


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Scrape custom URLs using Bright Data Web Unlocker.

    Pipeline variables:
    - urls: List of URLs to scrape
    - zone: Web Unlocker zone name (default: web_unlocker1)
    - country: Target country (default: us)

    Environment variables:
    - BRIGHT_DATA_CUSTOMER_ID: Your Bright Data customer ID (e.g., brd-customer-xxx)
    - BRIGHT_DATA_ZONE_PASSWORD: Zone password for Web Unlocker
    """
    # Get credentials
    customer_id = os.getenv('BRIGHT_DATA_CUSTOMER_ID')
    zone_password = os.getenv('BRIGHT_DATA_ZONE_PASSWORD')

    if not customer_id or not zone_password:
        print("=" * 60)
        print("WEB UNLOCKER NOT CONFIGURED")
        print("=" * 60)
        print("To use Web Unlocker, set these environment variables:")
        print("  BRIGHT_DATA_CUSTOMER_ID=brd-customer-hl_xxxxx")
        print("  BRIGHT_DATA_ZONE_PASSWORD=your_zone_password")
        print("")
        print("Get credentials from: https://brightdata.com/cp/zones")
        print("=" * 60)

        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=['url', 'domain', 'status_code', 'success', 'title'])

    # Get URLs from pipeline variables
    urls = kwargs.get('urls', [
        "https://example.com",  # Default example
    ])

    zone = kwargs.get('zone', 'web_unlocker1')
    country = kwargs.get('country', 'us')

    if not urls:
        print("No URLs provided. Set 'urls' pipeline variable.")
        return pd.DataFrame()

    print(f"Scraping {len(urls)} URLs with Web Unlocker...")
    print(f"Zone: {zone} | Country: {country}")
    print("-" * 50)

    results = []

    for url in urls:
        print(f"Scraping: {url[:60]}...")

        # Scrape the URL
        result = scrape_url_with_unlocker(
            url=url,
            username=customer_id,
            password=zone_password,
            zone=zone,
            country=country
        )

        # Extract basic info from HTML
        if result["success"] and result["html"]:
            info = extract_basic_info(result["html"], url)
            result.update(info)

            # Don't store full HTML in DataFrame (too large)
            del result["html"]

            print(f"  ✓ Success: {info.get('title', 'No title')[:40]}")
        else:
            print(f"  ✗ Failed: {result.get('error', 'Unknown error')}")

        results.append(result)

    df = pd.DataFrame(results)

    print("-" * 50)
    print(f"Scraped {len(df)} URLs")
    print(f"Success rate: {df['success'].sum()}/{len(df)} ({df['success'].mean()*100:.0f}%)")

    return df


@test
def test_output(output, *args) -> None:
    """Validate output."""
    assert output is not None, 'Output is undefined'
    assert 'url' in output.columns, 'URL column missing'
    assert 'success' in output.columns, 'Success column missing'
