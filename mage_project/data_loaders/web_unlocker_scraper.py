"""
Web Unlocker API Scraper: Scrape ANY website using Bright Data's Web Unlocker API.

This uses the API method (not proxy method) for simpler integration.
The Web Unlocker automatically handles:
- IP rotation
- CAPTCHA solving
- Anti-bot bypass
- JavaScript rendering
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


WEB_UNLOCKER_API = "https://api.brightdata.com/request"


def scrape_url_with_api(
    url: str,
    api_token: str,
    zone: str = "web_unlocker",
    timeout: int = 60
) -> Dict[str, Any]:
    """
    Scrape a URL using Bright Data's Web Unlocker API.

    Args:
        url: The URL to scrape
        api_token: Bright Data API token
        zone: Web Unlocker zone name (default: web_unlocker)
        timeout: Request timeout in seconds

    Returns:
        Dict with url, status, html, and metadata
    """
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
        response = requests.post(
            WEB_UNLOCKER_API,
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json"
            },
            json={
                "zone": zone,
                "url": url,
                "format": "raw"
            },
            timeout=timeout
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


def extract_basic_info(html: str) -> Dict[str, Any]:
    """Extract basic information from HTML."""
    import re

    info = {
        "title": None,
        "description": None,
        "h1_count": 0,
        "link_count": 0,
        "image_count": 0
    }

    if not html:
        return info

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
    Scrape custom URLs using Bright Data Web Unlocker API.

    Pipeline variables:
    - urls: List of URLs to scrape
    - zone: Web Unlocker zone name (default: web_unlocker)

    Environment variables:
    - BRIGHT_DATA_API_TOKEN: Your Bright Data API token
    """
    api_token = os.getenv('BRIGHT_DATA_API_TOKEN')

    if not api_token:
        raise ValueError(
            "BRIGHT_DATA_API_TOKEN not set. "
            "Get your token from: https://brightdata.com/cp/api_tokens"
        )

    # Get URLs from pipeline variables
    urls = kwargs.get('urls', [
        "https://example.com",
        "https://httpbin.org/html"
    ])

    zone = kwargs.get('zone', 'web_unlocker')

    if not urls:
        print("No URLs provided. Set 'urls' pipeline variable.")
        return pd.DataFrame()

    print(f"Scraping {len(urls)} URLs with Web Unlocker API...")
    print(f"Zone: {zone}")
    print("-" * 50)

    results = []

    for url in urls:
        print(f"Scraping: {url[:60]}...")

        result = scrape_url_with_api(
            url=url,
            api_token=api_token,
            zone=zone
        )

        # Extract basic info from HTML
        if result["success"] and result["html"]:
            info = extract_basic_info(result["html"])
            result.update(info)
            del result["html"]  # Don't store full HTML
            print(f"  ✓ Success: {info.get('title', 'No title')[:40]}")
        else:
            print(f"  ✗ Failed: {result.get('error', 'Unknown error')}")

        results.append(result)

    df = pd.DataFrame(results)

    print("-" * 50)
    print(f"Scraped {len(df)} URLs")
    print(f"Success rate: {df['success'].sum()}/{len(df)}")

    return df


@test
def test_output(output, *args) -> None:
    """Validate output."""
    assert output is not None, 'Output is undefined'
