"""
Multi-Platform Ecommerce Scraper using Bright Data Web Scraper APIs.

Bright Data provides pre-built scraper APIs for multiple ecommerce platforms:
- Amazon (product discovery, product details, reviews, sellers)
- Google Shopping
- eBay
- Walmart
- Target
- Wayfair
- And many more!

This loader lets you scrape the same keywords across multiple platforms
for competitive analysis and market research.
"""

import os
import requests
import pandas as pd
import time
from typing import List, Dict, Any

if 'data_loader' not in dir():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


# Bright Data Dataset IDs for different platforms
# Get more from: https://brightdata.com/products/web-scraper
DATASET_IDS = {
    "amazon": "gd_l7q7dkf244hwjntr0",           # Amazon Products Discovery
    "amazon_product": "gd_l7q7dkf244hwjntr0",   # Amazon Product Details
    "google_shopping": "gd_lz6x2hq92h3r3phsy0", # Google Shopping
    "ebay": "gd_ltr923oln1ryujh9n",              # eBay Products
    "walmart": "gd_lwdb2lfz1k2c7lcel8",          # Walmart Products
    "target": "gd_l1vikfch2fp1cy17ws",           # Target Products
    "wayfair": "gd_l1vikfch1hm8nt10o0",          # Wayfair Products
    "bestbuy": "gd_l7q7dkf244hwjntr0",           # Best Buy (placeholder)
    "homedepot": "gd_l1vikfch2fp1cy17ws",        # Home Depot (placeholder)
}

API_BASE = "https://api.brightdata.com/datasets/v3"


def scrape_platform(
    platform: str,
    keywords: List[str],
    api_token: str,
    limit_per_keyword: int = 25,
    max_wait_seconds: int = 300
) -> List[Dict[str, Any]]:
    """
    Scrape products from a specific platform.

    Args:
        platform: Platform name (amazon, google_shopping, ebay, walmart, etc.)
        keywords: Search keywords
        api_token: Bright Data API token
        limit_per_keyword: Max products per keyword
        max_wait_seconds: Max wait time for results

    Returns:
        List of product dictionaries
    """
    dataset_id = DATASET_IDS.get(platform.lower())

    if not dataset_id:
        print(f"Warning: Unknown platform '{platform}'. Skipping.")
        return []

    print(f"\n{'='*50}")
    print(f"Scraping {platform.upper()}")
    print(f"Keywords: {keywords}")
    print(f"{'='*50}")

    # Prepare input based on platform
    if platform.lower() == "google_shopping":
        input_data = [{"keyword": kw, "country": "us"} for kw in keywords]
    else:
        input_data = [{"keyword": kw} for kw in keywords]

    # Trigger scrape
    try:
        response = requests.post(
            f"{API_BASE}/scrape",
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json"
            },
            params={
                "dataset_id": dataset_id,
                "notify": "false",
                "include_errors": "true",
                "type": "discover_new",
                "discover_by": "keyword",
                "limit_per_input": limit_per_keyword
            },
            json={"input": input_data},
            timeout=30
        )

        result = response.json()

        if "snapshot_id" not in result:
            print(f"Failed to start {platform} scrape: {result}")
            return []

        snapshot_id = result["snapshot_id"]
        print(f"Snapshot ID: {snapshot_id}")

    except Exception as e:
        print(f"Error starting {platform} scrape: {e}")
        return []

    # Poll for results
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        print(f"  Waiting for {platform} results... ({int(time.time() - start_time)}s)")
        time.sleep(30)

        try:
            snapshot_response = requests.get(
                f"{API_BASE}/snapshot/{snapshot_id}",
                headers={"Authorization": f"Bearer {api_token}"},
                params={"format": "json"},
                timeout=30
            )

            data = snapshot_response.json()

            if isinstance(data, list):
                products = [d for d in data if "error" not in d]

                # Add platform identifier
                for p in products:
                    p['source_platform'] = platform

                print(f"  Got {len(products)} products from {platform}")
                return products

            if isinstance(data, dict) and data.get("status") in ["running", "closing"]:
                continue

        except Exception as e:
            print(f"  Error polling: {e}")

    print(f"  Timeout waiting for {platform} results")
    return []


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Scrape products from multiple ecommerce platforms.

    Pipeline variables:
    - keywords: List of search keywords
    - platforms: List of platforms to scrape (default: ["amazon"])
    - limit_per_keyword: Max products per keyword per platform

    Available platforms:
    - amazon: Amazon product discovery
    - google_shopping: Google Shopping results
    - ebay: eBay product listings
    - walmart: Walmart products
    - target: Target products
    - wayfair: Wayfair products

    Example config:
    ```yaml
    variables:
      keywords:
        - laptop stand
        - mechanical keyboard
      platforms:
        - amazon
        - google_shopping
        - walmart
      limit_per_keyword: 20
    ```
    """
    api_token = os.getenv('BRIGHT_DATA_API_TOKEN')

    if not api_token:
        raise ValueError("BRIGHT_DATA_API_TOKEN not set")

    keywords = kwargs.get('keywords', ['laptop stand', 'mechanical keyboard'])
    platforms = kwargs.get('platforms', ['amazon'])
    limit_per_keyword = kwargs.get('limit_per_keyword', 25)

    print(f"Multi-Platform Ecommerce Scraper")
    print(f"Platforms: {platforms}")
    print(f"Keywords: {keywords}")
    print(f"Limit per keyword: {limit_per_keyword}")

    all_products = []

    for platform in platforms:
        products = scrape_platform(
            platform=platform,
            keywords=keywords,
            api_token=api_token,
            limit_per_keyword=limit_per_keyword
        )
        all_products.extend(products)

    if not all_products:
        print("\nNo products found from any platform!")
        return pd.DataFrame()

    df = pd.DataFrame(all_products)

    # Normalize column names across platforms
    column_mapping = {
        # Common mappings
        'name': 'title',
        'product_name': 'title',
        'item_price': 'final_price',
        'price': 'final_price',
        'current_price': 'final_price',
        'stars': 'rating',
        'star_rating': 'rating',
        'review_count': 'reviews_count',
        'num_reviews': 'reviews_count',
    }

    for old_col, new_col in column_mapping.items():
        if old_col in df.columns and new_col not in df.columns:
            df[new_col] = df[old_col]

    print(f"\n{'='*50}")
    print(f"TOTAL: {len(df)} products from {len(platforms)} platforms")

    if 'source_platform' in df.columns:
        print("\nProducts by platform:")
        for platform, count in df['source_platform'].value_counts().items():
            print(f"  - {platform}: {count}")

    return df


@test
def test_output(output, *args) -> None:
    """Validate output."""
    assert output is not None, 'Output is undefined'
    if len(output) > 0:
        assert 'source_platform' in output.columns, 'Missing platform identifier'
