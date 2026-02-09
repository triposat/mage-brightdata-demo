"""
Amazon Product Discovery using Bright Data Web Scraper API.

Discovers products by keywords and returns structured product data
including title, price, rating, reviews, and 50+ other fields.
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


DATASET_ID = "gd_l7q7dkf244hwjntr0"  # Amazon Products dataset
API_BASE = "https://api.brightdata.com/datasets/v3"


def discover_amazon_products(
    keywords: List[str],
    api_token: str,
    limit_per_keyword: int = 40,
    max_wait_seconds: int = 300
) -> List[Dict[str, Any]]:
    """
    Discover Amazon products by keywords using Bright Data Web Scraper API.

    Args:
        keywords: List of search keywords (e.g., ["light bulb", "dog toys"])
        api_token: Bright Data API token
        limit_per_keyword: Max products per keyword (default: 40)
        max_wait_seconds: Max time to wait for results (default: 300)

    Returns:
        List of product dictionaries with structured data
    """

    # Prepare input
    input_data = [{"keyword": kw} for kw in keywords]

    print(f"Discovering Amazon products for {len(keywords)} keywords...")
    print(f"Keywords: {keywords}")
    print(f"Limit per keyword: {limit_per_keyword}")
    print("-" * 50)

    # Trigger the scrape
    response = requests.post(
        f"{API_BASE}/scrape",
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        },
        params={
            "dataset_id": DATASET_ID,
            "notify": "false",
            "include_errors": "true",
            "type": "discover_new",
            "discover_by": "keyword",
            "limit_per_input": limit_per_keyword
        },
        json={"input": input_data}
    )

    result = response.json()

    if "snapshot_id" not in result:
        raise Exception(f"Failed to start scrape: {result}")

    snapshot_id = result["snapshot_id"]
    print(f"Scrape started. Snapshot ID: {snapshot_id}")

    # Poll for results
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        print(f"Waiting for results... ({int(time.time() - start_time)}s elapsed)")
        time.sleep(30)

        # Fetch snapshot
        snapshot_response = requests.get(
            f"{API_BASE}/snapshot/{snapshot_id}",
            headers={"Authorization": f"Bearer {api_token}"},
            params={"format": "json"}
        )

        data = snapshot_response.json()

        # Check if we got results (list of products)
        if isinstance(data, list):
            successful = [d for d in data if "error" not in d]
            errors = [d for d in data if "error" in d]

            print(f"Results ready! Total: {len(data)} | Successful: {len(successful)} | Errors: {len(errors)}")
            return successful

        # Still processing
        if isinstance(data, dict) and data.get("status") in ["running", "closing"]:
            continue

        # Unknown response
        print(f"Unexpected response: {data}")

    raise Exception(f"Timeout waiting for results after {max_wait_seconds}s")


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load Amazon product data by discovering products via keywords.

    Pipeline variables:
        keywords: List of search keywords (default: ["wireless earbuds", "phone case"])
        limit_per_keyword: Max products per keyword (default: 40)
    """
    api_token = os.getenv('BRIGHT_DATA_API_TOKEN')

    if not api_token:
        raise ValueError("BRIGHT_DATA_API_TOKEN environment variable is required")

    # Get keywords from pipeline variables or use defaults
    keywords = kwargs.get('keywords', [
        "wireless earbuds",
        "phone case",
    ])

    limit_per_keyword = kwargs.get('limit_per_keyword', 40)

    # Discover products
    products = discover_amazon_products(
        keywords=keywords,
        api_token=api_token,
        limit_per_keyword=limit_per_keyword
    )

    if not products:
        print("No products found!")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(products)

    # Select most useful columns (if they exist)
    priority_columns = [
        'title', 'brand', 'asin', 'url',
        'initial_price', 'final_price', 'currency',
        'rating', 'reviews_count', 'bought_past_month',
        'availability', 'categories',
        'seller_name', 'image_url',
        'discovery_input'
    ]

    available_columns = [c for c in priority_columns if c in df.columns]
    other_columns = [c for c in df.columns if c not in priority_columns]

    # Reorder columns
    df = df[available_columns + other_columns]

    print("-" * 50)
    print(f"Loaded {len(df)} Amazon products")
    print(f"Columns: {len(df.columns)}")

    # Show sample
    if 'title' in df.columns:
        print("\nSample products:")
        for i, row in df.head(3).iterrows():
            title = row.get('title', 'N/A')[:50]
            price = row.get('final_price') or row.get('initial_price', 'N/A')
            rating = row.get('rating', 'N/A')
            print(f"  - {title}... | ${price} | {rating}★")

    return df


@test
def test_output(output, *args) -> None:
    """Validate output."""
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No products found'
    assert 'title' in output.columns or 'asin' in output.columns, 'Missing product identifiers'
