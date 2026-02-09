import re
import pandas as pd
from typing import Optional

if 'transformer' not in dir():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


def clean_price(price_str: Optional[str]) -> Optional[float]:
    """
    Extract numeric price from string.

    Handles formats like: $19.99, USD 19.99, 19,99 EUR, etc.
    """
    if not price_str:
        return None

    # Remove currency symbols and whitespace
    cleaned = re.sub(r'[^\d.,]', '', price_str)

    # Handle European format (comma as decimal)
    if ',' in cleaned and '.' not in cleaned:
        cleaned = cleaned.replace(',', '.')
    elif ',' in cleaned and '.' in cleaned:
        # Format like 1,234.56
        cleaned = cleaned.replace(',', '')

    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_rating(rating_str: Optional[str]) -> Optional[float]:
    """Extract numeric rating from string."""
    if not rating_str:
        return None

    # Find pattern like "4.5 out of 5" or "4.5/5" or just "4.5"
    match = re.search(r'(\d+\.?\d*)', rating_str)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean and normalize text content."""
    if not text:
        return None

    # Remove extra whitespace
    cleaned = ' '.join(text.split())

    # Remove common artifacts
    cleaned = cleaned.strip()

    return cleaned if cleaned else None


@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Clean and transform scraped product data.

    Operations:
    - Filter out failed scrapes
    - Clean price values to numeric
    - Normalize ratings
    - Clean text fields
    - Add computed columns
    """
    # Filter to successful scrapes only
    df = data[data['success'] == True].copy()

    if len(df) == 0:
        print("Warning: No successful scrapes to transform")
        return df

    print(f"Transforming {len(df)} products...")

    # Clean price column
    if 'price' in df.columns:
        df['price_raw'] = df['price']
        df['price'] = df['price'].apply(clean_price)
        print(f"  - Cleaned prices: {df['price'].notna().sum()} valid")

    # Clean rating column
    if 'rating' in df.columns:
        df['rating_raw'] = df['rating']
        df['rating'] = df['rating'].apply(clean_rating)
        print(f"  - Cleaned ratings: {df['rating'].notna().sum()} valid")

    # Clean text fields
    if 'title' in df.columns:
        df['title'] = df['title'].apply(clean_text)

    if 'description' in df.columns:
        df['description'] = df['description'].apply(clean_text)

    # Add computed columns
    df['has_price'] = df['price'].notna()
    df['has_rating'] = df['rating'].notna()

    # Extract domain from URL
    if 'url' in df.columns:
        df['domain'] = df['url'].str.extract(r'https?://(?:www\.)?([^/]+)')

    # Data quality score
    quality_columns = ['title', 'price', 'description', 'rating']
    available_columns = [c for c in quality_columns if c in df.columns]
    df['data_quality_score'] = df[available_columns].notna().sum(axis=1) / len(available_columns)

    print(f"  - Average data quality: {df['data_quality_score'].mean():.2%}")

    # Remove raw HTML length column if present (save space)
    if 'raw_html_length' in df.columns:
        df = df.drop(columns=['raw_html_length'])

    return df


@test
def test_output(output, *args) -> None:
    """Validate transformed data."""
    assert output is not None, 'Output is undefined'

    if len(output) > 0:
        # Check that price is numeric where present
        if 'price' in output.columns:
            numeric_prices = output['price'].dropna()
            assert numeric_prices.dtype in ['float64', 'int64'], 'Price should be numeric'

        # Check data quality score is between 0 and 1
        if 'data_quality_score' in output.columns:
            assert output['data_quality_score'].between(0, 1).all(), 'Quality score out of range'
