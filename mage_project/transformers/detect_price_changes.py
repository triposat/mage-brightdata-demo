"""
Detect Price Changes: Compare current scrape with historical data.

This transformer compares newly scraped prices against the last known prices
in PostgreSQL and flags significant changes for alerting.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

if 'transformer' not in dir():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


def get_db_connection():
    """Create database connection."""
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'mage')
    password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
    database = os.getenv('POSTGRES_DB', 'scraped_data')

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)


def get_last_prices(engine, asins: list) -> dict:
    """
    Get the most recent price for each ASIN from PostgreSQL.

    Returns dict: {asin: last_price}
    """
    if not asins:
        return {}

    try:
        # Get the most recent price for each ASIN
        query = text("""
            SELECT DISTINCT ON (asin)
                asin,
                best_price,
                scraped_at
            FROM amazon_products
            WHERE asin = ANY(:asins)
            ORDER BY asin, scraped_at DESC
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {"asins": asins})
            rows = result.fetchall()

        return {row[0]: row[1] for row in rows}

    except Exception as e:
        print(f"Warning: Could not fetch historical prices: {e}")
        return {}


@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Detect price changes by comparing with historical data.

    Adds columns:
    - previous_price: Last known price from database
    - price_change: Absolute price difference
    - price_change_pct: Percentage change
    - price_alert: Flag for significant changes (>10% by default)

    Pipeline variables:
    - price_change_threshold: Percentage threshold for alerts (default: 10)
    """
    if len(data) == 0:
        print("No data to analyze")
        return data

    df = data.copy()
    threshold = kwargs.get('price_change_threshold', 10)  # 10% default

    print(f"Analyzing price changes (threshold: {threshold}%)...")
    print("-" * 50)

    # Initialize new columns
    df['previous_price'] = None
    df['price_change'] = 0.0
    df['price_change_pct'] = 0.0
    df['price_alert'] = False
    df['alert_type'] = None

    # Get ASINs that we have
    if 'asin' not in df.columns or 'best_price' not in df.columns:
        print("Missing required columns (asin, best_price). Skipping price change detection.")
        return df

    asins = df['asin'].dropna().unique().tolist()

    if not asins:
        print("No ASINs found. Skipping price change detection.")
        return df

    # Fetch historical prices
    try:
        engine = get_db_connection()
        last_prices = get_last_prices(engine, asins)
        engine.dispose()

        print(f"Found historical prices for {len(last_prices)} of {len(asins)} products")

    except Exception as e:
        print(f"Could not connect to database: {e}")
        print("Skipping price change detection (no historical data)")
        return df

    if not last_prices:
        print("No historical price data found. This might be the first run.")
        return df

    # Calculate price changes
    price_drops = 0
    price_increases = 0
    alerts = []

    for idx, row in df.iterrows():
        asin = row['asin']
        current_price = row['best_price']

        if asin in last_prices and pd.notna(current_price):
            previous_price = last_prices[asin]

            if previous_price and previous_price > 0:
                df.at[idx, 'previous_price'] = previous_price

                change = current_price - previous_price
                change_pct = (change / previous_price) * 100

                df.at[idx, 'price_change'] = round(change, 2)
                df.at[idx, 'price_change_pct'] = round(change_pct, 1)

                # Check for significant changes
                if abs(change_pct) >= threshold:
                    df.at[idx, 'price_alert'] = True

                    if change_pct < 0:
                        df.at[idx, 'alert_type'] = 'PRICE_DROP'
                        price_drops += 1
                        alerts.append({
                            'title': row.get('title', 'Unknown')[:40],
                            'asin': asin,
                            'old_price': previous_price,
                            'new_price': current_price,
                            'change_pct': change_pct
                        })
                    else:
                        df.at[idx, 'alert_type'] = 'PRICE_INCREASE'
                        price_increases += 1

    # Summary
    print("-" * 50)
    print("Price Change Summary:")
    print(f"  Products with history: {len(last_prices)}")
    print(f"  Price drops (>={threshold}%): {price_drops}")
    print(f"  Price increases (>={threshold}%): {price_increases}")

    if alerts:
        print(f"\nTop Price Drops:")
        sorted_alerts = sorted(alerts, key=lambda x: x['change_pct'])[:5]
        for alert in sorted_alerts:
            print(f"  {alert['title']}...")
            print(f"    ${alert['old_price']:.2f} → ${alert['new_price']:.2f} ({alert['change_pct']:.1f}%)")

    # Store alerts summary in kwargs for callback
    df.attrs['price_alerts'] = {
        'total_alerts': price_drops + price_increases,
        'price_drops': price_drops,
        'price_increases': price_increases,
        'top_alerts': alerts[:10] if alerts else []
    }

    return df


@test
def test_output(output, *args) -> None:
    """Validate price change detection."""
    assert output is not None, 'Output is undefined'
    assert 'price_change_pct' in output.columns, 'Price change column missing'
    assert 'price_alert' in output.columns, 'Price alert column missing'
