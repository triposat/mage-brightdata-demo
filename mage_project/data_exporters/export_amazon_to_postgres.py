"""
Export Amazon product data to PostgreSQL for historical tracking.
"""

import os
import pandas as pd
from sqlalchemy import create_engine

if 'data_exporter' not in dir():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs) -> None:
    """
    Export Amazon products to PostgreSQL.

    Creates a historical record of products for price tracking and analysis.
    """
    if len(data) == 0:
        print("No data to export")
        return

    # Database connection
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'mage')
    password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
    database = os.getenv('POSTGRES_DB', 'scraped_data')

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    # Select columns to export (avoid complex nested objects)
    export_columns = [
        'title', 'brand', 'asin', 'url',
        'initial_price', 'final_price', 'best_price', 'currency',
        'discount_percent', 'price_tier',
        'rating', 'rating_category', 'reviews_count', 'popularity',
        'availability', 'main_category', 'search_keyword',
        'seller_name', 'image_url', 'bought_past_month'
    ]

    # Filter to available columns
    available = [c for c in export_columns if c in data.columns]
    df_export = data[available].copy()

    # Add timestamp
    df_export['scraped_at'] = pd.Timestamp.now()

    # Export to PostgreSQL
    table_name = kwargs.get('table_name', 'amazon_products')

    try:
        df_export.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',  # Append for historical tracking
            index=False,
            method='multi',
            chunksize=500
        )
        print(f"Exported {len(df_export)} products to '{table_name}'")

    except Exception as e:
        print(f"Error exporting to PostgreSQL: {e}")
        raise

    finally:
        engine.dispose()


@test
def test_output(*args, **kwargs) -> None:
    """Data exporters don't return output, so no validation needed."""
    pass
