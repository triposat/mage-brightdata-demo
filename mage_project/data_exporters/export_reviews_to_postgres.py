"""
Export Amazon reviews to PostgreSQL.
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
    """Export reviews to PostgreSQL."""
    if len(data) == 0:
        print("No reviews to export")
        return

    # Database connection
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'mage')
    password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
    database = os.getenv('POSTGRES_DB', 'scraped_data')

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    # Select columns to export
    export_columns = [
        'asin', 'product_asin', 'title', 'rating', 'review_text', 'text',
        'date', 'review_date', 'verified_purchase', 'helpful_count',
        'sentiment', 'negative_keywords', 'is_recent'
    ]

    available = [c for c in export_columns if c in data.columns]
    df_export = data[available].copy()
    df_export['scraped_at'] = pd.Timestamp.now()

    table_name = kwargs.get('table_name', 'amazon_reviews')

    try:
        df_export.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500
        )
        print(f"Exported {len(df_export)} reviews to '{table_name}'")

    except Exception as e:
        print(f"Error exporting to PostgreSQL: {e}")
        raise

    finally:
        engine.dispose()


@test
def test_output(*args, **kwargs) -> None:
    pass
