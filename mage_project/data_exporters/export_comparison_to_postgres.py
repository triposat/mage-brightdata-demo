"""
Export multi-platform comparison to PostgreSQL.
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
    """Export platform comparison to PostgreSQL."""
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

    # Select columns to export
    export_columns = [
        'title', 'brand', 'source_platform', 'url',
        'final_price', 'price_tier', 'price_vs_avg', 'is_best_deal',
        'rating', 'reviews_count', 'search_keyword'
    ]

    available = [c for c in export_columns if c in data.columns]
    df_export = data[available].copy()
    df_export['scraped_at'] = pd.Timestamp.now()

    table_name = kwargs.get('table_name', 'platform_comparison')

    try:
        df_export.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
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
    pass
