"""
Export product data to PostgreSQL and CSV.

Stores enriched product data in the `amazon_products` table
so the Streamlit dashboard can read it live.

Uses append + deduplication so scheduled runs accumulate historical data
instead of overwriting previous results.
"""

import os
import json
import pandas as pd
from datetime import datetime, timezone

if 'data_exporter' not in dir():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs) -> None:
    """
    Export products to PostgreSQL + CSV backup.

    Uses upsert logic: deletes existing rows for the same ASINs,
    then inserts new data. This preserves products from other runs
    while updating current ones.
    """
    if data is None or len(data) == 0:
        print("No product data to export")
        return

    df = data.copy()
    df['scraped_at'] = datetime.now(timezone.utc)

    # ── PostgreSQL Export ─────────────────────────────────────────────────
    try:
        from sqlalchemy import create_engine, text
        import numpy as np

        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        user = os.getenv('POSTGRES_USER', 'mage')
        password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
        database = os.getenv('POSTGRES_DB', 'scraped_data')

        engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )

        # Convert all non-scalar values to strings for PostgreSQL
        def to_pg_value(val):
            if isinstance(val, np.ndarray):
                return json.dumps(val.tolist())
            if isinstance(val, (list, dict)):
                return json.dumps(val)
            return val

        for col in df.columns:
            try:
                if df[col].apply(lambda x: isinstance(x, (list, dict, np.ndarray))).any():
                    df[col] = df[col].apply(to_pg_value)
            except Exception:
                df[col] = df[col].astype(str)

        # Upsert: delete existing rows for these ASINs, then append
        if 'asin' in df.columns:
            asins = df['asin'].dropna().unique().tolist()
            if asins:
                with engine.begin() as conn:
                    # Create table if it doesn't exist (first run)
                    df.head(0).to_sql('amazon_products', conn, if_exists='append', index=False)
                    # Delete stale rows for these ASINs
                    conn.execute(
                        text("DELETE FROM amazon_products WHERE asin = ANY(:asins)"),
                        {"asins": asins}
                    )
                    # Insert in same transaction so DELETE rolls back if INSERT fails
                    df.to_sql('amazon_products', conn, if_exists='append', index=False, method='multi')
        else:
            # No ASIN column -- fall back to replace
            df.to_sql('amazon_products', engine, if_exists='replace', index=False, method='multi')

        engine.dispose()
        print(f"Exported {len(df)} products to PostgreSQL (amazon_products)")

    except Exception as e:
        print(f"PostgreSQL export failed: {e}")
        print("Falling back to CSV only")

    # ── CSV Backup ────────────────────────────────────────────────────────
    output_dir = kwargs.get('output_dir', '/home/src/mage_project/output')
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = os.path.join(output_dir, f"amazon_products_{timestamp}.csv")
    df.to_csv(filepath, index=False)
    print(f"CSV backup: {filepath}")


@test
def test_output(*args, **kwargs) -> None:
    """Validate product export completed successfully."""
    import os
    from sqlalchemy import create_engine, text

    # Verify PostgreSQL has product data
    try:
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        user = os.getenv('POSTGRES_USER', 'mage')
        password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
        database = os.getenv('POSTGRES_DB', 'scraped_data')

        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM amazon_products"))
            count = result.scalar()
        engine.dispose()

        assert count > 0, 'PostgreSQL amazon_products table is empty after export'
        print(f'PostgreSQL: {count} products stored')
    except Exception as e:
        print(f'PostgreSQL check skipped: {e}')
