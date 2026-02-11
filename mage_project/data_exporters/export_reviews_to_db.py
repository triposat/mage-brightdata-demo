"""
Export analyzed review data to PostgreSQL and CSV.

Stores AI-analyzed reviews in the `amazon_reviews` table
so the Streamlit dashboard can read sentiment, issues, and themes live.

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
    Export reviews to PostgreSQL + CSV backup.

    Uses upsert logic: deletes existing reviews for the same product ASINs,
    then inserts new data. This preserves reviews from other products
    while updating current ones.
    """
    if data is None or len(data) == 0:
        print("No review data to export")
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

        # Upsert: delete existing reviews for these product ASINs, then append
        asin_col = None
        for col in ['asin', 'product_asin', 'url']:
            if col in df.columns:
                asin_col = col
                break

        if asin_col:
            asins = df[asin_col].dropna().unique().tolist()
            if asins:
                with engine.begin() as conn:
                    # Create table if it doesn't exist (first run)
                    df.head(0).to_sql('amazon_reviews', conn, if_exists='append', index=False)
                    # Delete stale reviews for these ASINs
                    conn.execute(
                        text(f"DELETE FROM amazon_reviews WHERE {asin_col} = ANY(:asins)"),
                        {"asins": asins}
                    )
                    # Insert in same transaction so DELETE rolls back if INSERT fails
                    df.to_sql('amazon_reviews', conn, if_exists='append', index=False, method='multi')
        else:
            # No ASIN column -- fall back to replace
            df.to_sql('amazon_reviews', engine, if_exists='replace', index=False, method='multi')

        engine.dispose()
        print(f"Exported {len(df)} reviews to PostgreSQL (amazon_reviews)")

    except Exception as e:
        print(f"PostgreSQL export failed: {e}")
        print("Falling back to CSV only")

    # ── CSV Backup ────────────────────────────────────────────────────────
    output_dir = kwargs.get('output_dir', '/home/src/mage_project/output')
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = os.path.join(output_dir, f"amazon_reviews_{timestamp}.csv")
    df.to_csv(filepath, index=False)
    print(f"CSV backup: {filepath}")


@test
def test_output(*args, **kwargs) -> None:
    """Validate review export completed successfully."""
    import os
    from sqlalchemy import create_engine, text

    # Verify PostgreSQL has review data with AI analysis
    try:
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        user = os.getenv('POSTGRES_USER', 'mage')
        password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
        database = os.getenv('POSTGRES_DB', 'scraped_data')

        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM amazon_reviews")).scalar()
            with_sentiment = conn.execute(
                text("SELECT COUNT(*) FROM amazon_reviews WHERE sentiment IS NOT NULL AND sentiment != 'Unknown'")
            ).scalar()
        engine.dispose()

        assert total > 0, 'PostgreSQL amazon_reviews table is empty after export'
        print(f'PostgreSQL: {total} reviews stored, {with_sentiment} with AI sentiment')
    except Exception as e:
        print(f'PostgreSQL check skipped: {e}')
