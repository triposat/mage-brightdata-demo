import os
import pandas as pd
from sqlalchemy import create_engine

if 'data_exporter' not in dir():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in dir():
    from mage_ai.data_preparation.decorators import test


def get_connection_string() -> str:
    """Build PostgreSQL connection string from environment variables."""
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'mage')
    password = os.getenv('POSTGRES_PASSWORD', 'mage_password')
    database = os.getenv('POSTGRES_DB', 'scraped_data')

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs) -> None:
    """
    Export scraped product data to PostgreSQL.

    Table name can be configured via pipeline variables:
        table_name: Name of the target table (default: 'products')

    The table will be created if it doesn't exist.
    """
    if len(data) == 0:
        print("No data to export")
        return

    table_name = kwargs.get('table_name', 'products')
    if_exists = kwargs.get('if_exists', 'append')  # 'replace', 'append', or 'fail'

    connection_string = get_connection_string()
    engine = create_engine(connection_string)

    try:
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method='multi',  # Batch inserts for better performance
            chunksize=1000
        )

        print(f"Exported {len(data)} rows to table '{table_name}'")
        print(f"  - Mode: {if_exists}")

    except Exception as e:
        print(f"Error exporting to PostgreSQL: {e}")
        raise

    finally:
        engine.dispose()


@test
def test_output(output, *args) -> None:
    """Test runs after export completes."""
    # Export function returns None, so we just verify it ran without errors
    pass
