import os

# Set before any fred_macro_pulse import so get_settings() doesn't fail in tests.
os.environ.setdefault("FRED_API_KEY", "test_api_key")

import duckdb
import pytest

from fred_macro_pulse.warehouse.schema import bootstrap


@pytest.fixture
def tmp_db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    bootstrap(conn)
    yield conn
    conn.close()
