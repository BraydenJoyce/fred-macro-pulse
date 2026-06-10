from pathlib import Path

import duckdb

_VIEW_ORDER = [
    "v_latest_values.sql",
    "v_rolling_avg.sql",
    "v_yield_curve.sql",
    "v_yoy_change.sql",
    "v_recession_signals.sql",
    "v_macro_composite.sql",  # must be last — depends on v_latest_values and v_yoy_change
]


def bootstrap(conn: duckdb.DuckDBPyConnection) -> None:
    """Run all migrations then deploy views in dependency order."""
    base = Path(__file__).parent
    for migration in sorted((base / "migrations").glob("*.sql")):
        conn.execute(migration.read_text())
    views_dir = base / "views"
    for name in _VIEW_ORDER:
        conn.execute((views_dir / name).read_text())


def get_connection() -> duckdb.DuckDBPyConnection:
    from ..config.settings import get_settings
    return duckdb.connect(get_settings().db_path)
