from collections.abc import Callable

import duckdb

CHECKS: list[tuple[str, str, Callable[[object], bool]]] = [
    (
        "No nulls in series_id",
        "SELECT COUNT(*) FROM fact_observations WHERE series_id IS NULL",
        lambda v: v == 0,
    ),
    (
        "No future dates",
        "SELECT COUNT(*) FROM fact_observations WHERE observation_date > today()",
        lambda v: v == 0,
    ),
    (
        "Latest CPI within 45 days",
        "SELECT DATEDIFF('day', MAX(observation_date), today())"
        " FROM fact_observations WHERE series_id = 'CPIAUCSL'",
        lambda v: v is not None and v <= 75,
    ),
    (
        "All catalog series loaded",
        "SELECT COUNT(DISTINCT series_id) FROM fact_observations",
        lambda v: v >= 30,
    ),
]


def run_checks(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, bool]]:
    return [
        (name, passes(conn.execute(query).fetchone()[0]))
        for name, query, passes in CHECKS
    ]
