from datetime import UTC, datetime

import duckdb
import polars as pl

from ..client.models import ObservationsResponse


def load_raw_observations(
    conn: duckdb.DuckDBPyConnection,
    responses: list[ObservationsResponse],
    run_id: str,
    vintage_date: datetime | None = None,
) -> int:
    """Append raw API observations (including '.' missing markers) to the raw layer."""
    if not responses:
        return 0

    ts = (vintage_date or datetime.now(UTC)).replace(tzinfo=None)
    records = [
        {
            "series_id": resp.series_id,
            "observation_date": obs.date,
            "value": obs.value,
            "vintage_date": ts,
            "run_id": run_id,
        }
        for resp in responses
        for obs in resp.observations
    ]
    # DuckDB replacement scan references this local variable by name in the SQL above.
    df_raw = pl.DataFrame(records).with_columns(  # noqa: F841
        pl.col("observation_date").cast(pl.Date)
    )
    conn.execute("INSERT INTO raw_observations SELECT * FROM df_raw")
    return len(records)


def upsert_facts(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> None:
    """Upsert cleaned observations into fact_observations, flagging revisions."""
    if df.is_empty():
        return
    # DuckDB replacement scan: references the local `df` parameter directly
    conn.execute("""
        INSERT INTO fact_observations (series_id, observation_date, value, is_revised, loaded_at)
        SELECT
            src.series_id,
            src.observation_date,
            src.value,
            FALSE,
            now()
        FROM df AS src
        ON CONFLICT (series_id, observation_date)
        DO UPDATE SET
            is_revised = (excluded.value IS DISTINCT FROM fact_observations.value)
                         OR fact_observations.is_revised,
            value      = excluded.value,
            loaded_at  = now()
    """)


def load_series_metadata(
    conn: duckdb.DuckDBPyConnection,
    metadata_records: list[dict],
) -> None:
    """Upsert series metadata into dim_series."""
    if not metadata_records:
        return
    # Timestamps must be timezone-naive for DuckDB TIMESTAMP columns
    for r in metadata_records:
        if isinstance(r.get("last_updated"), datetime) and r["last_updated"].tzinfo is not None:
            r["last_updated"] = r["last_updated"].replace(tzinfo=None)
    # DuckDB replacement scan references this local variable by name in the SQL below.
    df_meta = pl.DataFrame(metadata_records)  # noqa: F841
    conn.execute("""
        INSERT INTO dim_series
        SELECT * FROM df_meta
        ON CONFLICT (series_id) DO UPDATE SET
            title               = excluded.title,
            units               = excluded.units,
            frequency           = excluded.frequency,
            seasonal_adjustment = excluded.seasonal_adjustment,
            category            = COALESCE(excluded.category, dim_series.category),
            notes               = excluded.notes,
            last_updated        = excluded.last_updated
    """)
