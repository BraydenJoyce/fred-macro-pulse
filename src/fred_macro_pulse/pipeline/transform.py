import polars as pl

from ..client.models import ObservationsResponse


def to_dataframe(responses: list[ObservationsResponse]) -> pl.DataFrame:
    """Convert API responses to a cleaned Polars DataFrame."""
    if not responses:
        return pl.DataFrame(schema={
            "series_id": pl.Utf8,
            "observation_date": pl.Date,
            "value": pl.Float64,
        })

    records = [
        {
            "series_id": resp.series_id,
            "observation_date": obs.date,
            "value_raw": obs.value,
        }
        for resp in responses
        for obs in resp.observations
    ]

    df = pl.DataFrame(records)

    return (
        df.with_columns(
            # strict=False turns any non-numeric value (including FRED's "." sentinel) into null
            pl.col("value_raw").cast(pl.Float64, strict=False).alias("value"),
        )
        .drop("value_raw")
        .filter(pl.col("value").is_not_null())
    )
