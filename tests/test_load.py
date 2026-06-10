from datetime import UTC, date, datetime

import polars as pl
import pytest

from fred_macro_pulse.client.models import Observation, ObservationsResponse
from fred_macro_pulse.pipeline.load import load_raw_observations, load_series_metadata, upsert_facts


def _df(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        {"series_id": [r[0] for r in rows],
         "observation_date": [r[1] for r in rows],
         "value": [r[2] for r in rows]},
    ).with_columns(pl.col("observation_date").cast(pl.Date))


def test_upsert_inserts_new_rows(tmp_db):
    df = _df([("UNRATE", date(2024, 1, 1), 3.7), ("UNRATE", date(2024, 2, 1), 3.8)])
    upsert_facts(tmp_db, df)
    count = tmp_db.execute("SELECT COUNT(*) FROM fact_observations").fetchone()[0]
    assert count == 2


def test_upsert_detects_revision(tmp_db):
    upsert_facts(tmp_db, _df([("UNRATE", date(2024, 1, 1), 3.7)]))
    upsert_facts(tmp_db, _df([("UNRATE", date(2024, 1, 1), 3.8)]))  # revised value

    row = tmp_db.execute(
        "SELECT value, is_revised FROM fact_observations WHERE series_id = 'UNRATE'"
    ).fetchone()
    assert row[0] == pytest.approx(3.8)
    assert row[1] is True


def test_upsert_no_revision_flag_when_value_unchanged(tmp_db):
    upsert_facts(tmp_db, _df([("FEDFUNDS", date(2024, 3, 1), 5.33)]))
    upsert_facts(tmp_db, _df([("FEDFUNDS", date(2024, 3, 1), 5.33)]))  # same value

    row = tmp_db.execute(
        "SELECT is_revised FROM fact_observations WHERE series_id = 'FEDFUNDS'"
    ).fetchone()
    assert row[0] is False


def test_load_raw_observations_appends(tmp_db):
    responses = [
        ObservationsResponse(
            series_id="FEDFUNDS",
            observations=[
                Observation(date=date(2024, 3, 1), value="5.33"),
                Observation(date=date(2024, 4, 1), value="."),
            ],
        )
    ]
    ts = datetime.now(UTC)
    n = load_raw_observations(tmp_db, responses, run_id="test-001", vintage_date=ts)
    assert n == 2  # raw layer keeps "." rows
    count = tmp_db.execute("SELECT COUNT(*) FROM raw_observations").fetchone()[0]
    assert count == 2


def test_load_series_metadata_upserts(tmp_db):
    records = [{
        "series_id": "UNRATE",
        "title": "Unemployment Rate",
        "units": "Percent",
        "frequency": "Monthly",
        "seasonal_adjustment": "Seasonally Adjusted",
        "category": "Labor",
        "notes": None,
        "last_updated": datetime.now(UTC),
    }]
    load_series_metadata(tmp_db, records)
    row = tmp_db.execute(
        "SELECT title, category FROM dim_series WHERE series_id = 'UNRATE'"
    ).fetchone()
    assert row[0] == "Unemployment Rate"
    assert row[1] == "Labor"
