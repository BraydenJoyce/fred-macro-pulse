from datetime import date

import polars as pl
import pytest

from fred_macro_pulse.client.models import Observation, ObservationsResponse
from fred_macro_pulse.pipeline.transform import to_dataframe


def make_response(series_id: str, observations: list[tuple[str, str]]) -> ObservationsResponse:
    return ObservationsResponse(
        series_id=series_id,
        observations=[Observation(date=d, value=v) for d, v in observations],
    )


def test_missing_values_become_null():
    resp = make_response(
        "UNRATE",
        [
            ("2024-01-01", "."),
            ("2024-02-01", "3.7"),
        ],
    )
    df = to_dataframe([resp])
    assert len(df) == 1
    assert df["value"][0] == pytest.approx(3.7)


def test_value_cast_to_float():
    resp = make_response("FEDFUNDS", [("2024-03-01", "5.33")])
    df = to_dataframe([resp])
    assert df["value"].dtype == pl.Float64


def test_empty_responses_returns_empty_frame():
    df = to_dataframe([])
    assert len(df) == 0
    assert "series_id" in df.columns
    assert "value" in df.columns


def test_multiple_series_combined():
    resp1 = make_response("UNRATE", [("2024-01-01", "3.7"), ("2024-02-01", ".")])
    resp2 = make_response("FEDFUNDS", [("2024-01-01", "5.33")])
    df = to_dataframe([resp1, resp2])
    assert set(df["series_id"].to_list()) == {"UNRATE", "FEDFUNDS"}
    assert len(df) == 2


def test_observation_date_is_date_type():
    resp = make_response("GDPC1", [("2024-01-01", "22000.0")])
    df = to_dataframe([resp])
    assert df["observation_date"].dtype == pl.Date
    assert df["observation_date"][0] == date(2024, 1, 1)
