import httpx
import pytest

from fred_macro_pulse.client.fred import AsyncFREDClient
from fred_macro_pulse.client.models import ObservationsResponse, SeriesMetadata

MOCK_OBSERVATIONS = {
    "observations": [
        {"date": "2024-01-01", "value": "3.7"},
        {"date": "2024-02-01", "value": "."},
        {"date": "2024-03-01", "value": "3.8"},
    ]
}

MOCK_SERIES = {
    "seriess": [{
        "id": "UNRATE",
        "title": "Unemployment Rate",
        "units": "Percent",
        "frequency": "Monthly",
        "seasonal_adjustment": "Seasonally Adjusted",
        "notes": None,
    }]
}


class _MockTransport(httpx.AsyncBaseTransport):
    def __init__(self, json_data: dict) -> None:
        self._json = json_data

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=self._json)


@pytest.mark.asyncio
async def test_get_observations_returns_response():
    client = AsyncFREDClient()
    client._client = httpx.AsyncClient(transport=_MockTransport(MOCK_OBSERVATIONS))
    result = await client.get_observations("UNRATE")
    await client.close()

    assert isinstance(result, ObservationsResponse)
    assert result.series_id == "UNRATE"
    assert len(result.observations) == 3
    assert result.observations[1].value == "."


@pytest.mark.asyncio
async def test_get_observations_filters_nothing_in_client():
    """Client returns raw observations; filtering happens in transform layer."""
    client = AsyncFREDClient()
    client._client = httpx.AsyncClient(transport=_MockTransport(MOCK_OBSERVATIONS))
    result = await client.get_observations("UNRATE", observation_start="2024-01-01")
    await client.close()

    assert len(result.observations) == 3


@pytest.mark.asyncio
async def test_get_series_metadata():
    client = AsyncFREDClient()
    client._client = httpx.AsyncClient(transport=_MockTransport(MOCK_SERIES))
    result = await client.get_series_metadata("UNRATE")
    await client.close()

    assert isinstance(result, SeriesMetadata)
    assert result.id == "UNRATE"
    assert result.frequency == "Monthly"
