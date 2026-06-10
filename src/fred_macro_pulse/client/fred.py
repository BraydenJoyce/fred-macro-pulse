import asyncio

import httpx

from ..config.settings import get_settings
from .models import Observation, ObservationsResponse, SeriesMetadata


class AsyncFREDClient:
    def __init__(self) -> None:
        s = get_settings()
        self._semaphore = asyncio.Semaphore(s.max_concurrent_requests)
        self._client = httpx.AsyncClient(timeout=30.0)
        self._settings = s

    async def get_observations(
        self,
        series_id: str,
        observation_start: str | None = None,
    ) -> ObservationsResponse:
        params: dict = {
            "series_id": series_id,
            "api_key": self._settings.fred_api_key,
            "file_type": "json",
        }
        if observation_start:
            params["observation_start"] = observation_start

        async with self._semaphore:
            resp = await self._client.get(
                f"{self._settings.fred_base_url}/series/observations",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()

        return ObservationsResponse(
            series_id=series_id,
            observations=[Observation(**o) for o in data["observations"]],
        )

    async def get_series_metadata(self, series_id: str) -> SeriesMetadata:
        async with self._semaphore:
            resp = await self._client.get(
                f"{self._settings.fred_base_url}/series",
                params={
                    "series_id": series_id,
                    "api_key": self._settings.fred_api_key,
                    "file_type": "json",
                },
            )
            resp.raise_for_status()
            data = resp.json()["seriess"][0]

        return SeriesMetadata(
            id=data["id"],
            title=data["title"],
            units=data["units"],
            frequency=data["frequency"],
            seasonal_adjustment=data["seasonal_adjustment"],
            notes=data.get("notes"),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncFREDClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
