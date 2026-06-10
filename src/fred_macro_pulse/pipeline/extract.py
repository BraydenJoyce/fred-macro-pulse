import asyncio
import logging
from datetime import UTC, datetime

import duckdb

from ..client.fred import AsyncFREDClient
from ..client.models import ObservationsResponse

logger = logging.getLogger(__name__)


def get_watermarks(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return {series_id: last_observation_date_str} for all loaded series."""
    result = conn.execute(
        "SELECT series_id, MAX(observation_date)::VARCHAR FROM fact_observations GROUP BY series_id"
    ).fetchall()
    return {row[0]: row[1] for row in result}


async def extract_all(
    series_ids: list[str],
    conn: duckdb.DuckDBPyConnection,
    backfill: bool = False,
) -> tuple[list[ObservationsResponse], list[dict]]:
    """Fetch observations and metadata for all series. Returns (observations, metadata_records)."""
    watermarks = {} if backfill else get_watermarks(conn)

    async with AsyncFREDClient() as client:
        obs_tasks = [
            client.get_observations(sid, observation_start=watermarks.get(sid))
            for sid in series_ids
        ]
        meta_tasks = [client.get_series_metadata(sid) for sid in series_ids]

        obs_results, meta_results = await asyncio.gather(
            asyncio.gather(*obs_tasks, return_exceptions=True),
            asyncio.gather(*meta_tasks, return_exceptions=True),
        )

    observations: list[ObservationsResponse] = []
    for sid, result in zip(series_ids, obs_results):
        if isinstance(result, Exception):
            logger.error("Failed to fetch observations for %s: %s", sid, result)
        else:
            observations.append(result)

    now = datetime.now(UTC)
    metadata: list[dict] = []
    for sid, result in zip(series_ids, meta_results):
        if isinstance(result, Exception):
            logger.warning("Failed to fetch metadata for %s: %s", sid, result)
        else:
            metadata.append(
                {
                    "series_id": result.id,
                    "title": result.title,
                    "units": result.units,
                    "frequency": result.frequency,
                    "seasonal_adjustment": result.seasonal_adjustment,
                    "category": None,  # enriched from YAML catalog in CLI
                    "notes": result.notes,
                    "last_updated": now,
                }
            )

    return observations, metadata
