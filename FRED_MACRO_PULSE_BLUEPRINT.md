# FRED Macro Pulse
### A Production-Grade Incremental ETL Pipeline for Federal Reserve Economic Data

---

## Overview

FRED Macro Pulse ingests economic time series from the St. Louis Federal Reserve (FRED) API,
models them in a local DuckDB analytical warehouse, and surfaces analytics-ready SQL views
covering growth, inflation, labor, housing, and recession signals.

**Data source:** [api.stlouisfed.org](https://fred.stlouisfed.org/docs/api/fred/) (free API key required)  
**Rate limit:** 120 requests per minute  
**Target series:** ~35 curated macroeconomic indicators  
**Refresh cadence:** Weekly via GitHub Actions (free tier)
**DB distribution:** Uploaded as a GitHub Actions artifact after each run (30-day retention)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        FRED PUBLIC API                          │
│          api.stlouisfed.org/fred/series/observations            │
└────────────────────────────┬────────────────────────────────────┘
                             │ async httpx (batch, rate-limited)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      EXTRACTION LAYER                           │
│  AsyncFREDClient  ──►  Pydantic Response Models                 │
│  Semaphore-gated batching (respects 120 req/min)                │
│  Incremental: only fetches observations newer than watermark    │
└────────────────────────────┬────────────────────────────────────┘
                             │ List[Observation]
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER                         │
│  Polars DataFrames                                              │
│  • Cast "." missing values to null                              │
│  • Parse date strings to date type                              │
│  • Attach series metadata (units, frequency, seasonal adj.)     │
│  • Stamp vintage date (when data was pulled)                    │
└────────────────────────────┬────────────────────────────────────┘
                             │ Polars DataFrame
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DUCKDB WAREHOUSE                         │
│                                                                 │
│  ┌──────────────────┐     ┌───────────────────────────────┐    │
│  │   raw layer      │     │     dimensional model          │    │
│  │                  │     │                                │    │
│  │ raw_observations │────►│ dim_series                     │    │
│  │ (append only,    │     │ fact_observations               │    │
│  │  full history)   │     │ (upsert, revision-aware)       │    │
│  └──────────────────┘     └───────────────┬───────────────┘    │
│                                           │                     │
│                           ┌───────────────▼───────────────┐    │
│                           │       analytics views          │    │
│                           │                                │    │
│                           │ v_yoy_change                   │    │
│                           │ v_rolling_avg                  │    │
│                           │ v_recession_signals            │    │
│                           │ v_latest_values                │    │
│                           │ v_yield_curve                  │    │
│                           │ v_macro_composite              │    │
│                           └───────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   INTERFACES (optional)                         │
│  Typer CLI  ──  DuckDB SQL shell  ──  Streamlit dashboard       │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION (free)                           │
│  GitHub Actions: weekly cron on Monday 6AM ET                   │
│  Uploads updated fred_macro.duckdb as Actions artifact          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Language | Python 3.11+ | Type hints, async support, ecosystem |
| HTTP client | async httpx | Native async, connection pooling |
| Data transformation | Polars | Fast columnar ops, LazyFrame API |
| Analytical warehouse | DuckDB | Embedded, SQL-native, zero infra |
| Config and validation | Pydantic v2 | Settings, API response models |
| CLI | Typer | Clean, typed argument parsing |
| Testing | pytest + pytest-asyncio | Async test support |
| Linting and formatting | Ruff | Fast, single tool |
| Package management | uv | Modern, fast, lockfile support |
| Scheduling | GitHub Actions | Free cron jobs |
| Dashboard (optional) | Streamlit | Free hosting on Streamlit Cloud |

---

## Directory Structure

```
fred-macro-pulse/
│
├── .github/
│   └── workflows/
│       ├── ci.yml                  # Lint + test on every PR
│       └── pipeline.yml            # Weekly scheduled pipeline run
│
├── src/
│   └── fred_macro_pulse/
│       ├── __init__.py
│       │
│       ├── client/
│       │   ├── __init__.py
│       │   ├── fred.py             # AsyncFREDClient class
│       │   └── models.py           # Pydantic models for API responses
│       │
│       ├── config/
│       │   ├── __init__.py
│       │   ├── settings.py         # Pydantic BaseSettings (env vars)
│       │   └── series.yaml         # Curated series catalog
│       │
│       ├── pipeline/
│       │   ├── __init__.py
│       │   ├── extract.py          # Orchestrates async fetch per series
│       │   ├── transform.py        # Polars cleaning and shaping
│       │   └── load.py             # DuckDB upsert logic
│       │
│       ├── warehouse/
│       │   ├── __init__.py
│       │   ├── schema.py           # Python DDL runner (creates tables/views)
│       │   ├── migrations/
│       │   │   ├── 001_raw.sql
│       │   │   ├── 002_dimensional.sql
│       │   │   └── 003_views.sql
│       │   └── views/
│       │       ├── v_yoy_change.sql
│       │       ├── v_rolling_avg.sql
│       │       ├── v_recession_signals.sql
│       │       ├── v_latest_values.sql
│       │       ├── v_yield_curve.sql
│       │       └── v_macro_composite.sql
│       │
│       └── cli.py                  # Typer entrypoint
│
├── tests/
│   ├── conftest.py                 # Shared fixtures (mock client, temp DuckDB)
│   ├── test_client.py              # API client unit tests
│   ├── test_transform.py           # Polars transformation tests
│   └── test_load.py                # Upsert and schema tests
│
├── dashboard/
│   └── app.py                      # Streamlit app (optional Stage 4)
│
├── data/
│   └── .gitkeep                    # fred_macro.duckdb lives here (gitignored)
│
├── pyproject.toml                  # uv project config + dependencies
├── Makefile                        # One-command dev shortcuts
├── .env.example                    # Template for secrets
├── .gitignore
└── README.md
```

---

## DuckDB Schema

### Raw Layer (append only, never mutated)

```sql
CREATE TABLE IF NOT EXISTS raw_observations (
    series_id        VARCHAR     NOT NULL,
    observation_date DATE        NOT NULL,
    value            VARCHAR,            -- raw string from API, "." for missing
    vintage_date     TIMESTAMP   NOT NULL DEFAULT now(),
    run_id           VARCHAR     NOT NULL -- UUID per pipeline run
);
```

### Dimensional Model

```sql
-- Series metadata dimension
CREATE TABLE IF NOT EXISTS dim_series (
    series_id           VARCHAR PRIMARY KEY,
    title               VARCHAR NOT NULL,
    units               VARCHAR,
    frequency           VARCHAR,           -- Monthly, Quarterly, Weekly, etc.
    seasonal_adjustment VARCHAR,           -- SA, NSA, SAAR
    category            VARCHAR,           -- Growth, Labor, Inflation, etc.
    notes               VARCHAR,
    last_updated        TIMESTAMP
);

-- Clean fact table (revision-aware upsert target)
CREATE TABLE IF NOT EXISTS fact_observations (
    series_id        VARCHAR     NOT NULL,
    observation_date DATE        NOT NULL,
    value            DOUBLE,              -- null for missing
    is_revised       BOOLEAN DEFAULT FALSE,
    loaded_at        TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, observation_date)
);

-- Pipeline run audit log
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       VARCHAR   PRIMARY KEY,
    started_at   TIMESTAMP NOT NULL DEFAULT now(),
    finished_at  TIMESTAMP,
    series_count INTEGER,
    rows_loaded  INTEGER,
    status       VARCHAR   DEFAULT 'running',   -- running | success | failed
    error_msg    VARCHAR
);
```

### Analytics Views

```sql
-- Year-over-year percent change (frequency-safe: uses date arithmetic, not LAG(12))
CREATE OR REPLACE VIEW v_yoy_change AS
SELECT
    f.series_id,
    d.title,
    d.units,
    d.frequency,
    f.observation_date,
    f.value,
    prev.value AS value_prior_year,
    ROUND(
        (f.value - prev.value) / NULLIF(ABS(prev.value), 0) * 100,
        2
    ) AS yoy_pct_change
FROM fact_observations f
JOIN dim_series d USING (series_id)
LEFT JOIN fact_observations prev
    ON  prev.series_id        = f.series_id
    AND prev.observation_date = f.observation_date - INTERVAL 1 YEAR
WHERE f.value IS NOT NULL
  AND prev.value IS NOT NULL;

-- Rolling averages (3, 6, 12 month)
CREATE OR REPLACE VIEW v_rolling_avg AS
SELECT
    series_id,
    observation_date,
    value,
    AVG(value) OVER (
        PARTITION BY series_id ORDER BY observation_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3m,
    AVG(value) OVER (
        PARTITION BY series_id ORDER BY observation_date
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_6m,
    AVG(value) OVER (
        PARTITION BY series_id ORDER BY observation_date
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_12m
FROM fact_observations
WHERE value IS NOT NULL;

-- Latest value per series (snapshot)
CREATE OR REPLACE VIEW v_latest_values AS
SELECT DISTINCT ON (series_id)
    f.series_id,
    d.title,
    d.units,
    d.frequency,
    d.category,
    f.observation_date,
    f.value
FROM fact_observations f
JOIN dim_series d USING (series_id)
WHERE f.value IS NOT NULL
ORDER BY series_id, observation_date DESC;

-- Yield curve spread — uses pre-fetched T10Y2Y series rather than recomputing DGS10-DGS2
CREATE OR REPLACE VIEW v_yield_curve AS
SELECT
    t10.observation_date,
    t10.value  AS rate_10y,
    t2.value   AS rate_2y,
    spr.value  AS spread,
    spr.value < 0 AS inverted
FROM fact_observations t10
JOIN fact_observations t2
    ON  t2.observation_date = t10.observation_date
   AND  t2.series_id        = 'DGS2'
JOIN fact_observations spr
    ON  spr.observation_date = t10.observation_date
   AND  spr.series_id        = 'T10Y2Y'
WHERE t10.series_id = 'DGS10'
ORDER BY observation_date;

-- Composite recession signal
CREATE OR REPLACE VIEW v_recession_signals AS
WITH signals AS (
    SELECT
        observation_date,
        MAX(CASE WHEN series_id = 'T10Y2Y'
            THEN CASE WHEN value < 0 THEN 1 ELSE 0 END END) AS yield_curve_inverted,
        MAX(CASE WHEN series_id = 'UNRATE'
            THEN CASE WHEN value > LAG(value, 3) OVER (
                PARTITION BY series_id ORDER BY observation_date)
            THEN 1 ELSE 0 END END) AS unemployment_rising,
        MAX(CASE WHEN series_id = 'ICSA'
            THEN CASE WHEN value > AVG(value) OVER (
                PARTITION BY series_id ORDER BY observation_date
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW) * 1.15
            THEN 1 ELSE 0 END END) AS claims_elevated
    FROM fact_observations
    WHERE series_id IN ('T10Y2Y', 'UNRATE', 'ICSA')
    GROUP BY observation_date
)
SELECT
    observation_date,
    yield_curve_inverted,
    unemployment_rising,
    claims_elevated,
    (COALESCE(yield_curve_inverted, 0)
     + COALESCE(unemployment_rising, 0)
     + COALESCE(claims_elevated, 0)) AS signal_score,
    CASE
        WHEN (COALESCE(yield_curve_inverted, 0)
              + COALESCE(unemployment_rising, 0)
              + COALESCE(claims_elevated, 0)) >= 2
        THEN 'ELEVATED'
        WHEN (COALESCE(yield_curve_inverted, 0)
              + COALESCE(unemployment_rising, 0)
              + COALESCE(claims_elevated, 0)) = 1
        THEN 'WATCH'
        ELSE 'NORMAL'
    END AS risk_level
FROM signals
ORDER BY observation_date;

-- Composite macro regime score — combines five independent signals into a single regime label
CREATE OR REPLACE VIEW v_macro_composite AS
WITH latest_cpi_yoy AS (
    SELECT yoy_pct_change
    FROM v_yoy_change
    WHERE series_id = 'CPIAUCSL' AND yoy_pct_change IS NOT NULL
    ORDER BY observation_date DESC
    LIMIT 1
),
components AS (
    SELECT 'yield_curve' AS factor,
        CASE WHEN value <  0   THEN -2 WHEN value < 0.5 THEN -1 ELSE 1 END AS score
    FROM v_latest_values WHERE series_id = 'T10Y2Y'
    UNION ALL
    SELECT 'unemployment',
        CASE WHEN value < 4.0 THEN 2 WHEN value < 5.5 THEN 0 ELSE -2 END
    FROM v_latest_values WHERE series_id = 'UNRATE'
    UNION ALL
    SELECT 'cpi_trend',
        CASE WHEN yoy_pct_change < 2.5 THEN 2 WHEN yoy_pct_change < 4.0 THEN 0 ELSE -2 END
    FROM latest_cpi_yoy
    UNION ALL
    SELECT 'leading_index',
        CASE WHEN value > 0 THEN 1 ELSE -1 END
    FROM v_latest_values WHERE series_id = 'USSLIND'
    UNION ALL
    SELECT 'recession_prob',
        CASE WHEN value < 10 THEN 1 WHEN value < 30 THEN 0 ELSE -2 END
    FROM v_latest_values WHERE series_id = 'RECPROUSM156N'
)
SELECT
    factor,
    score,
    SUM(score) OVER ()  AS composite_score,
    CASE
        WHEN SUM(score) OVER () >=  3 THEN 'EXPANSION'
        WHEN SUM(score) OVER () >= -1 THEN 'NEUTRAL'
        ELSE 'CONTRACTION'
    END AS macro_regime
FROM components;
```

---

## FRED Series Catalog (series.yaml)

```yaml
series:
  # Growth
  - id: GDPC1
    title: Real GDP (Quarterly)
    category: Growth
    frequency: Quarterly

  - id: A191RL1Q225SBEA
    title: Real GDP Growth Rate (QoQ SAAR)
    category: Growth
    frequency: Quarterly

  # Labor
  - id: UNRATE
    title: Unemployment Rate
    category: Labor
    frequency: Monthly

  - id: PAYEMS
    title: Total Nonfarm Payrolls
    category: Labor
    frequency: Monthly

  - id: ICSA
    title: Initial Jobless Claims (Weekly)
    category: Labor
    frequency: Weekly

  - id: U6RATE
    title: Underemployment Rate (U-6)
    category: Labor
    frequency: Monthly

  - id: LNS12300000
    title: Employment-Population Ratio
    category: Labor
    frequency: Monthly

  # Inflation
  - id: CPIAUCSL
    title: CPI All Items (SA)
    category: Inflation
    frequency: Monthly

  - id: CPILFESL
    title: Core CPI ex Food and Energy (SA)
    category: Inflation
    frequency: Monthly

  - id: PCEPI
    title: PCE Price Index
    category: Inflation
    frequency: Monthly

  - id: PCEPILFE
    title: Core PCE Price Index
    category: Inflation
    frequency: Monthly

  - id: PPIFIS
    title: PPI Final Demand
    category: Inflation
    frequency: Monthly

  # Interest Rates
  - id: FEDFUNDS
    title: Federal Funds Effective Rate
    category: Rates
    frequency: Monthly

  - id: DGS10
    title: 10-Year Treasury Constant Maturity
    category: Rates
    frequency: Daily

  - id: DGS2
    title: 2-Year Treasury Constant Maturity
    category: Rates
    frequency: Daily

  - id: T10Y2Y
    title: 10Y minus 2Y Treasury Spread
    category: Rates
    frequency: Daily

  - id: MORTGAGE30US
    title: 30-Year Fixed Mortgage Rate
    category: Rates
    frequency: Weekly

  # Housing
  - id: HOUST
    title: Housing Starts (Total)
    category: Housing
    frequency: Monthly

  - id: PERMIT
    title: New Private Housing Building Permits
    category: Housing
    frequency: Monthly

  - id: CSUSHPISA
    title: Case-Shiller US Home Price Index
    category: Housing
    frequency: Monthly

  - id: MSPUS
    title: Median Sales Price of Houses Sold
    category: Housing
    frequency: Quarterly

  # Consumer
  - id: RSXFS
    title: Advance Retail Sales ex Food Services
    category: Consumer
    frequency: Monthly

  - id: UMCSENT
    title: University of Michigan Consumer Sentiment
    category: Consumer
    frequency: Monthly

  - id: PSAVERT
    title: Personal Savings Rate
    category: Consumer
    frequency: Monthly

  - id: PCE
    title: Personal Consumption Expenditures
    category: Consumer
    frequency: Monthly

  # Industry and Manufacturing
  - id: INDPRO
    title: Industrial Production Index
    category: Industry
    frequency: Monthly

  - id: IPMAN
    title: Industrial Production Manufacturing
    category: Industry
    frequency: Monthly

  - id: AMTMNO
    title: Manufacturers New Orders (Total)
    category: Industry
    frequency: Monthly

  # Leading Indicators
  - id: USSLIND
    title: US Leading Index
    category: Leading
    frequency: Monthly

  - id: RECPROUSM156N
    title: Smoothed US Recession Probability
    category: Leading
    frequency: Monthly

  - id: CFNAI
    title: Chicago Fed National Activity Index
    category: Leading
    frequency: Monthly
```

---

## Stage 1: Foundation (Week 1)

**Goal:** Scaffolding, API client, and schema creation running end to end locally.

### Step 1.1: Project Setup

```bash
uv init fred-macro-pulse
cd fred-macro-pulse
uv add httpx polars duckdb pydantic pydantic-settings typer pyyaml
uv add --dev pytest pytest-asyncio ruff
```

Create `.env.example`:

```
FRED_API_KEY=your_key_here
DB_PATH=data/fred_macro.duckdb
LOG_LEVEL=INFO
```

Create `src/fred_macro_pulse/config/settings.py`:

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    fred_api_key: str
    db_path: str = "data/fred_macro.duckdb"
    log_level: str = "INFO"
    fred_base_url: str = "https://api.stlouisfed.org/fred"
    max_concurrent_requests: int = 10

    class Config:
        env_file = ".env"

settings = Settings()
```

### Step 1.2: Pydantic API Response Models

`src/fred_macro_pulse/client/models.py`:

```python
from pydantic import BaseModel
from datetime import date
from typing import Optional

class Observation(BaseModel):
    date: date
    value: str                      # raw string; "." means missing

class SeriesMetadata(BaseModel):
    id: str
    title: str
    units: str
    frequency: str
    seasonal_adjustment: str
    notes: Optional[str] = None

class ObservationsResponse(BaseModel):
    series_id: str
    observations: list[Observation]
```

### Step 1.3: Async FRED Client

`src/fred_macro_pulse/client/fred.py`:

```python
import asyncio
import httpx
from .models import Observation, SeriesMetadata, ObservationsResponse
from ..config.settings import settings

class AsyncFREDClient:
    def __init__(self):
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
        self._client = httpx.AsyncClient(timeout=30.0)

    async def get_observations(
        self,
        series_id: str,
        observation_start: str | None = None,
    ) -> ObservationsResponse:
        params = {
            "series_id": series_id,
            "api_key": settings.fred_api_key,
            "file_type": "json",
        }
        if observation_start:
            params["observation_start"] = observation_start

        async with self._semaphore:
            resp = await self._client.get(
                f"{settings.fred_base_url}/series/observations",
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
                f"{settings.fred_base_url}/series",
                params={"series_id": series_id, "api_key": settings.fred_api_key, "file_type": "json"},
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

    async def close(self):
        await self._client.aclose()
```

### Step 1.4: Schema Bootstrap

`src/fred_macro_pulse/warehouse/schema.py`:

```python
import duckdb
from pathlib import Path
from ..config.settings import settings

def bootstrap(conn: duckdb.DuckDBPyConnection) -> None:
    migration_dir = Path(__file__).parent / "migrations"
    for migration in sorted(migration_dir.glob("*.sql")):
        conn.execute(migration.read_text())

def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(settings.db_path)
```

**Deliverable check:** `uv run python -c "from fred_macro_pulse.client.fred import AsyncFREDClient; print('OK')"` passes. DuckDB tables created on first run.

---

## Stage 2: Pipeline Core (Week 2)

**Goal:** Full extract, transform, load cycle running with real data and incremental watermarks.

### Step 2.1: Watermark Logic

The incremental load pattern works like a bookmark. On each run, look up the most recent observation date already in DuckDB per series. Only request data after that date from FRED. This avoids refetching years of history on every run.

`src/fred_macro_pulse/pipeline/extract.py`:

```python
import asyncio
import logging
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
) -> list[ObservationsResponse]:
    client = AsyncFREDClient()
    watermarks = {} if backfill else get_watermarks(conn)

    tasks = [
        client.get_observations(
            series_id=sid,
            observation_start=watermarks.get(sid),
        )
        for sid in series_ids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    await client.close()

    successes = []
    for sid, result in zip(series_ids, results):
        if isinstance(result, Exception):
            logger.error("Failed to fetch %s: %s", sid, result)
        else:
            successes.append(result)
    return successes
```

### Step 2.2: Polars Transformation

`src/fred_macro_pulse/pipeline/transform.py`:

```python
import polars as pl
from ..client.models import ObservationsResponse

def to_dataframe(responses: list[ObservationsResponse]) -> pl.DataFrame:
    records = []
    for resp in responses:
        for obs in resp.observations:
            records.append({
                "series_id": resp.series_id,
                "observation_date": obs.date,
                "value_raw": obs.value,
            })

    df = pl.DataFrame(records)

    return (
        df.with_columns([
            pl.col("observation_date").cast(pl.Date),
            pl.when(pl.col("value_raw") == ".")
              .then(None)
              .otherwise(pl.col("value_raw").cast(pl.Float64))
              .alias("value"),
        ])
        .drop("value_raw")
        .filter(pl.col("value").is_not_null())
    )
```

### Step 2.3: DuckDB Upsert

FRED periodically revises historical values. The upsert detects changes and flags them.

`src/fred_macro_pulse/pipeline/load.py`:

```python
import duckdb
import polars as pl
import uuid
from datetime import datetime, timezone

def load_raw(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame, run_id: str) -> int:
    df_raw = df.with_columns([
        pl.lit(run_id).alias("run_id"),
        pl.lit(datetime.now(timezone.utc)).alias("vintage_date"),
    ])
    conn.execute("INSERT INTO raw_observations SELECT * FROM df_raw")
    return len(df_raw)

def upsert_facts(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> None:
    conn.execute("""
        INSERT INTO fact_observations (series_id, observation_date, value, is_revised, loaded_at)
        SELECT
            series_id,
            observation_date,
            value,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM fact_observations f
                    WHERE f.series_id = src.series_id
                      AND f.observation_date = src.observation_date
                      AND f.value != src.value
                ) THEN TRUE ELSE FALSE
            END,
            now()
        FROM df AS src
        ON CONFLICT (series_id, observation_date)
        DO UPDATE SET
            value     = excluded.value,
            is_revised = TRUE,
            loaded_at  = now()
    """)

def load_series_metadata(
    conn: duckdb.DuckDBPyConnection,
    metadata_records: list[dict],
) -> None:
    df_meta = pl.DataFrame(metadata_records)
    conn.execute("""
        INSERT OR REPLACE INTO dim_series SELECT * FROM df_meta
    """)
```

### Step 2.4: Basic CLI

`src/fred_macro_pulse/cli.py`:

```python
import typer
import asyncio
import logging
import yaml
from pathlib import Path
from .pipeline.extract import extract_all
from .pipeline.transform import to_dataframe
from .pipeline.load import load_raw, upsert_facts
from .warehouse.schema import bootstrap, get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)
app = typer.Typer()

def load_series_ids() -> list[str]:
    catalog = Path(__file__).parent / "config" / "series.yaml"
    data = yaml.safe_load(catalog.read_text())
    return [s["id"] for s in data["series"]]

@app.command()
def run(
    series: list[str] = typer.Option(None, help="Override series IDs"),
    dry_run: bool = typer.Option(False, help="Extract only, skip load"),
    backfill: bool = typer.Option(False, help="Ignore watermarks; re-fetch full history for all series"),
):
    """Run the FRED ETL pipeline."""
    conn = get_connection()
    bootstrap(conn)
    ids = series or load_series_ids()

    logger.info("Fetching %d series (backfill=%s)", len(ids), backfill)
    responses = asyncio.run(extract_all(ids, conn, backfill=backfill))
    df = to_dataframe(responses)
    logger.info("Transformed %d observations from %d series", len(df), len(responses))

    if not dry_run:
        upsert_facts(conn, df)
        logger.info("Loaded to DuckDB.")

if __name__ == "__main__":
    app()
```

**Deliverable check:** `uv run python -m fred_macro_pulse.cli run --dry-run` fetches real data.

---

## Stage 3: Analytics Layer (Week 3)

**Goal:** SQL views deployed, data quality checks running, tests written.

### Step 3.1: Deploy Analytics Views

`src/fred_macro_pulse/warehouse/schema.py` runs all `.sql` files in `migrations/` in order on bootstrap. Create `003_views.sql` with all five views from the Schema section above.

### Step 3.2: Data Quality Checks

Add a `qa.py` module that runs after every load:

```python
import duckdb

CHECKS: list[tuple[str, str, object]] = [
    ("No nulls in series_id",
     "SELECT COUNT(*) FROM fact_observations WHERE series_id IS NULL",
     lambda v: v == 0),
    ("No future dates",
     "SELECT COUNT(*) FROM fact_observations WHERE observation_date > today()",
     lambda v: v == 0),
    ("Latest CPI within 45 days",
     "SELECT DATEDIFF('day', MAX(observation_date), today()) FROM fact_observations WHERE series_id = 'CPIAUCSL'",
     lambda v: v is not None and v <= 45),
    ("All catalog series loaded",
     "SELECT COUNT(DISTINCT series_id) FROM fact_observations",
     lambda v: v >= 30),
]

def run_checks(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, bool]]:
    return [
        (name, passes(conn.execute(query).fetchone()[0]))
        for name, query, passes in CHECKS
    ]
```

### Step 3.3: Tests

`tests/test_transform.py`:

```python
import polars as pl
from fred_macro_pulse.pipeline.transform import to_dataframe
from fred_macro_pulse.client.models import ObservationsResponse, Observation
from datetime import date

def test_missing_values_become_null():
    resp = ObservationsResponse(
        series_id="UNRATE",
        observations=[
            Observation(date=date(2024, 1, 1), value="."),
            Observation(date=date(2024, 2, 1), value="3.7"),
        ]
    )
    df = to_dataframe([resp])
    assert len(df) == 1
    assert df["value"][0] == 3.7

def test_value_cast_to_float():
    resp = ObservationsResponse(
        series_id="FEDFUNDS",
        observations=[Observation(date=date(2024, 3, 1), value="5.33")]
    )
    df = to_dataframe([resp])
    assert df["value"].dtype == pl.Float64
```

`tests/test_client.py` uses `httpx.MockTransport` to avoid live API calls in CI.

### Step 3.4: Makefile

```makefile
.PHONY: setup run test lint clean

setup:
	uv sync
	@[ -f .env ] || cp .env.example .env

run:
	uv run python -m fred_macro_pulse.cli run

backfill:
	uv run python -m fred_macro_pulse.cli run --backfill

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

clean:
	rm -f data/fred_macro.duckdb
```

**Deliverable check:** `make test` passes all tests. `make run` loads data. All five views queryable.

---

## Stage 4: Production Polish (Week 4)

**Goal:** GitHub Actions running, README complete, optional dashboard deployed.

### Step 4.1: GitHub Actions

`.github/workflows/pipeline.yml`:

```yaml
name: Weekly Pipeline

on:
  schedule:
    - cron: '0 11 * * 1'   # Every Monday 6AM ET (11AM UTC)
  workflow_dispatch:         # Manual trigger button in GitHub UI

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v2
      - name: Install dependencies
        run: uv sync
      - name: Run pipeline
        env:
          FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
        run: uv run python -m fred_macro_pulse.cli run
      - name: Upload DuckDB as artifact
        uses: actions/upload-artifact@v4
        with:
          name: fred-macro-duckdb
          path: data/fred_macro.duckdb
          retention-days: 30
```

`.github/workflows/ci.yml`:

```yaml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v2
      - run: uv sync
      - run: uv run ruff check src/ tests/
      - run: uv run pytest tests/ -v
```

Add `FRED_API_KEY` as a GitHub Actions secret under Settings > Secrets.

### Step 4.2: Streamlit Dashboard (Optional)

`dashboard/app.py`:

```python
import streamlit as st
import duckdb
import polars as pl

st.set_page_config(page_title="FRED Macro Pulse", layout="wide")
st.title("FRED Macro Pulse")
st.caption("Incremental ETL pipeline for Federal Reserve economic data")

conn = duckdb.connect("data/fred_macro.duckdb", read_only=True)

st.subheader("Latest Values")
df = conn.execute("SELECT * FROM v_latest_values ORDER BY category, title").pl()
st.dataframe(df, use_container_width=True)

st.subheader("Recession Signal Monitor")
signals = conn.execute("""
    SELECT * FROM v_recession_signals
    ORDER BY observation_date DESC LIMIT 24
""").pl()
st.dataframe(signals, use_container_width=True)

st.subheader("Yield Curve")
yc = conn.execute("SELECT * FROM v_yield_curve ORDER BY observation_date DESC LIMIT 500").pl()
st.line_chart(yc.select(["observation_date", "spread"]).to_pandas().set_index("observation_date"))
```

Deploy free: push to GitHub, connect repo at share.streamlit.io.

### Step 4.3: README Structure

The README is a first impression. Structure it as:

1. One sentence describing the project
2. Architecture diagram (copy the ASCII art from this document)
3. Quickstart (4 commands from clone to first run)
4. Series catalog summary (link to `series.yaml`)
5. Example SQL queries against the views
6. How the incremental load works (the watermark pattern explained in plain English)
7. GitHub Actions badge showing last pipeline run status

### Step 4.4: Example Queries Section (README showcase)

```sql
-- Current recession risk level
SELECT observation_date, risk_level, signal_score
FROM v_recession_signals
ORDER BY observation_date DESC LIMIT 1;

-- CPI trend last 24 months with YoY change
SELECT observation_date, value, yoy_pct_change
FROM v_yoy_change
WHERE series_id = 'CPIAUCSL'
ORDER BY observation_date DESC LIMIT 24;

-- Is the yield curve currently inverted?
SELECT observation_date, spread, inverted
FROM v_yield_curve
ORDER BY observation_date DESC LIMIT 1;

-- All series that have been revised since initial load
SELECT DISTINCT series_id, COUNT(*) AS revised_count
FROM fact_observations
WHERE is_revised = TRUE
GROUP BY series_id
ORDER BY revised_count DESC;

-- Current macro regime (EXPANSION / NEUTRAL / CONTRACTION) with component breakdown
SELECT factor, score, composite_score, macro_regime
FROM v_macro_composite
ORDER BY factor;
```

---

## Environment Setup (from zero to first run)

```bash
# 1. Clone and enter project
git clone https://github.com/YOUR_USERNAME/fred-macro-pulse
cd fred-macro-pulse

# 2. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 3. Install dependencies and copy env template
make setup

# 4. Add your free FRED API key to .env
#    Get one at: https://fred.stlouisfed.org/docs/api/api_key.html
echo "FRED_API_KEY=your_key_here" >> .env

# 5. Run the pipeline
make run

# 6. Query the warehouse
duckdb data/fred_macro.duckdb "SELECT * FROM v_latest_values LIMIT 10;"
```

---

## Key Technical Decisions (for interview discussion)

| Decision | Alternative Considered | Why This Choice |
|---|---|---|
| DuckDB over SQLite | SQLite | DuckDB has native Polars integration, window functions, and columnar storage ideal for time-series analytics |
| Polars over Pandas | Pandas | Faster on large frames, LazyFrame API, stricter null handling matches FRED missing data patterns |
| Append-only raw layer | Overwrite on each run | Auditable history; can replay transformations; detects revisions |
| YAML series catalog | Hardcoded list | Declarative config mirrors production data platform patterns (dbt, Airbyte) |
| uv over pip/poetry | pip | Single tool for venv + lockfile + package management; meaningfully faster |
| GitHub Actions for scheduling | Airflow, Prefect | Zero infrastructure, zero cost, sufficient for weekly cadence |
| Semaphore for rate limiting | Sleep between requests | Semaphore allows true concurrency up to the limit rather than serializing all requests |
| `v_macro_composite` regime view | Multiple individual signal views | Single queryable label (EXPANSION/NEUTRAL/CONTRACTION) makes the output consumable in dashboards and interviews without requiring callers to interpret raw signals |
| Date arithmetic in `v_yoy_change` (`- INTERVAL 1 YEAR`) | `LAG(12)` | `LAG(12)` assumes monthly frequency; date arithmetic is correct for daily, weekly, and quarterly series without per-frequency logic |
| `pipeline_runs` audit table | None | Enables observability: you can tell when the last successful run was, how many rows loaded, and whether the weekly cron is actually executing |
