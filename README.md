# FRED Macro Pulse

[![CI Status](https://github.com/YOUR_USERNAME/fred-macro-pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/fred-macro-pulse/actions/workflows/ci.yml)
[![Weekly Pipeline](https://github.com/YOUR_USERNAME/fred-macro-pulse/actions/workflows/pipeline.yml/badge.svg)](https://github.com/YOUR_USERNAME/fred-macro-pulse/actions/workflows/pipeline.yml)

A production-grade, incremental ETL (Extract, Transform, Load) pipeline and analytics dashboard for Federal Reserve Economic Data (FRED). The system automatically extracts key macroeconomic time series, performs schema validation and data cleaning via Polars, upserts them into a local DuckDB analytical warehouse, calculates high-value analytical views (such as rolling averages, YoY changes, yield spreads, and composite recession indicators), and serves them via a Streamlit interface.

---

## Architecture Diagram

```text
┌─────────────────────────────────────────────────────────────────┐
│                        FRED PUBLIC API                          │
│          api.stlouisfed.org/fred/series/observations            │
└────────────────────────────┬────────────────────────────────────┘
                             │ async httpx (batch, rate-limited)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      EXTRACTION LAYER                           │
│  AsyncFREDClient  ──►  Pydantic Response Models                 │
│  Semaphore-gated batching (respects 120 req/min limit)          │
│  Incremental: only fetches observations newer than watermark    │
└────────────────────────────┬────────────────────────────────────┘
                             │ List[Observation]
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER                         │
│  Polars DataFrames                                              │
│  • Cast "." missing values to null                              │
│  • Parse date strings to date type                              │
│  • Enrich series categories from catalog YAML                   │
│  • Drop nulls & prepare for analytical staging                  │
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
│  │ (append-only     │     │ fact_observations               │    │
│  │  audit trail)    │     │ (upsert, revision-aware)       │    │
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
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                           INTERFACES                            │
│  Typer CLI  ──  DuckDB SQL shell  ──  Streamlit dashboard       │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION & CI                         │
│  GitHub Actions: weekly cron, uploads duckdb file as artifact   │
│  Streamlit Cloud: Auto-pulls database artifact from GitHub      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Features

1. **High-Performance Async Extraction**: Semaphore-controlled concurrency using `httpx` to fetch data asynchronously without triggering FRED's 120-request/min rate limits.
2. **Intelligent Incremental Loads (Watermarking)**: Checks existing warehouse data before requesting observations, fetching only new data since the latest recorded observation date (`watermark`).
3. **Data Quality & Sanitization**: Polars handles raw cleaning, casting the FRED `.` missing indicator to SQL `NULL` and converting columns to proper numerical and date types.
4. **Auditability (Raw & Schema layers)**: Retains an append-only historical audit trail in `raw_observations` tagged with a UUID `run_id`, while keeping clean fact/dimension tables.
5. **Revision Detection**: Ingests historical data and checks for changes. If FRED revises historical observations, the pipeline automatically detects the discrepancy, updates the record, and flips the `is_revised` flag to `TRUE`.
6. **Sophisticated Analytics Views**: Built-in SQL views for complex analytical tasks:
   - **Year-Over-Year (YoY)**: Frequency-safe YoY percent changes.
   - **Rolling Averages**: 3, 6, and 12-month moving averages.
   - **Yield Curve**: Inversion monitoring using 10Y-2Y spreads.
   - **Recession Monitoring**: Aggregates unemployment momentum (Sahm Rule proxy), jobless claims, and yield spreads into a composite risk gauge (`NORMAL`, `WATCH`, `ELEVATED`).
   - **Macro Regime Classification**: Combines indicators to label the macroeconomy under `EXPANSION`, `NEUTRAL`, or `CONTRACTION` regimes.
7. **Post-Load QA Suite**: Runs validation scripts ensuring database integrity, no future observations, and that catalog datasets are loaded properly.
8. **Interactive Streamlit Dashboard**: Displays regime status, recession risks, yield curve charts, and pipeline run logs. Includes support for GitHub Actions integration in deployment.

---

## Quickstart

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (fast Python package installer and manager)
- A free FRED API Key (register on [FRED API Page](https://fred.stlouisfed.org/docs/api/api_key.html))

### 1. Installation & Environment Setup

Clone this repository and run the setup task using the provided `Makefile`:

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/fred-macro-pulse.git
cd fred-macro-pulse

# Install dependencies and create .env file automatically
make setup
```

Add your FRED API key to the newly created `.env` file:

```ini
FRED_API_KEY=your_fred_api_key_here
DB_PATH=data/fred_macro.duckdb
LOG_LEVEL=INFO
```

### 2. Execute the Pipeline

Run the ETL pipeline to pull the historical observations for all catalog indicators:

```bash
# Run initial load (bootstrap tables, fetch, transform, load and run QA)
make run
```

If you need to discard watermarks and completely re-fetch the historical observation logs:

```bash
# Execute a full backfill
make backfill
```

### 3. Running the Test Suite

Run unit and integration tests (mocking the HTTP clients):

```bash
make test
```

### 4. Running the Dashboard

Start the Streamlit dashboard app to visualize the ingested and derived data:

```bash
uv run streamlit run dashboard/app.py
```

---

## Catalog Summary

The system tracks 30+ highly-curated macroeconomic series declared in `src/fred_macro_pulse/config/series.yaml`:

| Category | Indicators Included | Key Series IDs |
| :--- | :--- | :--- |
| **Growth** | Real GDP, Real GDP Growth Rate (QoQ SAAR) | `GDPC1`, `A191RL1Q225SBEA` |
| **Labor** | Unemployment Rate, Total Nonfarm Payrolls, Initial Claims, Underemployment (U-6), Employment-Population Ratio | `UNRATE`, `PAYEMS`, `ICSA`, `U6RATE`, `LNS12300000` |
| **Inflation** | CPI (Headline & Core), PCE Price Index (Headline & Core), PPI Final Demand | `CPIAUCSL`, `CPILFESL`, `PCEPI`, `PCEPILFE`, `PPIFIS` |
| **Rates** | Federal Funds Rate, 10-Year Treasury, 2-Year Treasury, 10Y-2Y Spread, 30Y Fixed Mortgage | `FEDFUNDS`, `DGS10`, `DGS2`, `T10Y2Y`, `MORTGAGE30US` |
| **Housing** | Housing Starts, Building Permits, Case-Shiller House Price Index, Median House Price | `HOUST`, `PERMIT`, `CSUSHPISA`, `MSPUS` |
| **Consumer** | Advance Retail Sales, UMich Consumer Sentiment, Personal Savings Rate, Personal Consumption Expenditures | `RSXFS`, `UMCSENT`, `PSAVERT`, `PCE` |
| **Industry** | Industrial Production, Manufacturing Industrial Production, Manufacturers New Orders | `INDPRO`, `IPMAN`, `AMTMNO` |
| **Leading** | US Leading Index, Smoothed US Recession Probability, Chicago Fed National Activity Index | `USSLIND`, `RECPROUSM156N`, `CFNAI` |

---

## Database Schema Design

The target DuckDB database contains a three-layer dimensional model:

### 1. Raw Audit Layer
* **`raw_observations`**: Captures raw payload strings exactly as returned from the API, including vintage dates and pipeline execution UUIDs to allow full historical audits and replays.

### 2. Dimensional Model (Clean Layer)
* **`dim_series`**: Dimension table storing series metadata (title, units, frequency, seasonal adjustment, category, notes, and last updated).
* **`fact_observations`**: The primary facts target containing cleaned dates, parsed float values, loading timestamps, and revision markers.
* **`pipeline_runs`**: Logs every execution run including status, start/finish times, record counts, and errors.

### 3. Analytics Layer (Views)
SQL view definitions are automatically deployed during pipeline initialization (defined in `src/fred_macro_pulse/warehouse/views`):

* **`v_latest_values`**: Fetches the most recent observation for every catalog series.
* **`v_yoy_change`**: Computes year-over-year percentage adjustments. Utilizes actual calendar math (`- INTERVAL 1 YEAR`) instead of fixed lag values (e.g. `LAG(12)`) to remain frequency-safe.
* **`v_rolling_avg`**: Computes 3, 6, and 12-month rolling means.
* **`v_yield_curve`**: Analyzes yield spread inversions using Daily Constant Maturity rates.
* **`v_recession_signals`**: Tracks risk triggers (unemployment momentum, claims deviations, and curve inversions) into scorecards.
* **`v_macro_composite`**: Labels overall economic condition (`EXPANSION`, `NEUTRAL`, `CONTRACTION`).

---

## Core Pipeline Mechanics

### Watermark Pattern (Incremental Ingestion)
To avoid overloading the FRED API and downloading megabytes of redundant historical records, the pipeline implements an incremental watermark pattern:

1. Prior to downloading, the pipeline queries the local warehouse:
   ```sql
   SELECT series_id, MAX(observation_date)::VARCHAR FROM fact_observations GROUP BY series_id;
   ```
2. The returned maximum date serves as the `observation_start` bookmark for that series.
3. The async client appends this constraint to the API query parameters. Only newer observations released since the last pipeline run are fetched.
4. Pass the `--backfill` flag to the CLI command to disable this constraint and run a full refresh.

### Revision Detection & Fact Upsert
Economic data is frequently updated and revised retroactively by government agencies. To account for this, the pipeline performs a state-aware upsert into `fact_observations`:

```sql
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
    is_revised = (excluded.value IS DISTINCT FROM fact_observations.value) OR fact_observations.is_revised,
    value      = excluded.value,
    loaded_at  = now();
```
If a new payload changes an existing observation's value, it sets the `is_revised` flag to `TRUE`.

### Data Quality (QA) Suite
After every load, the system runs data quality assertions:
- **Null Checks**: Validates that all records in `fact_observations` have active series IDs.
- **Future Date Check**: Asserts that no economic records are logged with future timestamps.
- **Freshness Check**: Ensures key index updates (like CPI) are within expectation bounds (<= 75 days).
- **Volume Check**: Verifies that the catalog requirements are met (>= 30 active series loaded).

---

## Example SQL Queries

You can query the DuckDB warehouse file directly using the CLI:

```bash
duckdb data/fred_macro.duckdb
```

Here are some high-value analytical queries you can run against the views:

### 1. Current Composite Recession Risk Level
```sql
SELECT observation_date, risk_level, signal_score, yield_curve_inverted, unemployment_rising, claims_elevated
FROM v_recession_signals
ORDER BY observation_date DESC
LIMIT 1;
```

### 2. CPI Inflation Trend (Last 12 Months)
```sql
SELECT observation_date, value AS cpi_index, yoy_pct_change AS cpi_yoy_inflation
FROM v_yoy_change
WHERE series_id = 'CPIAUCSL'
ORDER BY observation_date DESC
LIMIT 12;
```

### 3. Current Macro Regime Component Breakdown
```sql
SELECT factor, score, composite_score, macro_regime
FROM v_macro_composite
ORDER BY factor;
```

### 4. Top Revised Series
```sql
SELECT series_id, COUNT(*) AS revision_count
FROM fact_observations
WHERE is_revised = TRUE
GROUP BY series_id
ORDER BY revision_count DESC;
```

---

## Streamlit Dashboard

The Streamlit app (`dashboard/app.py`) parses the local DuckDB database or pulls the latest weekly database artifact from GitHub Actions:

- **Regime Scorecard**: Displays current macro status and components.
- **Recession Signal Monitor**: Tracks risk factors in a readable table.
- **Yield Curve chart**: Generates a 500-day rolling line graph of the 10Y-2Y Treasury spread.
- **Auditing Logs**: Visualizes recent runs, error messages, and ingested row counts.

### Streamlit Cloud Deploy
To host the dashboard online for free:
1. Push your project to GitHub.
2. Register/connect your repository at [share.streamlit.io](https://share.streamlit.io).
3. Set these Streamlit Secrets in your dashboard control panel:
   - `GH_TOKEN`: Your GitHub Personal Access Token (for fetching Action artifacts).
   - `GITHUB_REPO`: `your_github_username/fred-macro-pulse` (repo path).

---

## Key Technical Decisions

| Decision | Alternative | Rationale |
| :--- | :--- | :--- |
| **DuckDB** | SQLite | Columnar architecture, native Polars/Arrow compatibility, and high-performance SQL window functions ideal for time-series. |
| **Polars** | Pandas | Stricter null checking, faster typed casting, and LazyFrame optimizations. |
| **Append-only Raw Layer** | Direct Truncate / Overwrite | Preserves complete history, supports time-travel analysis, and enables revision detection. |
| **YAML Catalog** | Hardcoded Lists | Separation of config and code. Makes adding/removing indicators declarative. |
| **`uv` Manager** | Poetry / Pip | Unmatched lockfile performance and virtualenv speeds. |
| **GitHub Actions** | Airflow / Prefect | Zero-cost serverless execution. Easily handles weekly cron and uploads artifacts. |
| **Semaphores** | Blocking/Sleep | Allows controlled concurrency (throttling requests up to FRED limits) rather than single-thread blocking. |

---

## Development & Makefile Commands

A `Makefile` is provided to streamline local development:

- `make setup`: Syncs python virtual environments and copies environment variables.
- `make run`: Starts the ETL pipeline locally.
- `make backfill`: Runs the pipeline with a forced historical backfill (ignores watermarks).
- `make test`: Executes all pytest suites (unit, mock integrations, schema checks).
- `make lint`: Performs fast code style formatting and linting checks using Ruff.
- `make clean`: Removes the active local DuckDB database to support clean restarts.
