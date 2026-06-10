import asyncio
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

import typer
import yaml

from .pipeline.extract import extract_all
from .pipeline.load import load_raw_observations, load_series_metadata, upsert_facts
from .pipeline.qa import run_checks
from .pipeline.transform import to_dataframe
from .warehouse.schema import bootstrap, get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="FRED Macro Pulse — incremental ETL for Federal Reserve economic data.")


def load_catalog() -> list[dict]:
    path = Path(__file__).parent / "config" / "series.yaml"
    return yaml.safe_load(path.read_text())["series"]


@app.command()
def run(
    series: list[str] = typer.Option(None, help="Override series IDs (default: full catalog)"),
    dry_run: bool = typer.Option(False, help="Extract and transform only; skip all writes"),
    backfill: bool = typer.Option(False, help="Ignore watermarks; re-fetch full history"),
    skip_qa: bool = typer.Option(False, help="Skip post-load data quality checks"),
) -> None:
    """Run the FRED ETL pipeline."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    conn = get_connection()
    bootstrap(conn)

    catalog = load_catalog()
    catalog_by_id = {s["id"]: s for s in catalog}
    ids = series or [s["id"] for s in catalog]

    if not dry_run:
        conn.execute(
            "INSERT INTO pipeline_runs (run_id, started_at, status) VALUES (?, ?, 'running')",
            [run_id, started_at],
        )

    try:
        logger.info("Fetching %d series (backfill=%s, dry_run=%s)", len(ids), backfill, dry_run)
        responses, metadata = asyncio.run(extract_all(ids, conn, backfill=backfill))
        df = to_dataframe(responses)
        logger.info("Transformed %d observations from %d series", len(df), len(responses))

        if dry_run:
            logger.info("Dry run — skipping all writes.")
            return

        # Enrich metadata with categories from the YAML catalog
        for record in metadata:
            record["category"] = catalog_by_id.get(record["series_id"], {}).get("category")

        n_raw = load_raw_observations(conn, responses, run_id, started_at)
        upsert_facts(conn, df)
        load_series_metadata(conn, metadata)

        conn.execute("""
            UPDATE pipeline_runs SET
                finished_at  = ?,
                series_count = ?,
                rows_loaded  = ?,
                status       = 'success'
            WHERE run_id = ?
        """, [datetime.now(UTC), len(responses), n_raw, run_id])

        logger.info("Loaded %d raw rows across %d series.", n_raw, len(responses))

        if not skip_qa:
            results = run_checks(conn)
            failed = [(name, ok) for name, ok in results if not ok]
            for name, _ in failed:
                logger.warning("QA FAIL: %s", name)
            if not failed:
                logger.info("All QA checks passed.")

    except Exception as exc:
        if not dry_run:
            conn.execute("""
                UPDATE pipeline_runs SET
                    finished_at = ?,
                    status      = 'failed',
                    error_msg   = ?
                WHERE run_id = ?
            """, [datetime.now(UTC), str(exc), run_id])
        logger.error("Pipeline failed: %s", exc)
        raise typer.Exit(1) from exc


@app.command()
def status() -> None:
    """Show last pipeline runs."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT run_id, started_at, finished_at, series_count, rows_loaded, status, error_msg
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 10
    """).fetchall()
    if not rows:
        typer.echo("No pipeline runs recorded yet.")
        return
    for row in rows:
        typer.echo(
            f"{row[1]}  {row[5]:10}  series={row[3]}  rows={row[4]}  id={row[0][:8]}"
            + (f"  err={row[6]}" if row[6] else "")
        )


if __name__ == "__main__":
    app()
