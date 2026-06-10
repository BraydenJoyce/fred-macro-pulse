.PHONY: setup run backfill test lint clean

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
