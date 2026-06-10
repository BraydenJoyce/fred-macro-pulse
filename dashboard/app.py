import io
import zipfile
from pathlib import Path

import duckdb
import httpx
import streamlit as st

st.set_page_config(page_title="FRED Macro Pulse", layout="wide")
st.title("FRED Macro Pulse")
st.caption("Incremental ETL pipeline for Federal Reserve economic data")

_LOCAL_DB = Path("data/fred_macro.duckdb")
_CLOUD_DB = Path("/tmp/fred_macro.duckdb")


@st.cache_resource(show_spinner="Loading data...")
def get_connection() -> duckdb.DuckDBPyConnection | None:
    if _LOCAL_DB.exists():
        return duckdb.connect(str(_LOCAL_DB), read_only=True)

    if _CLOUD_DB.exists():
        return duckdb.connect(str(_CLOUD_DB), read_only=True)

    # Streamlit Cloud: pull latest artifact from GitHub Actions
    try:
        gh_token = st.secrets["GH_TOKEN"]
        gh_repo = st.secrets["GITHUB_REPO"]
    except (KeyError, FileNotFoundError):
        return None

    headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    resp = httpx.get(
        f"https://api.github.com/repos/{gh_repo}/actions/artifacts"
        "?name=fred-macro-duckdb&per_page=1",
        headers=headers,
        timeout=30,
    )
    artifacts = resp.json().get("artifacts", [])
    if not artifacts:
        return None

    dl = httpx.get(
        f"https://api.github.com/repos/{gh_repo}/actions/artifacts/{artifacts[0]['id']}/zip",
        headers=headers,
        follow_redirects=True,
        timeout=120,
    )
    with zipfile.ZipFile(io.BytesIO(dl.content)) as zf:
        zf.extract("fred_macro.duckdb", "/tmp")

    return duckdb.connect(str(_CLOUD_DB), read_only=True)


conn = get_connection()

if conn is None:
    st.warning(
        "No data found. Run the pipeline locally (`uv run python -m fred_macro_pulse.cli run`)"
        " or configure Streamlit secrets (`GH_TOKEN` and `GITHUB_REPO`) for cloud deployment."
    )
    st.stop()

# ── Regime banner ─────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Macro Regime")
    try:
        regime_df = conn.execute("SELECT * FROM v_macro_composite ORDER BY factor").pl()
        regime = regime_df["macro_regime"][0] if len(regime_df) else "N/A"
        score = regime_df["composite_score"][0] if len(regime_df) else 0
        color = {"EXPANSION": "green", "NEUTRAL": "orange", "CONTRACTION": "red"}.get(
            regime, "gray"
        )
        st.markdown(f"### :{color}[{regime}]  (composite score: {score})")
        st.dataframe(regime_df[["factor", "score"]], use_container_width=True)
    except Exception:
        st.info("Run the pipeline first to populate data.")

with col2:
    st.subheader("Recession Signal Monitor")
    try:
        signals = conn.execute("""
            SELECT * FROM v_recession_signals
            ORDER BY observation_date DESC LIMIT 24
        """).pl()
        st.dataframe(signals, use_container_width=True)
    except Exception:
        st.info("Run the pipeline first to populate data.")

# ── Latest values ─────────────────────────────────────────────────────────────
st.subheader("Latest Values by Category")
try:
    df = conn.execute("SELECT * FROM v_latest_values ORDER BY category, title").pl()
    st.dataframe(df, use_container_width=True)
except Exception:
    st.info("Run the pipeline first to populate data.")

# ── Yield curve ───────────────────────────────────────────────────────────────
st.subheader("Yield Curve Spread (10Y - 2Y)")
try:
    yc = conn.execute("""
        SELECT observation_date, spread, inverted
        FROM v_yield_curve
        ORDER BY observation_date DESC LIMIT 500
    """).pl()
    chart_df = yc.sort("observation_date").to_pandas().set_index("observation_date")
    st.line_chart(chart_df[["spread"]])
except Exception:
    st.info("Run the pipeline first to populate data.")

# ── Regime history ────────────────────────────────────────────────────────────
st.subheader("Regime History")
try:
    regime_hist = conn.execute("""
        SELECT started_at, macro_regime, rows_loaded, status
        FROM pipeline_runs
        WHERE macro_regime IS NOT NULL
        ORDER BY started_at DESC LIMIT 20
    """).pl()
    if len(regime_hist):
        st.dataframe(regime_hist, use_container_width=True)
    else:
        st.info("No regime history yet.")
except Exception:
    st.info("No regime history yet.")

# ── Pipeline runs ─────────────────────────────────────────────────────────────
st.subheader("Pipeline Run History")
try:
    runs = conn.execute("""
        SELECT run_id, started_at, status, series_count, rows_loaded, macro_regime
        FROM pipeline_runs ORDER BY started_at DESC LIMIT 10
    """).pl()
    st.dataframe(runs, use_container_width=True)
except Exception:
    st.info("No pipeline runs recorded yet.")

# ── Refresh button ────────────────────────────────────────────────────────────
if st.button("Refresh data"):
    st.cache_resource.clear()
    st.rerun()
