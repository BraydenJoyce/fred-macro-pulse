import duckdb
import streamlit as st

st.set_page_config(page_title="FRED Macro Pulse", layout="wide")
st.title("FRED Macro Pulse")
st.caption("Incremental ETL pipeline for Federal Reserve economic data")

conn = duckdb.connect("data/fred_macro.duckdb", read_only=True)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Macro Regime")
    try:
        regime_df = conn.execute("SELECT * FROM v_macro_composite ORDER BY factor").pl()
        regime = regime_df["macro_regime"][0] if len(regime_df) else "N/A"
        score = regime_df["composite_score"][0] if len(regime_df) else 0
        color = {"EXPANSION": "green", "NEUTRAL": "orange", "CONTRACTION": "red"}.get(regime, "gray")
        st.markdown(f"### :{color}[{regime}] (score: {score})")
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

st.subheader("Latest Values by Category")
try:
    df = conn.execute("SELECT * FROM v_latest_values ORDER BY category, title").pl()
    st.dataframe(df, use_container_width=True)
except Exception:
    st.info("Run the pipeline first to populate data.")

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

st.subheader("Pipeline Run History")
try:
    runs = conn.execute("""
        SELECT run_id, started_at, status, series_count, rows_loaded
        FROM pipeline_runs ORDER BY started_at DESC LIMIT 10
    """).pl()
    st.dataframe(runs, use_container_width=True)
except Exception:
    st.info("No pipeline runs recorded yet.")
