-- Frequency-safe YoY: uses date arithmetic (- INTERVAL 1 YEAR) rather than LAG(12),
-- which would be wrong for daily, weekly, and quarterly series.
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
LEFT JOIN dim_series d USING (series_id)
LEFT JOIN fact_observations prev
    ON  prev.series_id        = f.series_id
    AND prev.observation_date = f.observation_date - INTERVAL 1 YEAR
WHERE f.value IS NOT NULL
  AND prev.value IS NOT NULL;
