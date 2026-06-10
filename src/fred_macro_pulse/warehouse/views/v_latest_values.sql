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
LEFT JOIN dim_series d USING (series_id)
WHERE f.value IS NOT NULL
ORDER BY series_id, observation_date DESC;
