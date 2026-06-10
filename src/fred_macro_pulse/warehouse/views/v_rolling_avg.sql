CREATE OR REPLACE VIEW v_rolling_avg AS
SELECT
    f.series_id,
    d.title,
    d.frequency,
    f.observation_date,
    f.value,
    AVG(f.value) OVER (
        PARTITION BY f.series_id ORDER BY f.observation_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3p,
    AVG(f.value) OVER (
        PARTITION BY f.series_id ORDER BY f.observation_date
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_6p,
    AVG(f.value) OVER (
        PARTITION BY f.series_id ORDER BY f.observation_date
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_12p
FROM fact_observations f
LEFT JOIN dim_series d USING (series_id)
WHERE f.value IS NOT NULL;
