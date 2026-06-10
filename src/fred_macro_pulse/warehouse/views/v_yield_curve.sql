-- Uses pre-fetched T10Y2Y series for the spread rather than recomputing DGS10 - DGS2.
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
