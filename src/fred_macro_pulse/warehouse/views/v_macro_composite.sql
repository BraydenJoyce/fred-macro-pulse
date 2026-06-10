CREATE OR REPLACE VIEW v_macro_composite AS
WITH latest_cpi_yoy AS (
    SELECT yoy_pct_change
    FROM v_yoy_change
    WHERE series_id = 'CPIAUCSL'
      AND yoy_pct_change IS NOT NULL
    ORDER BY observation_date DESC
    LIMIT 1
),
components AS (
    SELECT 'yield_curve' AS factor,
        CASE WHEN value <  0   THEN -2
             WHEN value < 0.5  THEN -1
             ELSE 1
        END AS score
    FROM v_latest_values WHERE series_id = 'T10Y2Y'

    UNION ALL
    SELECT 'unemployment',
        CASE WHEN value < 4.0 THEN  2
             WHEN value < 5.5 THEN  0
             ELSE -2
        END
    FROM v_latest_values WHERE series_id = 'UNRATE'

    UNION ALL
    SELECT 'cpi_trend',
        CASE WHEN yoy_pct_change < 2.5 THEN  2
             WHEN yoy_pct_change < 4.0 THEN  0
             ELSE -2
        END
    FROM latest_cpi_yoy

    UNION ALL
    SELECT 'leading_index',
        CASE WHEN value > 0 THEN 1 ELSE -1 END
    FROM v_latest_values WHERE series_id = 'USSLIND'

    UNION ALL
    SELECT 'recession_prob',
        CASE WHEN value < 10 THEN  1
             WHEN value < 30 THEN  0
             ELSE -2
        END
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
