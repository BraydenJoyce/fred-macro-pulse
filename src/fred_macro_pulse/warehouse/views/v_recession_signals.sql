CREATE OR REPLACE VIEW v_recession_signals AS
WITH yield AS (
    SELECT
        observation_date,
        CASE WHEN value < 0 THEN 1 ELSE 0 END AS yield_curve_inverted
    FROM fact_observations
    WHERE series_id = 'T10Y2Y'
),
unemp AS (
    SELECT
        observation_date,
        CASE
            WHEN value > LAG(value, 3) OVER (ORDER BY observation_date) THEN 1
            ELSE 0
        END AS unemployment_rising
    FROM fact_observations
    WHERE series_id = 'UNRATE'
),
claims AS (
    SELECT
        observation_date,
        CASE
            WHEN value > AVG(value) OVER (
                ORDER BY observation_date
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ) * 1.15 THEN 1
            ELSE 0
        END AS claims_elevated
    FROM fact_observations
    WHERE series_id = 'ICSA'
),
aligned AS (
    SELECT
        y.observation_date,
        y.yield_curve_inverted,
        u.unemployment_rising,
        i.claims_elevated
    FROM yield y
    LEFT JOIN unemp u  ON u.observation_date = y.observation_date
    LEFT JOIN claims i ON i.observation_date = y.observation_date
)
SELECT
    observation_date,
    COALESCE(yield_curve_inverted, 0) AS yield_curve_inverted,
    COALESCE(unemployment_rising,  0) AS unemployment_rising,
    COALESCE(claims_elevated,      0) AS claims_elevated,
    COALESCE(yield_curve_inverted, 0)
        + COALESCE(unemployment_rising, 0)
        + COALESCE(claims_elevated, 0)   AS signal_score,
    CASE
        WHEN COALESCE(yield_curve_inverted, 0)
           + COALESCE(unemployment_rising,  0)
           + COALESCE(claims_elevated,      0) >= 2 THEN 'ELEVATED'
        WHEN COALESCE(yield_curve_inverted, 0)
           + COALESCE(unemployment_rising,  0)
           + COALESCE(claims_elevated,      0) =  1 THEN 'WATCH'
        ELSE 'NORMAL'
    END AS risk_level
FROM aligned
ORDER BY observation_date;
