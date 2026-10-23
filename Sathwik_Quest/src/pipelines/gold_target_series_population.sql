CREATE OR REFRESH MATERIALIZED VIEW gold_target_series_population (
    CONSTRAINT correct_series_and_period
        EXPECT (series_id = 'PRS30006032' AND period = 'Q01')
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT valid_observation
        EXPECT (year IS NOT NULL AND value IS NOT NULL)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT unique_observation
        EXPECT (observation_key_count = 1)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT valid_population_match
        EXPECT (
            (
                population_status = 'AVAILABLE'
                AND population IS NOT NULL
                AND population > 0
            )
            OR
            (
                population_status = 'NOT_AVAILABLE'
                AND population IS NULL
            )
        )
        ON VIOLATION FAIL UPDATE
)
COMMENT 'All Q01 observations for PRS30006032, preserving years without population.'
AS
WITH joined AS (
    SELECT
        o.run_id,
        o.series_id,
        o.year,
        o.period,
        o.value,
        p.population,
        CASE
            WHEN p.population IS NULL THEN 'NOT_AVAILABLE'
            ELSE 'AVAILABLE'
        END AS population_status
    FROM silver_bls_observations AS o
    LEFT JOIN silver_population AS p
        ON o.run_id = p.run_id
       AND o.year = p.year
       AND p.nation_id = '01000US'
    WHERE o.series_id = 'PRS30006032'
      AND o.period = 'Q01'
)
SELECT
    *,
    COUNT(*) OVER (
        PARTITION BY run_id, series_id, year, period
    ) AS observation_key_count
FROM joined;