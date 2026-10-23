CREATE OR REFRESH MATERIALIZED VIEW gold_population_statistics (
    CONSTRAINT six_years_present
        EXPECT (record_count = 6 AND distinct_year_count = 6)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT correct_year_range
        EXPECT (start_year = 2013 AND end_year = 2018)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT single_snapshot
        EXPECT (snapshot_count = 1 AND run_id IS NOT NULL)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT valid_statistics
        EXPECT (
            mean_population IS NOT NULL
            AND stddev_population IS NOT NULL
            AND stddev_sample IS NOT NULL
            AND stddev_population >= 0
            AND stddev_sample >= 0
        )
        ON VIOLATION FAIL UPDATE
)
COMMENT 'US population mean and both standard deviation definitions, 2013–2018.'
AS
SELECT
    MAX(run_id) AS run_id,
    COUNT(DISTINCT run_id) AS snapshot_count,
    MIN(year) AS start_year,
    MAX(year) AS end_year,
    COUNT(*) AS record_count,
    COUNT(DISTINCT year) AS distinct_year_count,
    AVG(population) AS mean_population,
    STDDEV_POP(population) AS stddev_population,
    STDDEV_SAMP(population) AS stddev_sample
FROM silver_population
WHERE nation_id = '01000US'
  AND year BETWEEN 2013 AND 2018;