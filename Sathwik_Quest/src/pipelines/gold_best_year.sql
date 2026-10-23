CREATE OR REFRESH MATERIALIZED VIEW gold_best_year (
    CONSTRAINT unique_series
        EXPECT (series_row_count = 1)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT readable_label
        EXPECT (series_label IS NOT NULL AND length(series_label) > 0)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT valid_result
        EXPECT (
            (
                result_status = 'AVAILABLE'
                AND best_year IS NOT NULL
                AND quarterly_value_sum IS NOT NULL
                AND quarter_count BETWEEN 1 AND 4
                AND tied_year_count >= 1
            )
            OR
            (
                result_status = 'NO_QUARTERLY_DATA'
                AND best_year IS NULL
                AND quarterly_value_sum IS NULL
                AND quarter_count IS NULL
                AND tied_year_count IS NULL
            )
        )
        ON VIOLATION FAIL UPDATE
)
COMMENT 'Best year per series by summed Q01–Q04 values; earliest year breaks ties.'
AS
WITH annual_totals AS (
    SELECT
        run_id,
        series_id,
        year,
        SUM(value) AS quarterly_value_sum,
        COUNT(DISTINCT period) AS quarter_count
    FROM silver_bls_observations
    WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
    GROUP BY run_id, series_id, year
),
ranked_years AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY run_id, series_id
            ORDER BY quarterly_value_sum DESC, year ASC
        ) AS year_rank,
        COUNT(*) OVER (
            PARTITION BY run_id, series_id, quarterly_value_sum
        ) AS tied_year_count
    FROM annual_totals
),
results AS (
    SELECT
        s.run_id,
        s.series_id,
        s.series_label,
        s.sector_name,
        s.class_text,
        s.measure_text,
        s.duration_text,
        s.seasonal_text,
        r.year AS best_year,
        r.quarterly_value_sum,
        r.quarter_count,
        r.tied_year_count,
        CASE
            WHEN r.year IS NULL THEN 'NO_QUARTERLY_DATA'
            ELSE 'AVAILABLE'
        END AS result_status
    FROM silver_series AS s
    LEFT JOIN ranked_years AS r
        ON s.run_id = r.run_id
       AND s.series_id = r.series_id
       AND r.year_rank = 1
)
SELECT
    *,
    COUNT(*) OVER (
        PARTITION BY run_id, series_id
    ) AS series_row_count
FROM results;