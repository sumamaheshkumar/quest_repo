EXPLAIN FORMATTED
SELECT
    o.series_id,
    o.year,
    o.period,
    o.value,
    p.population
FROM workspace.rearc.silver_bls_observations AS o
LEFT JOIN workspace.rearc.silver_population AS p
    ON o.run_id = p.run_id
   AND o.year = p.year
   AND p.nation_id = '01000US'
WHERE o.series_id = 'PRS30006032'
  AND o.period = 'Q01';