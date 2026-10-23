# Validation results

## Main-source baseline

- BLS source files: 12
- Population records: 11
- BLS AllData observations: 77,126
- Observed series: 282
- BLS year range: 1947–2026
- Missing required observation values: 0
- Duplicate observation keys: 0
- Observations without series metadata: 0

These counts describe the tested source snapshot, not permanent source limits.

## Analytical results

### Population statistics, 2013–2018
- Annual records: 6
- Mean: 322,069,808
- Population standard deviation: approximately 3,796,119.936934
- Sample standard deviation: approximately 4,158,441.040908
- SQL and PySpark matched an independent Python statistics calculation.

### Best year by series
- Output series: 282
- Series with quarterly data: 237
- Series without quarterly data: 45
- Partial-year winners: 1
- Series with tied maximum annual sums: 2
- PRS30006032: best year 1994, sum 14.6, four quarters, no tie.
- SQL and PySpark outputs matched.

### Target-series population join
- Series: PRS30006032; period: Q01
- Output observations: 39, covering 1988–2026
- Population matches: 11
- Unmatched observations retained with null population: 28
- SQL and PySpark outputs matched.

## Completed reliability tests

- Repeated publication of the same snapshot reused files and manifest.
- A new unchanged snapshot reused all 13 source files.
- Normal pipeline reruns did not duplicate Bronze records.
- A synthetic population change updated Silver and Gold.
- Previous population versions remained in Bronze.
- Optional-file addition appeared in the current inventory.
- Optional-file removal left the current inventory and retained history.
- An uploaded object without a manifest did not change analytical results.
- Subsequent publication reused that object and updated analytical results.
- Eight source-validation unit tests passed.
- A deliberately incorrect Gold expectation failed with
  EXPECTATION_VIOLATION and identified six_years_present.
- Restoring the correct expectation allowed a successful update.
- Main analytical results remained unchanged during isolated tests.

## Scope and limitations

- Integration fixtures ran in workspace.rearc_test.
- Optional-file removal was tested; required-file removal was not tested.
- Malformed-input unit tests exercised downloader validation.
- The pipeline failure test deliberately changed an expectation;
  it did not inject malformed source data into streaming Bronze.
- These tests do not establish atomic publication across all Gold tables.
- Concurrent publishers, network retry exhaustion, alert delivery,
  and large-scale performance have not been verified.