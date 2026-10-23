"""PySpark alternatives and validation for all three assessment questions.

Run in a Databricks Python notebook after a successful pipeline update.
"""

import math
import statistics

from pyspark.sql import functions as F

SCHEMA = "workspace.rearc"


def table(name):
    return spark.table(f"{SCHEMA}.{name}")


def require_same_rows(actual, expected, columns, label):
    actual = actual.select(*columns)
    expected = expected.select(*columns)

    assert actual.exceptAll(expected).limit(1).count() == 0, (
        f"{label}: unexpected output rows"
    )
    assert expected.exceptAll(actual).limit(1).count() == 0, (
        f"{label}: missing output rows"
    )
    print(f"PASS: {label} SQL and PySpark results match.")


# QUESTION 1: Population statistics, 2013–2018 inclusive.
population_window = table("silver_population").filter(
    (F.col("nation_id") == "01000US")
    & F.col("year").between(2013, 2018)
)

population_alternative = population_window.agg(
    F.count("*").alias("record_count"),
    F.avg("population").alias("mean_population"),
    F.stddev_pop("population").alias("stddev_population"),
    F.stddev_samp("population").alias("stddev_sample"),
).first()

gold_statistics = table("gold_population_statistics").collect()
assert len(gold_statistics) == 1
gold_statistics = gold_statistics[0]

# Only six annual values are collected for the independent reference.
annual_population = (
    population_window.select("year", "population")
    .orderBy("year")
    .collect()
)
assert [row["year"] for row in annual_population] == list(range(2013, 2019))

values = [row["population"] for row in annual_population]
reference = {
    "mean_population": statistics.mean(values),
    "stddev_population": statistics.pstdev(values),
    "stddev_sample": statistics.stdev(values),
}

assert (
    population_alternative["record_count"]
    == gold_statistics["record_count"]
    == 6
)

for metric, expected in reference.items():
    for result in (population_alternative, gold_statistics):
        assert math.isclose(
            float(result[metric]),
            float(expected),
            rel_tol=1e-12,
            abs_tol=1e-6,
        ), f"Question 1: mismatch in {metric}"

print("PASS: Question 1 SQL, PySpark, and Python statistics agree.")


# QUESTION 2: Best year by quarterly sum, with earliest-year tie breaking.
observations = table("silver_bls_observations")

annual = (
    observations
    .filter(F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
    .groupBy("run_id", "series_id", "year")
    .agg(
        F.sum("value").alias("quarterly_value_sum"),
        F.countDistinct("period").alias("quarter_count"),
    )
)

maxima = annual.groupBy("run_id", "series_id").agg(
    F.max("quarterly_value_sum").alias("quarterly_value_sum")
)

candidates = annual.join(
    maxima,
    ["run_id", "series_id", "quarterly_value_sum"],
    "inner",
)

choices = candidates.groupBy("run_id", "series_id").agg(
    F.min("year").alias("best_year"),
    F.count("*").alias("tied_year_count"),
)

winners = (
    candidates.join(choices, ["run_id", "series_id"], "inner")
    .filter(F.col("year") == F.col("best_year"))
    .select(
        "run_id", "series_id", "best_year",
        "quarterly_value_sum", "quarter_count", "tied_year_count",
    )
)

labels = [
    "series_label", "sector_name", "class_text",
    "measure_text", "duration_text", "seasonal_text",
]

best_year_alternative = (
    table("silver_series")
    .select("run_id", "series_id", *labels)
    .join(winners, ["run_id", "series_id"], "left")
    .withColumn(
        "result_status",
        F.when(
            F.col("best_year").isNull(), "NO_QUARTERLY_DATA"
        ).otherwise("AVAILABLE"),
    )
)

require_same_rows(
    table("gold_best_year"),
    best_year_alternative,
    [
        "run_id", "series_id", *labels, "best_year",
        "quarterly_value_sum", "quarter_count",
        "tied_year_count", "result_status",
    ],
    "Question 2",
)


# QUESTION 3: Preserve every target-series Q01 observation.
target = (
    observations
    .filter(
        (F.col("series_id") == "PRS30006032")
        & (F.col("period") == "Q01")
    )
    .select("run_id", "series_id", "year", "period", "value")
)

population = (
    table("silver_population")
    .filter(F.col("nation_id") == "01000US")
    .select("run_id", "year", "population")
)

target_alternative = (
    target.join(population, ["run_id", "year"], "left")
    .withColumn(
        "population_status",
        F.when(
            F.col("population").isNull(), "NOT_AVAILABLE"
        ).otherwise("AVAILABLE"),
    )
)

target_gold = table("gold_target_series_population")

require_same_rows(
    target_gold,
    target_alternative,
    [
        "run_id", "series_id", "year", "period", "value",
        "population", "population_status",
    ],
    "Question 3",
)

assert target.count() > 0
assert target_gold.count() == target.count(), (
    "Question 3: join lost or multiplied observations"
)


# Verify every analytical output belongs to the selected snapshot.
current_runs = (
    table("silver_snapshot_files").select("run_id").distinct().collect()
)
assert len(current_runs) == 1
current_run = current_runs[0]["run_id"]

for name in [
    "gold_population_statistics",
    "gold_best_year",
    "gold_target_series_population",
]:
    runs = table(name).select("run_id").distinct().collect()
    assert len(runs) == 1 and runs[0]["run_id"] == current_run, (
        f"{name}: output snapshot does not match the current inventory"
    )

print("PASS: All Gold outputs reference the current snapshot.")
print("PASS: Required analytical validation completed.")