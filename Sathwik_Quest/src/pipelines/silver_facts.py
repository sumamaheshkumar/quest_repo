from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def current_file_rows(bronze_table, relative_path):
    """Select parsed rows belonging to the current snapshot's file."""
    inventory = (
        spark.read.table("silver_snapshot_files")
        .filter(F.col("relative_path") == relative_path)
        .select("run_id", "volume_path", "sha256")
    )

    return spark.read.table(bronze_table).join(
        inventory,
        ["volume_path", "sha256"],
        "inner",
    )


@dp.materialized_view(
    name="silver_bls_observations",
    comment="Typed BLS observations for the current published snapshot."
)
@dp.expect_all_or_fail({
    "series_present": "series_id IS NOT NULL AND length(series_id) > 0",
    "valid_year": "year IS NOT NULL AND year BETWEEN 1900 AND 2100",
    "valid_period": "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')",
    "valid_value": "value IS NOT NULL",
    "unique_observation": "observation_key_count = 1",
})
def silver_bls_observations():
    typed = (
        current_file_rows(
            "bronze_bls_observations",
            "bls/pr.data.1.AllData",
        )
        .select(
            "run_id",
            "volume_path",
            "sha256",
            "source_line_number",
            "series_id",
            F.expr("try_cast(year_raw AS INT)").alias("year"),
            "period",
            F.expr(
                "try_cast(value_raw AS DECIMAL(20,6))"
            ).alias("value"),
            F.when(
                F.length(F.trim("footnote_codes")) == 0,
                F.lit(None).cast("string"),
            ).otherwise(F.col("footnote_codes")).alias("footnote_codes"),
        )
    )

    return typed.withColumn(
        "observation_key_count",
        F.count(F.lit(1)).over(
            Window.partitionBy("series_id", "year", "period")
        ),
    )


@dp.materialized_view(
    name="silver_population",
    comment="Typed US population estimates for the current snapshot."
)
@dp.expect_all_or_fail({
    "expected_nation": (
        "nation_id = '01000US' AND nation = 'United States'"
    ),
    "valid_year": "year IS NOT NULL AND year BETWEEN 1900 AND 2100",
    "valid_population": "population IS NOT NULL AND population > 0",
    "whole_number_source": (
        "population_decimal IS NOT NULL "
        "AND population_decimal = floor(population_decimal)"
    ),
    "unique_population_key": "population_key_count = 1",
})
def silver_population():
    typed = (
        current_file_rows(
            "bronze_population",
            "population/population.json",
        )
        .select(
            "run_id",
            "volume_path",
            "sha256",
            "nation_id",
            "nation",
            F.expr("try_cast(year_raw AS INT)").alias("year"),
            F.expr(
                "try_cast(population_raw AS DECIMAL(38,10))"
            ).alias("population_decimal"),
        )
        .withColumn(
            "population",
            F.expr("try_cast(population_decimal AS BIGINT)"),
        )
    )

    return typed.withColumn(
        "population_key_count",
        F.count(F.lit(1)).over(
            Window.partitionBy("nation_id", "year")
        ),
    )