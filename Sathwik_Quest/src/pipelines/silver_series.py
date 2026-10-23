from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def current_metadata(relative_path, columns):
    """Project named fields from the current version of a metadata file."""
    inventory = (
        spark.read.table("silver_snapshot_files")
        .filter(F.col("relative_path") == relative_path)
        .select("run_id", "volume_path", "sha256")
    )

    current_rows = spark.read.table("bronze_bls_metadata").join(
        inventory,
        ["volume_path", "sha256"],
        "inner",
    )

    return current_rows.select(
        "run_id",
        *[
            F.col("attributes").getItem(source).alias(target)
            for source, target in columns.items()
        ],
    )


@dp.materialized_view(
    name="silver_series",
    comment="Current BLS series definitions with readable metadata labels."
)
@dp.expect_all_or_fail({
    "series_id_present": (
        "series_id IS NOT NULL AND length(trim(series_id)) > 0"
    ),
    "unique_series": "series_key_count = 1",
    "sector_label_present": (
        "sector_name IS NOT NULL AND length(trim(sector_name)) > 0"
    ),
    "class_label_present": (
        "class_text IS NOT NULL AND length(trim(class_text)) > 0"
    ),
    "measure_label_present": (
        "measure_text IS NOT NULL AND length(trim(measure_text)) > 0"
    ),
    "duration_label_present": (
        "duration_text IS NOT NULL AND length(trim(duration_text)) > 0"
    ),
    "seasonal_label_present": (
        "seasonal_text IS NOT NULL AND length(trim(seasonal_text)) > 0"
    ),
})
def silver_series():
    series = current_metadata(
        "bls/pr.series",
        {
            "series_id": "series_id",
            "sector_code": "sector_code",
            "class_code": "class_code",
            "measure_code": "measure_code",
            "duration_code": "duration_code",
            "seasonal": "seasonal_code",
            "base_year": "base_year",
        },
    )

    lookup_specs = [
        (
            "bls/pr.sector",
            "sector_code",
            "sector_name",
        ),
        (
            "bls/pr.class",
            "class_code",
            "class_text",
        ),
        (
            "bls/pr.measure",
            "measure_code",
            "measure_text",
        ),
        (
            "bls/pr.duration",
            "duration_code",
            "duration_text",
        ),
        (
            "bls/pr.seasonal",
            "seasonal_code",
            "seasonal_text",
        ),
    ]

    enriched = series

    for path, key, label in lookup_specs:
        lookup = current_metadata(
            path,
            {key: key, label: label},
        )
        enriched = enriched.join(
            lookup,
            ["run_id", key],
            "left",
        )

    return (
        enriched
        .withColumn(
            "series_label",
            F.concat_ws(
                " | ",
                "sector_name",
                "class_text",
                "measure_text",
                "duration_text",
                "seasonal_text",
            ),
        )
        .withColumn(
            "series_key_count",
            F.count(F.lit(1)).over(
                Window.partitionBy("run_id", "series_id")
            ),
        )
    )