from pyspark import pipelines as dp
from pyspark.sql import functions as F

EXPECTED_HEADER = (
    "series_id|year|period|value|footnote_codes"
)


@dp.table(
    name="bronze_bls_observations",
    comment="BLS AllData observations parsed once per immutable file version."
)
@dp.expect_all_or_fail({
    "expected_header": (
        f"source_header = '{EXPECTED_HEADER}'"
    ),
    "expected_field_count": "field_count = 5",
    "series_id_present": (
        "series_id IS NOT NULL AND length(series_id) > 0"
    ),
    "year_present": (
        "year_raw IS NOT NULL AND length(year_raw) > 0"
    ),
    "period_present": (
        "period IS NOT NULL AND length(period) > 0"
    ),
    "value_present": (
        "value_raw IS NOT NULL AND length(value_raw) > 0"
    ),
})
def bronze_bls_observations():
    source = (
        spark.readStream.table("bronze_source_files")
        .filter(F.col("relative_path") == "bls/pr.data.1.AllData")
        .select(
            "volume_path",
            "sha256",
            F.split(
                F.decode("content", "UTF-8"),
                r"\r?\n",
                -1,
            ).alias("lines"),
        )
        .withColumn(
            "source_header",
            F.concat_ws(
                "|",
                F.transform(
                    F.split(F.get("lines", 0), "\t", -1),
                    lambda field: F.trim(field),
                ),
            ),
        )
    )

    lines = (
        source.select(
            "volume_path",
            "sha256",
            "source_header",
            F.posexplode("lines").alias("line_index", "raw_line"),
        )
        .filter(
            (F.col("line_index") > 0)
            & (F.length(F.trim("raw_line")) > 0)
        )
        .withColumn("fields", F.split("raw_line", "\t", -1))
    )

    return lines.select(
        "volume_path",
        "sha256",
        "source_header",
        (F.col("line_index") + 1).alias("source_line_number"),
        "raw_line",
        F.size("fields").alias("field_count"),
        F.trim(F.get("fields", 0)).alias("series_id"),
        F.trim(F.get("fields", 1)).alias("year_raw"),
        F.trim(F.get("fields", 2)).alias("period"),
        F.trim(F.get("fields", 3)).alias("value_raw"),
        F.trim(F.get("fields", 4)).alias("footnote_codes"),
    )