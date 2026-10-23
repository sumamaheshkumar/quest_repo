from pyspark import pipelines as dp
from pyspark.sql import functions as F

EXPECTED_HEADERS = {
    "bls/pr.series": (
        "series_id|sector_code|class_code|measure_code|duration_code|"
        "seasonal|base_year|footnote_codes|begin_year|begin_period|"
        "end_year|end_period"
    ),
    "bls/pr.sector": (
        "sector_code|sector_name|display_level|selectable|sort_sequence"
    ),
    "bls/pr.class": (
        "class_code|class_text|display_level|selectable|sort_sequence"
    ),
    "bls/pr.measure": (
        "measure_code|measure_text|display_level|selectable|sort_sequence"
    ),
    "bls/pr.duration": (
        "duration_code|duration_text|display_level|selectable|sort_sequence"
    ),
    "bls/pr.seasonal": "seasonal_code|seasonal_text",
    "bls/pr.period": "period|period_abbr|period_name",
}


@dp.table(
    name="bronze_bls_metadata",
    comment="Versioned BLS metadata rows with normalized field names."
)
@dp.expect_all_or_fail({
    "expected_header": "source_header = expected_header",
    "matching_field_count": "field_count = header_field_count",
    "metadata_present": "attributes IS NOT NULL AND size(attributes) > 0",
})
def bronze_bls_metadata():
    expected_header_map = F.create_map(
        *[
            F.lit(item)
            for pair in EXPECTED_HEADERS.items()
            for item in pair
        ]
    )

    documents = (
        spark.readStream.table("bronze_source_files")
        .filter(F.col("relative_path").isin(list(EXPECTED_HEADERS)))
        .select(
            "volume_path",
            "sha256",
            "relative_path",
            F.split(
                F.decode("content", "UTF-8"),
                r"\r?\n",
                -1,
            ).alias("lines"),
        )
        .withColumn(
            "header_fields",
            F.transform(
                F.split(F.get("lines", 0), "\t", -1),
                lambda field: F.lower(F.trim(field)),
            ),
        )
        .withColumn(
            "source_header",
            F.concat_ws("|", "header_fields"),
        )
        .withColumn(
            "expected_header",
            F.element_at(expected_header_map, F.col("relative_path")),
        )
    )

    rows = (
        documents.select(
            "volume_path",
            "sha256",
            "relative_path",
            "header_fields",
            "source_header",
            "expected_header",
            F.posexplode("lines").alias("line_index", "raw_line"),
        )
        .filter(
            (F.col("line_index") > 0)
            & (F.length(F.trim("raw_line")) > 0)
        )
        .withColumn(
            "fields",
            F.transform(
                F.split("raw_line", "\t", -1),
                lambda field: F.trim(field),
            ),
        )
    )

    return rows.select(
        "volume_path",
        "sha256",
        "relative_path",
        (F.col("line_index") + 1).alias("source_line_number"),
        "raw_line",
        "source_header",
        "expected_header",
        F.size("fields").alias("field_count"),
        F.size("header_fields").alias("header_field_count"),
        F.map_from_arrays("header_fields", "fields").alias("attributes"),
    )