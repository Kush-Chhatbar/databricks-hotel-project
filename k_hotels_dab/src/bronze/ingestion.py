import re

RAW_BASE_PATH = "/Volumes/hotel_project/raw_data/raw_files"
BRONZE_BASE_PATH = "/Volumes/hotel_project/bronze/bronzevolume"

def validate_source(source: str):
    """Validate the source name before using it in a Volume path."""
    if not source or not re.fullmatch(r"[A-Za-z0-9_-]+", source):
        raise ValueError(f"Invalid source name: {source!r}")


def get_source_paths(source: str):
    """Build the raw, schema, checkpoint and Delta output paths."""
    validate_source(source)
    return {
        "raw_path": f"{RAW_BASE_PATH}/{source}/",
        "schema_path": f"{BRONZE_BASE_PATH}/{source}/schema/",
        "checkpoint_path": f"{BRONZE_BASE_PATH}/{source}/checkpoints/",
        "bronze_path": f"{BRONZE_BASE_PATH}/{source}/data/",
    }


def ingest_to_bronze(spark, source: str):
    """Incrementally ingest CSV files from a source into a Bronze Delta location."""
    paths = get_source_paths(source)
    source_df = (
        spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", paths["schema_path"])\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load(paths["raw_path"])
    )
    query = (
        source_df.writeStream\
        .format("delta")\
        .outputMode("append")\
        .trigger(once=True)\
        .option("checkpointLocation", paths["checkpoint_path"])\
        .option("path", paths["bronze_path"])\
        .start()
    )

    return query