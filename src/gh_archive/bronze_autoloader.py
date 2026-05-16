"""
Bronze layer: Auto Loader ingestion from raw GH Archive files into Delta.
Reads JSON files from the raw container and writes to bronze as Delta,
adding metadata columns for ingestion tracking.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

from imports import (
    RAW_PATH,
    BRONZE_PATH,
    BRONZE_CHECKPOINT,
    BRONZE_SCHEMA_LOCATION,
)

spark = SparkSession.builder.getOrCreate()


def get_bronze_stream():
    """
    Configure Auto Loader stream from raw container.
    Uses schema inference with rescue column for schema evolution.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(RAW_PATH)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


def write_bronze(stream_df):
    """
    Write stream to Bronze Delta table with checkpoint.
    """
    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )


if __name__ == "__main__":
    stream = get_bronze_stream()
    query = write_bronze(stream)
    query.awaitTermination()
    print(f"Bronze ingestion complete. Written to: {BRONZE_PATH}")