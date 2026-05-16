# Databricks notebook source

# COMMAND ---------- 

"""
Silver layer: DLT pipeline that cleans and flattens Bronze GH Archive events.
Applies data quality expectations and filters invalid records.
"""

import dlt
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import DataFrame

from imports import BRONZE_PATH, SILVER_TABLE, VALID_EVENT_TYPES


@dlt.table(
    name=SILVER_TABLE,
    comment="Cleaned and flattened GitHub Archive events with validated fields.",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true",
    },
)
@dlt.expect("valid_event_id", "id IS NOT NULL")
@dlt.expect_or_drop(
    "valid_event_type",
    f"event_type IN ({', '.join(repr(e) for e in VALID_EVENT_TYPES)})",
)
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
def silver_gh_events() -> DataFrame:
    """
    Read from Bronze Delta path, flatten nested structs,
    parse timestamps, and apply quality constraints.
    """
    return (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .select(
            col("id"),
            col("type").alias("event_type"),
            col("actor.login").alias("actor_login"),
            col("actor.id").alias("actor_id"),
            col("repo.name").alias("repo_name"),
            col("repo.id").alias("repo_id"),
            to_timestamp(col("created_at")).alias("event_time"),
            col("org.login").alias("org_login"),
            col("public"),
            col("_ingested_at"),
            col("_source_file"),
        )
        .filter(col("event_type").isNotNull())
        .filter(to_timestamp(col("created_at")).isNotNull())
    )