# Databricks notebook source

# COMMAND ---------- 

"""
Gold layer: DLT pipeline that produces aggregated analytics tables
from Silver GH Archive events.

Tables produced:
- gold_top_repos: top 20 repos by total events per hour
- gold_event_volume: event counts by type and hour
"""

import dlt
from pyspark.sql.functions import (
    col,
    date_trunc,
    count,
    sum as spark_sum,
    when,
    dense_rank,
)
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from imports import SILVER_TABLE, GOLD_TOP_REPOS_TABLE, GOLD_EVENT_VOLUME_TABLE


def read_silver() -> DataFrame:
    """Read from Silver DLT table."""
    return dlt.read(SILVER_TABLE)


@dlt.table(
    name=GOLD_TOP_REPOS_TABLE,
    comment="Top 20 repositories by total event volume per hour.",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true",
    },
)
def gold_top_repos() -> DataFrame:
    """
    Aggregate event counts per repo per hour.
    Rank by total events and return top 20 per hour.
    """
    silver = read_silver()

    aggregated = (
        silver.withColumn("event_hour", date_trunc("hour", col("event_time")))
        .groupBy("event_hour", "repo_name")
        .agg(
            spark_sum(when(col("event_type") == "WatchEvent", 1).otherwise(0)).alias(
                "watch_count"
            ),
            spark_sum(when(col("event_type") == "ForkEvent", 1).otherwise(0)).alias(
                "fork_count"
            ),
            spark_sum(when(col("event_type") == "PushEvent", 1).otherwise(0)).alias(
                "push_count"
            ),
            count("*").alias("total_events"),
        )
    )

    window = Window.partitionBy("event_hour").orderBy(col("total_events").desc())

    return (
        aggregated.withColumn("rank", dense_rank().over(window))
        .filter(col("rank") <= 20)
        .drop("rank")
    )


@dlt.table(
    name=GOLD_EVENT_VOLUME_TABLE,
    comment="Event counts by type and hour across all GitHub Archive data.",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true",
    },
)
def gold_event_volume() -> DataFrame:
    """
    Aggregate total event counts by event type and hour.
    Powers the event trend dashboard tile.
    """
    return (
        read_silver()
        .withColumn("event_hour", date_trunc("hour", col("event_time")))
        .groupBy("event_hour", "event_type")
        .agg(count("*").alias("event_count"))
        .orderBy("event_hour", "event_type")
    )