# Databricks notebook source
# MAGIC %md
# MAGIC This notebook would be a part of a job to create an initial raw seed.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# COMMAND ----------
environment_param = dbutils.widgets.get("environment")
domain_name = dbutils.widgets.get("domain_name")
data_product_name = dbutils.widgets.get("data_product_name")
catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")


# COMMAND ----------

raw_table_name = f"{catalog_name}.{bronze_schema_name}.fincrime_customers_raw"

# COMMAND ----------

SEED_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("ssn", StringType(), True),
    StructField("account_balance", DoubleType(), True),
])

seed_rows = [
    ("C001", "Alice",   "123-45-6789", 15000.00),
    ("C002", "Bob",     None,           500.00),
    ("C003", "Charlie", "12-345-6789",  22000.00),
    ("C004", "Diana",   "912-70-1234",  5000.00),
    ("C005", None,      "123456789",    12000.00),
    ("C006", "Eve",     None,           3000.00),
    ("C007", "Frank",   None,           8000.00),
]

seed_df = (
    spark.createDataFrame(seed_rows, schema=SEED_SCHEMA)
        .withColumn("record_id", F.expr("uuid()"))
        .withColumn("source_system", F.lit("seed"))
        .withColumn("event_ts", F.current_timestamp())
)

if not spark.catalog.tableExists(raw_table_name):
    seed_df.write.format("delta").saveAsTable(raw_table_name)
else:
    seed_df.write.mode("append").format("delta").saveAsTable(raw_table_name)
