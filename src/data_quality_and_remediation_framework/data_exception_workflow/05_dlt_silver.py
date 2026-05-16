# Databricks notebook source
!pip install databricks-labs-dqx

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
import dlt
from pyspark.sql import functions as F


# COMMAND ----------

environment_param = spark.conf.get("dltparam.environment")
catalog_name = spark.conf.get("dltparam.catalog_name")
silver_schema_name = spark.conf.get("dltparam.silver_schema_name")
bronze_schema_name = spark.conf.get("dltparam.bronze_schema_name")
watermark_delay = spark.conf.get("dltparam.watermark_delay", "1 day")

# COMMAND ----------

ws = WorkspaceClient()
dq_engine = DQEngine(spark=spark, workspace_client=ws)

# COMMAND ----------

@dlt.table(
    name="silver_fincrime_customers_validated",
    comment="Records that pass all SSN DQ rules."
)
def silver_fincrime_customers_validated():
    return dq_engine.get_valid(
        dlt.read_stream(f"{catalog_name}.{bronze_schema_name}.bronze_ssn_dq_check")
    )


# COMMAND ----------

dlt.create_streaming_table(
    name="silver_fincrime_customers_scd2",
    comment="SCD2 historical system of record built from validated fincrime customer records."
)

dlt.apply_changes(
    target="silver_fincrime_customers_scd2",
    source="silver_fincrime_customers_validated",
    keys=["customer_id"],
    sequence_by=F.col("event_ts"),
    stored_as_scd_type="2",
    track_history_column_list=[
        "customer_name",
        "ssn",
        "account_balance",
        "source_system",
        "ssn_norm",
    ]
)

