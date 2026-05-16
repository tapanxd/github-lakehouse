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
quality_schema_name = spark.conf.get("dltparam.quality_schema_name")
bronze_schema_name = spark.conf.get("dltparam.bronze_schema_name") 
watermark_delay = spark.conf.get("dltparam.watermark_delay", "1 day")

# COMMAND ----------

ws = WorkspaceClient()
dq_engine = DQEngine(spark=spark, workspace_client=ws)

# COMMAND ----------
@dlt.table(
    name="silver_fincrime_customers_data_exception",
    comment="Records failing SSN DQ rules. Explicitly retained."
)
def silver_fincrime_customers_data_exception():
    return (
        dq_engine
        .get_invalid(dlt.read_stream(f"{catalog_name}.{bronze_schema_name}.bronze_ssn_dq_check"))
        .withColumn("exception_ts", F.current_timestamp())
        .withColumn("review_status", F.lit("PENDING"))
    )
# COMMAND ----------
dlt.create_streaming_table(
    name="silver_fincrime_customers_data_exception_scd",
    comment="SCD2 data exception historical system of record built from validated fincrime customer records."
)

dlt.apply_changes(
    target="silver_fincrime_customers_data_exception_scd",
    source="silver_fincrime_customers_data_exception",
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
# COMMAND ----------
@dlt.table(
    name="silver_fincrime_customers_latest_data_exception",
    comment="Current records from SCD2 table where END_AT is null."
)
def silver_fincrime_customers_latest_data_exception():
    return dlt.read("silver_fincrime_customers_data_exception_scd").filter(F.col("__END_AT").isNull())