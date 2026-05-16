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
bronze_schema_name = spark.conf.get("dltparam.bronze_schema_name")
watermark_delay = spark.conf.get("dltparam.watermark_delay", "1 day")

# COMMAND ----------

checks = [
    {
        "check": {"function": "is_not_null", "arguments": {"column": "ssn_norm"}},
        "name": "SSN_MISSING",
        "criticality": "error",
    },
    {
        "check": {
            "function": "sql_expression",
            "arguments": {
                "expression": """
                    ssn_norm IS NULL OR
                    ssn_norm RLIKE '^(?!000|666|9[0-9][0-9])[0-9]{3}(?!00)[0-9]{2}(?!0000)[0-9]{4}$'
                """
            },
        },
        "name": "SSN_INVALID_STRUCTURE",
        "criticality": "error",
        "user_metadata": {
            "rule_type": "format_validation",
            "business_description": "SSN must be 9 digits and cannot contain invalid area, group, or serial numbers.",
            "remediation_hint": "Ensure SSN is normalized to 9 digits and does not contain restricted prefixes (000, 666, 9xx) or zero group/serial segments.",
        },
    },
    {
        "check": {
            "function": "sql_expression",
            "arguments": {
                "expression": """
                    ssn_norm IS NULL OR
                    NOT (ssn_norm RLIKE '^9[0-9]{2}(7[0-9]|8[0-9]|9[0-2]|9[4-9])[0-9]{4}$')
                """
            },
        },
        "name": "ITIN_SUPPLIED_INSTEAD_OF_SSN",
        "criticality": "warn",
        "user_metadata": {
            "rule_type": "identifier_classification",
            "business_description": "ITIN detected where SSN was expected.",
            "remediation_hint": "Confirm whether taxpayer provided an ITIN instead of SSN. Route to alternate compliance workflow.",
        },
    },
]

# COMMAND ----------

ws = WorkspaceClient()
dq_engine = DQEngine(spark=spark, workspace_client=ws)

# COMMAND ----------

raw_table_name = f"{catalog_name}.{bronze_schema_name}.fincrime_customers_raw"


@dlt.table(
    name="fincrime_customers_raw_stream",
    comment="Raw source stream (append-only Delta table)",
)
def fincrime_customers_raw_stream():
    return spark.readStream.table(raw_table_name).withWatermark(
        "event_ts", watermark_delay
    )

# COMMAND ----------

def normalize_ssn(df):
    ssn_trim = F.trim(F.col("ssn"))

    # Only allow digits, dash, space
    is_clean_input = ssn_trim.rlike(r"^[0-9\-\s]+$")

    ssn_digits = F.regexp_replace(ssn_trim, r"[\-\s]", "")

    return df.withColumn(
        "ssn_norm",
        F.when(
            (ssn_trim.isNull()) | (ssn_trim == ""),
            None
        ).when(
            is_clean_input,
            ssn_digits
        ).otherwise(None)  # force invalid garbage to fail DQX
    )


# COMMAND ----------
@dlt.table(
    name="bronze_fincrime_customers",
    comment="Bronze ingestion + SSN standardization flags"
)
def bronze_fincrime_customers():
    return (dlt.read_stream("fincrime_customers_raw_stream").transform(normalize_ssn))

# COMMAND ----------

@dlt.table(
    name="bronze_ssn_dq_check",
    comment="DQX validated bronze"
)
def bronze_ssn_dq_check():
    return dq_engine.apply_checks_by_metadata(
        dlt.read_stream("bronze_fincrime_customers"),
        checks
    )

