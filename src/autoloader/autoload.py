# Databricks notebook source

# COMMAND ----------
environment_param = dbutils.widgets.get("environment")
domain_name = dbutils.widgets.get("domain_name")
data_product_name = dbutils.widgets.get("data_product_name")
catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")
table_name = dbutils.widgets.get("table_name")
# COMMAND ----------

base_dir = f"/Volumes/{catalog_name}/{bronze_schema_name}/autoload_source/{table_name}"
schema_dir = f"/Volumes/{catalog_name}/{bronze_schema_name}/autoload_schema/{table_name}/schema"
checkpoint_dir = f"/Volumes/{catalog_name}/{bronze_schema_name}/autoload_schema/{table_name}/checkpoint"

autoloaded_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", schema_dir)
    .load(base_dir)
)
# COMMAND ----------
autoloaded_df.writeStream.queryName(f"{bronze_schema_name}_{table_name}_autoload").option(
    "mergeSchema", "true"
).option("checkpointLocation", checkpoint_dir).trigger(availableNow=True).table(
    f"{catalog_name}.{bronze_schema_name}.{table_name}"
)