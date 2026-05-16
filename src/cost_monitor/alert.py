# Databricks notebook source
# COMMAND ----------
"""
Cost Monitor: Tracks DBU consumption per pipeline run via Databricks REST API.
Logs results to a Delta table and alerts on idle cluster spend.
"""

# COMMAND ----------
import requests
import json
from datetime import datetime
from pyspark.sql import Row

WORKSPACE_URL = "https://adb-7405605123541603.3.azuredatabricks.net"
CLUSTER_ID = "0515-023655-enc70ake"
COST_LOG_PATH = "abfss://bronze@ghlakehousestorage.dfs.core.windows.net/ops/cost_log"
IDLE_DBU_THRESHOLD = 0.5
ALERT_EMAIL = "tapan.dev@outlook.com"

# COMMAND ----------
def get_databricks_token() -> str:
    """Fetch PAT token from Databricks secret scope."""
    return dbutils.secrets.get(scope="gh-lakehouse-scope", key="databricks-pat")

# COMMAND ----------
def get_cluster_state(token: str) -> dict:
    """
    Fetch current cluster state from Databricks REST API.
    Returns cluster state and last activity info.
    """
    response = requests.get(
        f"{WORKSPACE_URL}/api/2.0/clusters/get",
        headers={"Authorization": f"Bearer {token}"},
        params={"cluster_id": CLUSTER_ID}
    )
    response.raise_for_status()
    return response.json()

# COMMAND ----------
def get_recent_pipeline_runs(token: str) -> list:
    """
    Fetch recent DLT pipeline run history to estimate DBU consumption.
    """
    response = requests.get(
        f"{WORKSPACE_URL}/api/2.0/pipelines",
        headers={"Authorization": f"Bearer {token}"}
    )
    response.raise_for_status()
    return response.json().get("statuses", [])

# COMMAND ----------
def estimate_dbu(cluster_state: dict) -> float:
    """
    Estimate DBU consumption based on cluster size and uptime.
    DC4as_v5 single node = ~2 DBU/hour.
    """
    if cluster_state.get("state") in ("RUNNING", "RESIZING"):
        num_workers = cluster_state.get("num_workers", 0)
        # Single node = driver only = ~2 DBU/hr, +1 DBU per worker
        return round(2.0 + (num_workers * 1.0), 2)
    return 0.0

# COMMAND ----------
def log_cost(cluster_state: dict, dbu_estimate: float, queries_run: int) -> None:
    """
    Append cost log entry to Delta table.
    """
    is_idle = queries_run == 0 and dbu_estimate > IDLE_DBU_THRESHOLD
    
    row = Row(
        run_timestamp=datetime.utcnow().isoformat(),
        cluster_id=CLUSTER_ID,
        cluster_state=cluster_state.get("state", "UNKNOWN"),
        dbu_estimate=dbu_estimate,
        queries_run=queries_run,
        idle=is_idle
    )

    df = spark.createDataFrame([row])
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .save(COST_LOG_PATH))

    if is_idle:
        print(f"ALERT: Cluster {CLUSTER_ID} is idle but consuming {dbu_estimate} DBU/hr. Consider terminating.")
    else:
        print(f"Cluster state: {cluster_state.get('state')} | DBU estimate: {dbu_estimate} | Queries run: {queries_run}")

# COMMAND ----------
try:
    token = get_databricks_token()
    cluster_state = get_cluster_state(token)
    dbu_estimate = estimate_dbu(cluster_state)
    
    # queries_run placeholder — replace with actual query count from system tables when available
    queries_run = 0
    
    log_cost(cluster_state, dbu_estimate, queries_run)
    print(f"Cost log written to: {COST_LOG_PATH}")

except Exception as e:
    print(f"Cost monitor error: {e}")
    raise