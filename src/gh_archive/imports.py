"""
Shared constants and imports for the GitHub Archive lakehouse pipeline.
"""

# Storage account
STORAGE_ACCOUNT = "ghlakehousestorage"
STORAGE_BASE = f"abfss://{{container}}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Container paths
RAW_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/gh_events"
SILVER_TABLE = "silver_gh_events"
GOLD_TOP_REPOS_TABLE = "gold_top_repos"
GOLD_EVENT_VOLUME_TABLE = "gold_event_volume"

# Cost monitor
COST_LOG_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/ops/cost_log"

# Schema
BRONZE_SCHEMA_HINTS = {
    "id": "string",
    "type": "string",
    "created_at": "string",
    "public": "boolean",
}

# Allowed event types for Silver validation
VALID_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "WatchEvent",
    "ForkEvent",
    "IssuesEvent",
    "CreateEvent",
    "DeleteEvent",
    "IssueCommentEvent",
]

# Checkpoint locations
BRONZE_CHECKPOINT = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/gh_events"
BRONZE_SCHEMA_LOCATION = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_schema/gh_events"