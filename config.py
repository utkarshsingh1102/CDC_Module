from pyspark.sql import SparkSession

# CDC flag constants
CDC_FLAG_INSERT = "I"
CDC_FLAG_UPDATE = "U"
CDC_FLAG_NO_CHANGE = "NC"
CDC_FLAG_DELETE = "D"

# Column name constants
LOAD_TIMESTAMP_COL = "load_timestamp"
CDC_FLAG_COL = "cdc_flag"

# Hash column constants
ROW_HASH_COL  = "row_hash"
PREV_HASH_COL = "prev_row_hash"
CURR_HASH_COL = "curr_row_hash"

# Broadcast join size threshold (bytes) — 10 MB default
BROADCAST_THRESHOLD_BYTES = 10 * 1024 * 1024


def build_spark_session(app_name: str = "CDC_Module", enable_aqe: bool = True) -> SparkSession:
    """Build and return a SparkSession with recommended CDC settings."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Adaptive Query Execution (Spark 3.x)
        .config("spark.sql.adaptive.enabled", str(enable_aqe).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(enable_aqe).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(enable_aqe).lower())
        # Broadcast join threshold
        .config("spark.sql.autoBroadcastJoinThreshold", str(BROADCAST_THRESHOLD_BYTES))
        # Null-safe comparisons
        .config("spark.sql.ansi.enabled", "false")
    )
    return builder.getOrCreate()
