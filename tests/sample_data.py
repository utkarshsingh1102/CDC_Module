"""
sample_data.py — Test fixtures for CDC unit tests.

Each fixture returns (df_prev, df_curr, key_cols) ready for run_cdc().
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def get_spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("CDC_Test").getOrCreate()


# ── Fixture 1: Pure inserts ──────────────────────────────────────────────────
def fixture_inserts():
    """
    df_prev is empty; df_curr has 2 rows — both should be classified as Insert.
    """
    spark = get_spark()
    schema = ["id", "name", "salary"]

    df_prev = spark.createDataFrame([], schema=spark.createDataFrame(
        [("x", "placeholder", 0)], schema
    ).schema)

    df_curr = spark.createDataFrame(
        [(1, "Alice", 50000), (2, "Bob", 60000)],
        schema,
    )
    return df_prev, df_curr, ["id"]


# ── Fixture 2: Mixed insert / update / no-change ─────────────────────────────
def fixture_mixed():
    """
    id=1 : salary unchanged  → NC
    id=2 : salary changed    → U
    id=3 : new record        → I
    """
    spark = get_spark()
    schema = ["id", "name", "salary"]

    df_prev = spark.createDataFrame(
        [(1, "Alice", 50000), (2, "Bob", 60000)],
        schema,
    )
    df_curr = spark.createDataFrame(
        [(1, "Alice", 50000), (2, "Bob", 75000), (3, "Carol", 55000)],
        schema,
    )
    return df_prev, df_curr, ["id"]


# ── Fixture 3: NULL values in non-key columns ────────────────────────────────
def fixture_nulls():
    """
    id=1 : salary was NULL, now 50000  → U
    id=2 : salary NULL in both         → NC
    """
    spark = get_spark()
    schema = ["id", "name", "salary"]

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("salary", IntegerType(), True),
    ])
    df_prev = spark.createDataFrame(
        [(1, "Alice", None), (2, "Bob", None)], schema=schema
    )
    df_curr = spark.createDataFrame(
        [(1, "Alice", 50000), (2, "Bob", None)], schema=schema
    )
    return df_prev, df_curr, ["id"]


# ── Fixture 4: Composite key ─────────────────────────────────────────────────
def fixture_composite_key():
    """
    Keys: (dept_id, emp_id)
    (1,1): name changed → U
    (1,2): unchanged    → NC
    (2,1): new record   → I
    """
    spark = get_spark()
    schema = ["dept_id", "emp_id", "name"]

    df_prev = spark.createDataFrame(
        [(1, 1, "Alice"), (1, 2, "Bob")],
        schema,
    )
    df_curr = spark.createDataFrame(
        [(1, 1, "Alicia"), (1, 2, "Bob"), (2, 1, "Carol")],
        schema,
    )
    return df_prev, df_curr, ["dept_id", "emp_id"]
