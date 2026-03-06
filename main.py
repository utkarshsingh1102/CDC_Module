"""
main.py — Example pipeline entry point for the CDC Module.

Demonstrates:
  - Building a SparkSession with AQE settings
  - Creating sample prev/curr DataFrames
  - Running CDC comparison
  - Displaying results
"""

from config import build_spark_session, CDC_FLAG_COL, LOAD_TIMESTAMP_COL
from cdc_module import run_cdc


def main():
    spark = build_spark_session(app_name="CDC_Pipeline_Demo")

    # ── Sample previous snapshot ─────────────────────────────────────────────
    df_prev = spark.createDataFrame(
        [
            (1, "Alice", "Engineering", 80000),
            (2, "Bob",   "Marketing",   60000),
            (3, "Carol", "Engineering", 75000),
        ],
        ["emp_id", "name", "dept", "salary"],
    )

    # ── Sample current incremental data ─────────────────────────────────────
    # emp_id=1 : salary raise          → Update
    # emp_id=2 : no changes            → No Change
    # emp_id=3 : absent (soft delete — not handled here, use df_prev left anti join)
    # emp_id=4 : brand new employee    → Insert
    df_curr = spark.createDataFrame(
        [
            (1, "Alice", "Engineering", 95000),
            (2, "Bob",   "Marketing",   60000),
            (4, "Dave",  "Sales",       55000),
        ],
        ["emp_id", "name", "dept", "salary"],
    )

    key_cols = ["emp_id"]

    # ── Run CDC ──────────────────────────────────────────────────────────────
    df_new, df_updated, df_no_change = run_cdc(
        df_prev,
        df_curr,
        key_cols,
        broadcast_prev=False,  # set True if df_prev fits in memory
    )

    # ── Show results ─────────────────────────────────────────────────────────
    print("\n===== INSERTS (cdc_flag = 'I') =====")
    df_new.show(truncate=False)

    print("\n===== UPDATES (cdc_flag = 'U') =====")
    df_updated.show(truncate=False)

    print("\n===== NO CHANGE (cdc_flag = 'NC') =====")
    df_no_change.show(truncate=False)

    # ── Combine all outputs into a single audit table ────────────────────────
    df_audit = df_new.unionByName(df_updated).unionByName(df_no_change)
    print("\n===== FULL AUDIT TABLE =====")
    df_audit.orderBy("emp_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
