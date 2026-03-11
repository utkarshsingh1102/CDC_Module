"""
main.py — Example pipeline entry point for the CDC Module.

Demonstrates:
  - Building a SparkSession with AQE settings
  - Creating sample prev/curr DataFrames (or loading from CSV)
  - Running CDC comparison
  - Displaying results

Usage:
  # Use built-in sample data
  python main.py

  # Use your own CSV files
  python main.py --prev path/to/prev.csv --curr path/to/curr.csv --keys emp_id

  # Multiple key columns
  python main.py --prev prev.csv --curr curr.csv --keys emp_id dept
"""

import argparse

from config import build_spark_session, CDC_FLAG_COL, LOAD_TIMESTAMP_COL
from cdc_module import run_cdc


def parse_args():
    parser = argparse.ArgumentParser(description="Run CDC pipeline on two snapshots.")
    parser.add_argument("--prev", type=str, default=None, help="Path to previous snapshot CSV")
    parser.add_argument("--curr", type=str, default=None, help="Path to current snapshot CSV")
    parser.add_argument("--keys", nargs="+", default=["emp_id"], help="Key column(s) for CDC join")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark_session(app_name="CDC_Pipeline_Demo")

    if args.prev and args.curr:
        # ── Load from user-supplied CSV files ────────────────────────────────
        df_prev = spark.read.csv(args.prev, header=True, inferSchema=True)
        df_curr = spark.read.csv(args.curr, header=True, inferSchema=True)
        key_cols = args.keys
        print(f"Loaded prev CSV: {args.prev}  ({df_prev.count()} rows)")
        print(f"Loaded curr CSV: {args.curr}  ({df_curr.count()} rows)")
    else:
        # ── Sample previous snapshot (10 rows x 15 columns) ──────────────────
        # Columns: emp_id, first_name, last_name, dept, role, location, salary,
        #          bonus, age, years_exp, manager_id, team_size, rating, status, email
        df_prev = spark.createDataFrame(
            [
                (1,  "Alice",   "Smith",   "Engineering", "Senior SWE",  "New York",    95000, 10000, 34, 10, 101, 5,  4.5, "active",   "alice@corp.com"),
                (2,  "Bob",     "Jones",   "Marketing",   "Manager",     "Chicago",     80000,  8000, 42, 18, 102, 8,  4.2, "active",   "bob@corp.com"),
                (3,  "Carol",   "White",   "Engineering", "Lead SWE",    "San Jose",    110000, 12000, 38, 14, 101, 7,  4.8, "active",   "carol@corp.com"),
                (4,  "Dave",    "Brown",   "Sales",       "Executive",   "Boston",      70000,  15000, 29, 5,  103, 3,  3.9, "active",   "dave@corp.com"),
                (5,  "Eve",     "Davis",   "HR",          "Specialist",  "Austin",      65000,  5000, 31, 7,  104, 2,  4.1, "active",   "eve@corp.com"),
                (6,  "Frank",   "Miller",  "Finance",     "Analyst",     "Seattle",     72000,  6000, 27, 3,  105, 4,  3.7, "active",   "frank@corp.com"),
                (7,  "Grace",   "Wilson",  "Engineering", "Junior SWE",  "Denver",      78000,  4000, 25, 2,  101, 5,  3.5, "active",   "grace@corp.com"),
                (8,  "Hank",    "Moore",   "Operations",  "Director",    "Miami",       120000, 20000, 50, 25, 106, 12, 4.6, "active",   "hank@corp.com"),
                (9,  "Iris",    "Taylor",  "Marketing",   "Coordinator", "Phoenix",     58000,  3000, 28, 4,  102, 3,  4.0, "active",   "iris@corp.com"),
                (10, "Jake",    "Anderson","Engineering", "Architect",   "Los Angeles", 130000, 18000, 45, 20, 101, 10, 4.9, "active",   "jake@corp.com"),
            ],
            ["emp_id", "first_name", "last_name", "dept", "role", "location", "salary",
             "bonus", "age", "years_exp", "manager_id", "team_size", "rating", "status", "email"],
        )

        # ── Sample current incremental data (10 rows x 15 columns) ───────────
        # emp_id=1  : salary + bonus raise             → Update
        # emp_id=2  : no changes                       → No Change
        # emp_id=3  : promoted, location changed       → Update
        # emp_id=4  : no changes                       → No Change
        # emp_id=5  : status changed to inactive       → Update
        # emp_id=6  : no changes                       → No Change
        # emp_id=7  : rating improved                  → Update
        # emp_id=8  : no changes                       → No Change
        # emp_id=9  : no changes                       → No Change
        # emp_id=11 : brand new employee               → Insert
        df_curr = spark.createDataFrame(
            [
                (1,  "Alice",   "Smith",   "Engineering", "Senior SWE",  "New York",    105000, 12000, 34, 10, 101, 5,  4.5, "active",   "alice@corp.com"),
                (2,  "Bob",     "Jones",   "Marketing",   "Manager",     "Chicago",     80000,  8000, 42, 18, 102, 8,  4.2, "active",   "bob@corp.com"),
                (3,  "Carol",   "White",   "Engineering", "Principal SWE","Remote",     125000, 15000, 38, 14, 101, 9,  4.8, "active",   "carol@corp.com"),
                (4,  "Dave",    "Brown",   "Sales",       "Executive",   "Boston",      70000,  15000, 29, 5,  103, 3,  3.9, "active",   "dave@corp.com"),
                (5,  "Eve",     "Davis",   "HR",          "Specialist",  "Austin",      65000,  5000, 31, 7,  104, 2,  4.1, "inactive", "eve@corp.com"),
                (6,  "Frank",   "Miller",  "Finance",     "Analyst",     "Seattle",     72000,  6000, 27, 3,  105, 4,  3.7, "active",   "frank@corp.com"),
                (7,  "Grace",   "Wilson",  "Engineering", "Junior SWE",  "Denver",      78000,  4000, 25, 2,  101, 5,  4.3, "active",   "grace@corp.com"),
                (8,  "Hank",    "Moore",   "Operations",  "Director",    "Miami",       120000, 20000, 50, 25, 106, 12, 4.6, "active",   "hank@corp.com"),
                (9,  "Iris",    "Taylor",  "Marketing",   "Coordinator", "Phoenix",     58000,  3000, 28, 4,  102, 3,  4.0, "active",   "iris@corp.com"),
                (11, "Lara",    "Chen",    "Engineering", "SWE",         "New York",    85000,  5000, 26, 2,  101, 5,  3.8, "active",   "lara@corp.com"),
            ],
            ["emp_id", "first_name", "last_name", "dept", "role", "location", "salary",
             "bonus", "age", "years_exp", "manager_id", "team_size", "rating", "status", "email"],
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
