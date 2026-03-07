# PySpark CDC Module

A Change Data Capture (CDC) module built with PySpark that compares a previous snapshot (`df_prev`) with current incremental data (`df_curr`) and classifies records as **Insert**, **Update**, or **No Change**.

---

## Complete Workflow

### Step 1 — Build SparkSession (`config.py`)

`build_spark_session()` is called first. It creates a Spark session with:
- **AQE (Adaptive Query Execution)** enabled — Spark dynamically optimizes the join at runtime
- **Skew join handling** — prevents one partition from being overloaded with data
- **Broadcast threshold** set to 10 MB — small DataFrames are automatically broadcast

```
build_spark_session()
        │
        ▼
SparkSession with AQE + skew join + broadcast settings
```

---

### Step 2 — Validate Inputs (`utils.py → validate_inputs`)

Before any processing, the module checks:
- Both `df_prev` and `df_curr` are valid Spark DataFrames
- `key_cols` is a non-empty list
- All specified key columns actually exist in both DataFrames

If any check fails, a `ValueError` is raised immediately — no wasted compute.

```
validate_inputs(df_prev, df_curr, key_cols)
        │
        ├── Is df_prev a DataFrame?
        ├── Is df_curr a DataFrame?
        ├── Is key_cols non-empty?
        ├── Do key columns exist in df_prev?
        └── Do key columns exist in df_curr?
```

---

### Step 3 — Identify Non-Key Columns (`utils.py → get_non_key_cols`)

All columns that are not key columns are extracted from `df_curr`. These are the columns that will be compared to detect changes.

```
df_curr.columns = ["emp_id", "name", "dept", "salary"]
key_cols        = ["emp_id"]
non_key_cols    = ["name", "dept", "salary"]   ← these get compared
```

---

### Step 4 — Prefix All Columns (`utils.py → add_prefix`)

Both DataFrames have all their columns renamed with a prefix to avoid column name collisions after the join.

```
df_prev columns:  emp_id, name, dept, salary
                        ↓ add_prefix("prev__")
                  prev__emp_id, prev__name, prev__dept, prev__salary

df_curr columns:  emp_id, name, dept, salary
                        ↓ add_prefix("curr__")
                  curr__emp_id, curr__name, curr__dept, curr__salary
```

---

### Step 5 — Full Outer Join (`cdc_module.py → _build_join_condition`)

A single **full outer join** is performed between the two prefixed DataFrames on the key columns using `eqNullSafe` (so NULL keys are handled correctly).

```
prev_prefixed  FULL OUTER JOIN  curr_prefixed
       ON  prev__emp_id <=> curr__emp_id

Result (joined DataFrame):
┌──────────────┬───────────────┬──────────────┬───────────────┐
│ prev__emp_id │ prev__salary  │ curr__emp_id │ curr__salary  │
├──────────────┼───────────────┼──────────────┼───────────────┤
│ 1            │ 80000         │ 1            │ 95000         │  ← both sides present
│ 2            │ 60000         │ 2            │ 60000         │  ← both sides present
│ 3            │ 75000         │ NULL         │ NULL          │  ← only in prev (delete)
│ NULL         │ NULL          │ 4            │ 55000         │  ← only in curr (insert)
└──────────────┴───────────────┴──────────────┴───────────────┘
```

This is done in **one shuffle** — the most expensive Spark operation — keeping the pipeline efficient.

---

### Step 6 — Classify Records by Filtering the Joined Result

The joined DataFrame is filtered three times to produce three output DataFrames:

**Inserts** — `prev__key IS NULL` (record only exists in curr)
```
insert_filter = prev__emp_id IS NULL
→ df_new  (cdc_flag = "I")
```

**Updates** — both keys exist AND at least one non-key column differs
```
update_filter = prev__emp_id IS NOT NULL
              AND curr__emp_id IS NOT NULL
              AND (prev__salary <=> curr__salary = FALSE   ← _build_changed_condition
                  OR prev__name <=> curr__name = FALSE
                  OR ...)
→ df_updated  (cdc_flag = "U")
```

**No Change** — both keys exist AND all non-key columns match
```
unchanged_filter = prev__emp_id IS NOT NULL
                 AND curr__emp_id IS NOT NULL
                 AND prev__salary <=> curr__salary        ← _build_unchanged_condition
                 AND prev__name   <=> curr__name
                 AND ...
→ df_no_change  (cdc_flag = "NC")
```

---

### Step 7 — Select Output Columns and Add Metadata

For each classified DataFrame, the prefixed column names are stripped back to their original names and two metadata columns are added:

```
curr__emp_id  →  emp_id
curr__salary  →  salary
+ cdc_flag       = "I" / "U" / "NC"
+ load_timestamp = current timestamp of the CDC run
```

---

### End-to-End Data Flow

```
df_prev (previous snapshot)          df_curr (current data)
         │                                    │
         └──────────── validate ──────────────┘
                            │
                     add column prefixes
                    (prev__*, curr__*)
                            │
                    FULL OUTER JOIN
                   (single shuffle)
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         prev IS NULL   both exist    both exist
              │        + col changed  + col same
              ▼             ▼             ▼
           df_new      df_updated   df_no_change
         (flag = I)   (flag = U)   (flag = NC)
              │             │             │
              └─────────────┴─────────────┘
                            │
                      df_audit table
                  (unionByName all three)
```

---

## How It Works

The module performs a single **full outer join** on the specified business key columns, then classifies each record using filters:

| Condition | Classification | `cdc_flag` |
|---|---|---|
| Key only in `df_curr` | New record | `I` |
| Key in both, non-key columns changed | Updated record | `U` |
| Key in both, all non-key columns match | Unchanged record | `NC` |

- Uses `eqNullSafe()` for correct NULL handling (NULL == NULL is treated as equal)
- Single shuffle (one join) for efficiency
- Supports AQE, broadcast hints, and skew join handling (Spark 3.x)

---

## Project Structure

```
CDC_Module/
├── config.py              # SparkSession builder, AQE settings, CDC constants
├── utils.py               # Input validation and column helpers
├── cdc_module.py          # Core CDC logic (run_cdc function)
├── tests/
│   ├── sample_data.py     # Test fixtures
│   └── test_cdc.py        # Unit tests
└── main.py                # Example pipeline entry point
```

---

## Requirements

- Python 3.8+
- PySpark 3.x
- pytest (for running tests)

Install dependencies:

```bash
pip install pyspark pytest
```

---

## Usage

### Basic

```python
from config import build_spark_session
from cdc_module import run_cdc

spark = build_spark_session(app_name="MyCDCPipeline")

df_prev = spark.read.parquet("path/to/previous/snapshot")
df_curr = spark.read.parquet("path/to/current/data")

df_new, df_updated, df_no_change = run_cdc(df_prev, df_curr, key_cols=["emp_id"])

df_new.show()       # cdc_flag = "I"
df_updated.show()   # cdc_flag = "U"
df_no_change.show() # cdc_flag = "NC"
```

### Composite Keys

```python
df_new, df_updated, df_no_change = run_cdc(
    df_prev, df_curr, key_cols=["dept_id", "emp_id"]
)
```

### Broadcast Hint (when one DataFrame is small)

```python
df_new, df_updated, df_no_change = run_cdc(
    df_prev, df_curr, key_cols=["id"], broadcast_prev=True
)
```

### Combined Audit Table

```python
df_audit = df_new.unionByName(df_updated).unionByName(df_no_change)
df_audit.write.partitionBy("cdc_flag").parquet("path/to/output")
```

---

## Output Schema

All three output DataFrames share the same schema as the input, plus two additional columns:

| Column | Type | Description |
|---|---|---|
| `cdc_flag` | String | `I`, `U`, or `NC` |
| `load_timestamp` | Timestamp | Time the CDC run was executed |

---

## Running the Demo

```bash
python main.py
```

## Running Tests

```bash
python -m pytest tests/test_cdc.py -v
```

### Test Coverage

| Test Class | Scenario |
|---|---|
| `TestInserts` | All records are new inserts |
| `TestMixed` | Mix of inserts, updates, and no-changes |
| `TestNulls` | NULL values in non-key columns |
| `TestCompositeKey` | Multi-column business keys |
| `TestValidation` | Invalid inputs raise `ValueError` |

---

## Configuration

`config.py` exposes settings you can tune:

| Setting | Default | Description |
|---|---|---|
| `BROADCAST_THRESHOLD_BYTES` | 10 MB | Spark auto-broadcast threshold |
| `enable_aqe` | `True` | Adaptive Query Execution (Spark 3.x) |
| `CDC_FLAG_INSERT` | `"I"` | Flag value for inserts |
| `CDC_FLAG_UPDATE` | `"U"` | Flag value for updates |
| `CDC_FLAG_NO_CHANGE` | `"NC"` | Flag value for no-change records |
