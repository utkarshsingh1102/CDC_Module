# PySpark CDC Module

A Change Data Capture (CDC) module built with PySpark that compares a previous snapshot (`df_prev`) with current incremental data (`df_curr`) and classifies records as **Insert**, **Update**, or **No Change**.

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
