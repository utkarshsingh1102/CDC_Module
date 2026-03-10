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

### Step 4 — Compute SHA-256 Row Hash (`utils.py → compute_row_hash`)

A SHA-256 hash is computed over all non-key columns for both DataFrames. NULLs are replaced with `"__NULL__"` before hashing so that NULL vs NULL is correctly treated as equal.

```
df_prev row:  emp_id=1, name="Alice", dept="Engineering", salary=80000
                        ↓ SHA-256( "Alice|Engineering|80000" )
              prev_row_hash = "a3f9..."

df_curr row:  emp_id=1, name="Alice", dept="Engineering", salary=95000
                        ↓ SHA-256( "Alice|Engineering|95000" )
              curr_row_hash = "b72c..."   ← different hash → Update detected
```

`df_prev` is then slimmed to **key columns + hash only** — no full row data needed for the join.

---

### Step 5 — Full Outer Join on Key Columns

A single **full outer join** is performed on the key columns. Because `df_prev` carries only the hash (not all columns), the data shuffled across the network is minimal regardless of how wide the table is.

```
prev_slim (key + prev_row_hash)  FULL OUTER JOIN  curr_hashed (all cols + curr_row_hash)
                          ON  emp_id

Result (joined DataFrame):
┌────────┬──────────────┬─────┬──────────────────┐
│ emp_id │ prev_row_hash│ ... │ curr_row_hash     │
├────────┼──────────────┼─────┼──────────────────┤
│ 1      │ "a3f9..."    │ ... │ "b72c..."         │  ← hashes differ → Update
│ 2      │ "c841..."    │ ... │ "c841..."         │  ← hashes match  → No Change
│ 3      │ "e10f..."    │ ... │ NULL              │  ← only in prev  → Delete (not handled)
│ 4      │ NULL         │ ... │ "f55a..."         │  ← only in curr  → Insert
└────────┴──────────────┴─────┴──────────────────┘
```

---

### Step 6 — Classify Records by Comparing Hashes

The joined DataFrame is filtered three times using a **single hash column comparison** instead of N individual column conditions:

**Inserts** — `prev_row_hash IS NULL` (record only exists in curr)
```
insert_filter = prev_row_hash IS NULL
→ df_new  (cdc_flag = "I")
```

**Updates** — both hashes exist AND they differ
```
update_filter = prev_row_hash IS NOT NULL
              AND curr_row_hash IS NOT NULL
              AND prev_row_hash != curr_row_hash
→ df_updated  (cdc_flag = "U")
```

**No Change** — hashes are equal
```
unchanged_filter = prev_row_hash == curr_row_hash
→ df_no_change  (cdc_flag = "NC")
```

---

### Step 7 — Select Output Columns and Add Metadata

For each classified DataFrame, the curr columns are selected (hash columns dropped) and two metadata columns are added:

```
emp_id, name, dept, salary   ← original columns from curr
+ cdc_flag                   = "I" / "U" / "NC"
+ load_timestamp             = current timestamp of the CDC run
```

---

### End-to-End Data Flow

```
df_prev (previous snapshot)          df_curr (current data)
         │                                    │
         └──────────── validate ──────────────┘
                            │
              ┌─────────────┴──────────────┐
              ▼                            ▼
   compute SHA-256 hash          compute SHA-256 hash
   slim to key + hash only       keep full row + hash
   (prev_row_hash)               (curr_row_hash)
              │                            │
              └──── FULL OUTER JOIN ────────┘
                     on key columns
                    (single shuffle,
                     prev side is tiny)
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
    prev_hash IS NULL   hashes differ  hashes match
              │             │             │
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

The module computes a **SHA-256 hash** of all non-key columns for each row, then performs a single full outer join on key columns and compares only the hash values to classify changes:

| Condition | Classification | `cdc_flag` |
|---|---|---|
| `prev_row_hash` IS NULL | New record | `I` |
| Both hashes exist and differ | Updated record | `U` |
| Both hashes are equal | Unchanged record | `NC` |

- SHA-256 hashing collapses N-column comparison into a single hash comparison
- `df_prev` is slimmed to key columns + hash only before the join — minimal shuffle
- NULLs are handled correctly (`NULL` → `"__NULL__"` before hashing)
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

## Setup & Installation

### Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.11+ | Must be the actual binary, not a shell alias |
| Java (JDK) | 11+ | Required by Apache Spark |
| PySpark | 4.x | Installed via pip |
| pytest | Any | For running tests only |

---

### Step 1 — Install Java

Spark requires Java to run. If Java is not installed:

**macOS (Homebrew):**
```bash
brew install openjdk
```

After installation, add Java to your PATH. Homebrew will print the exact command — it will look like:
```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
```

Verify:
```bash
java -version
```

**Ubuntu / Debian:**
```bash
sudo apt update && sudo apt install -y openjdk-17-jdk
```

**Windows:**
Download and install from https://adoptium.net and ensure `JAVA_HOME` is set.

---

### Step 2 — Install Python 3.11+

**macOS:**
```bash
brew install python@3.11
```

**Ubuntu:**
```bash
sudo apt install -y python3.11 python3.11-pip
```

**Windows:**
Download from https://www.python.org/downloads/

Verify:
```bash
python3.11 --version
```

---

### Step 3 — Clone the Repository

```bash
git clone <repository-url>
cd CDC_Module
```

---

### Step 4 — Install Python Dependencies

```bash
pip install pyspark pytest
```

---

### Step 5 — Set Environment Variables

This is **required** — Spark's internal worker process must use the same Python that has PySpark installed.

**macOS / Linux:**
```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"          # Java (macOS only)
export PYSPARK_PYTHON=/opt/homebrew/bin/python3.11         # macOS
# or
export PYSPARK_PYTHON=/usr/bin/python3.11                  # Linux
```

To make these permanent, add them to your `~/.zshrc` or `~/.bashrc`:
```bash
echo 'export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"' >> ~/.zshrc
echo 'export PYSPARK_PYTHON=/opt/homebrew/bin/python3.11' >> ~/.zshrc
source ~/.zshrc
```

**Windows (PowerShell):**
```powershell
$env:PYSPARK_PYTHON = "C:\Python311\python.exe"
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17"
```

---

## Running the Demo

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
export PYSPARK_PYTHON=/opt/homebrew/bin/python3.11
python3.11 main.py
```

Expected output:
```
===== INSERTS (cdc_flag = 'I') =====
+------+----+-----+------+--------+-------------------+
|emp_id|name|dept |salary|cdc_flag|load_timestamp     |
+------+----+-----+------+--------+-------------------+
|4     |Dave|Sales|55000 |I       |2024-01-10 ...     |
+------+----+-----+------+--------+-------------------+

===== UPDATES (cdc_flag = 'U') =====
+------+-----+-----------+------+--------+-------------------+
|emp_id|name |dept       |salary|cdc_flag|load_timestamp     |
+------+-----+-----------+------+--------+-------------------+
|1     |Alice|Engineering|95000 |U       |2024-01-10 ...     |
+------+-----+-----------+------+--------+-------------------+

===== NO CHANGE (cdc_flag = 'NC') =====
+------+---+---------+------+--------+-------------------+
|emp_id|name|dept    |salary|cdc_flag|load_timestamp     |
+------+---+---------+------+--------+-------------------+
|2     |Bob|Marketing|60000 |NC      |2024-01-10 ...     |
+------+---+---------+------+--------+-------------------+
```

---

## Running Tests

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
export PYSPARK_PYTHON=/opt/homebrew/bin/python3.11
python3.11 -m pytest tests/test_cdc.py -v
```

Expected output:
```
25 passed in ~11s
```

---

## Requirements

- Python 3.11+
- Java JDK 11+
- PySpark 4.x
- pytest (for running tests only)

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

## Test Coverage

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
