"""
cdc_module.py — Core Change Data Capture logic.

Compares df_prev (previous snapshot) with df_curr (current incremental data)
on key_cols and returns three classified DataFrames:
  - df_new       : new records          (cdc_flag = "I")
  - df_updated   : changed records      (cdc_flag = "U")
  - df_no_change : unchanged records    (cdc_flag = "NC")

All outputs contain a load_timestamp metadata column.

Strategy:
  Hash-based CDC — computes a SHA-256 hash of all non-key columns for both
  df_prev and df_curr, then joins on key columns and compares a single hash
  value instead of individual columns. Only key columns + hash are read from
  df_prev, minimising shuffle size regardless of table width.
"""

from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import (
    CDC_FLAG_INSERT, CDC_FLAG_UPDATE, CDC_FLAG_NO_CHANGE,
    CDC_FLAG_COL, LOAD_TIMESTAMP_COL,
    PREV_HASH_COL, CURR_HASH_COL,
)
from utils import validate_inputs, get_non_key_cols, compute_row_hash


def run_cdc(
    df_prev: DataFrame,
    df_curr: DataFrame,
    key_cols: List[str],
    broadcast_prev: bool = False,
    broadcast_curr: bool = False,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Run hash-based CDC comparison between df_prev and df_curr.

    Parameters
    ----------
    df_prev        : Previous snapshot DataFrame.
    df_curr        : Current incremental DataFrame.
    key_cols       : List of business key column names.
    broadcast_prev : Hint Spark to broadcast df_prev (use when df_prev is small).
    broadcast_curr : Hint Spark to broadcast df_curr (use when df_curr is small).

    Returns
    -------
    (df_new, df_updated, df_no_change)
    """
    validate_inputs(df_prev, df_curr, key_cols)

    non_key_cols = get_non_key_cols(df_curr, key_cols)

    # Compute SHA-256 hash of all non-key columns
    # df_prev: keep only key cols + hash — reduces shuffle size significantly
    prev_hashed = (
        compute_row_hash(df_prev, non_key_cols)
        .select(key_cols + ["row_hash"])
        .withColumnRenamed("row_hash", PREV_HASH_COL)
    )

    # df_curr: keep full row + hash (needed for output columns)
    curr_hashed = (
        compute_row_hash(df_curr, non_key_cols)
        .withColumnRenamed("row_hash", CURR_HASH_COL)
    )

    # Optionally apply broadcast hints
    if broadcast_prev:
        prev_hashed = F.broadcast(prev_hashed)
    if broadcast_curr:
        curr_hashed = F.broadcast(curr_hashed)

    # Join on key columns — Spark deduplicates key cols when joining with a list
    # One shuffle, but prev side carries only key + hash (not full row)
    joined = prev_hashed.join(curr_hashed, on=key_cols, how="full")

    load_ts = F.current_timestamp().alias(LOAD_TIMESTAMP_COL)
    output_cols = [F.col(c) for c in key_cols + non_key_cols]

    # ── Inserts: prev hash IS NULL (record only exists in curr) ─────────────
    df_new = (
        joined.filter(F.col(PREV_HASH_COL).isNull())
        .select(*output_cols, F.lit(CDC_FLAG_INSERT).alias(CDC_FLAG_COL), load_ts)
    )

    # ── Updates: both exist + hashes differ ─────────────────────────────────
    if non_key_cols:
        update_filter = (
            F.col(PREV_HASH_COL).isNotNull()
            & F.col(CURR_HASH_COL).isNotNull()
            & (F.col(PREV_HASH_COL) != F.col(CURR_HASH_COL))
        )
        unchanged_filter = F.col(PREV_HASH_COL) == F.col(CURR_HASH_COL)
    else:
        # No non-key columns — nothing can change, so updates are impossible
        update_filter = F.lit(False)
        unchanged_filter = (
            F.col(PREV_HASH_COL).isNotNull() & F.col(CURR_HASH_COL).isNotNull()
        )

    df_updated = (
        joined.filter(update_filter)
        .select(*output_cols, F.lit(CDC_FLAG_UPDATE).alias(CDC_FLAG_COL), load_ts)
    )

    # ── No Change: hashes match ──────────────────────────────────────────────
    df_no_change = (
        joined.filter(unchanged_filter)
        .select(*output_cols, F.lit(CDC_FLAG_NO_CHANGE).alias(CDC_FLAG_COL), load_ts)
    )

    return df_new, df_updated, df_no_change
