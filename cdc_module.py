"""
cdc_module.py — Core Change Data Capture logic.

Compares df_prev (previous snapshot) with df_curr (current incremental data)
on key_cols and returns three classified DataFrames:
  - df_new       : new records          (cdc_flag = "I")
  - df_updated   : changed records      (cdc_flag = "U")
  - df_no_change : unchanged records    (cdc_flag = "NC")

All outputs contain a load_timestamp metadata column.

Strategy:
  Single full outer join (one shuffle) with prefixed aliases to avoid column
  collisions, followed by filter-based classification using eqNullSafe for
  correct NULL handling.
"""

from typing import List, Tuple, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from config import (
    CDC_FLAG_INSERT, CDC_FLAG_UPDATE, CDC_FLAG_NO_CHANGE,
    CDC_FLAG_COL, LOAD_TIMESTAMP_COL,
    PREV_PREFIX, CURR_PREFIX,
)
from utils import validate_inputs, get_non_key_cols, add_prefix


def _build_join_condition(key_cols: List[str]) -> Column:
    """Build a conjunctive join condition on key columns across prev/curr aliases."""
    condition = F.col(f"{PREV_PREFIX}{key_cols[0]}").eqNullSafe(F.col(f"{CURR_PREFIX}{key_cols[0]}"))
    for k in key_cols[1:]:
        condition = condition & F.col(f"{PREV_PREFIX}{k}").eqNullSafe(F.col(f"{CURR_PREFIX}{k}"))
    return condition


def _build_changed_condition(non_key_cols: List[str]) -> Column:
    """
    Returns a condition that is True when at least one non-key column differs
    between prev and curr (NULL-safe comparison).
    """
    condition = ~F.col(f"{PREV_PREFIX}{non_key_cols[0]}").eqNullSafe(F.col(f"{CURR_PREFIX}{non_key_cols[0]}"))
    for c in non_key_cols[1:]:
        condition = condition | ~F.col(f"{PREV_PREFIX}{c}").eqNullSafe(F.col(f"{CURR_PREFIX}{c}"))
    return condition


def _build_unchanged_condition(non_key_cols: List[str]) -> Column:
    """
    Returns a condition that is True when all non-key columns are equal
    between prev and curr (NULL-safe comparison).
    """
    condition = F.col(f"{PREV_PREFIX}{non_key_cols[0]}").eqNullSafe(F.col(f"{CURR_PREFIX}{non_key_cols[0]}"))
    for c in non_key_cols[1:]:
        condition = condition & F.col(f"{PREV_PREFIX}{c}").eqNullSafe(F.col(f"{CURR_PREFIX}{c}"))
    return condition


def _select_output_cols(
    joined: DataFrame,
    key_cols: List[str],
    non_key_cols: List[str],
    use_curr: bool,
) -> List[Column]:
    """
    Build the final SELECT expression from the joined DataFrame.
    - Key columns are taken from curr (inserts/updates) or prev (no-change).
    - Non-key columns follow the same side preference.
    """
    prefix = CURR_PREFIX if use_curr else PREV_PREFIX
    output = [F.col(f"{prefix}{c}").alias(c) for c in key_cols + non_key_cols]
    return output


def run_cdc(
    df_prev: DataFrame,
    df_curr: DataFrame,
    key_cols: List[str],
    broadcast_prev: bool = False,
    broadcast_curr: bool = False,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Run CDC comparison between df_prev and df_curr.

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

    # Prefix all columns to avoid ambiguity after join
    prev_prefixed = add_prefix(df_prev, PREV_PREFIX)
    curr_prefixed = add_prefix(df_curr, CURR_PREFIX)

    # Optionally apply broadcast hints
    if broadcast_prev:
        prev_prefixed = F.broadcast(prev_prefixed)
    if broadcast_curr:
        curr_prefixed = F.broadcast(curr_prefixed)

    join_condition = _build_join_condition(key_cols)

    # Single full outer join — one shuffle
    joined = prev_prefixed.join(curr_prefixed, on=join_condition, how="full")

    load_ts = F.current_timestamp().alias(LOAD_TIMESTAMP_COL)

    # ── Inserts: prev key IS NULL (record only exists in curr) ──────────────
    insert_filter = F.col(f"{PREV_PREFIX}{key_cols[0]}").isNull()
    df_new = (
        joined.filter(insert_filter)
        .select(
            *_select_output_cols(joined, key_cols, non_key_cols, use_curr=True),
            F.lit(CDC_FLAG_INSERT).alias(CDC_FLAG_COL),
            load_ts,
        )
    )

    # ── Updates: both keys exist + at least one non-key col differs ─────────
    both_exist = (
        F.col(f"{PREV_PREFIX}{key_cols[0]}").isNotNull()
        & F.col(f"{CURR_PREFIX}{key_cols[0]}").isNotNull()
    )

    if non_key_cols:
        changed_condition = _build_changed_condition(non_key_cols)
        update_filter = both_exist & changed_condition
        unchanged_filter = both_exist & _build_unchanged_condition(non_key_cols)
    else:
        # No non-key columns — nothing can change, so updates are impossible
        update_filter = F.lit(False)
        unchanged_filter = both_exist

    df_updated = (
        joined.filter(update_filter)
        .select(
            *_select_output_cols(joined, key_cols, non_key_cols, use_curr=True),
            F.lit(CDC_FLAG_UPDATE).alias(CDC_FLAG_COL),
            load_ts,
        )
    )

    # ── No Change: both keys exist + all non-key cols match ─────────────────
    df_no_change = (
        joined.filter(unchanged_filter)
        .select(
            *_select_output_cols(joined, key_cols, non_key_cols, use_curr=True),
            F.lit(CDC_FLAG_NO_CHANGE).alias(CDC_FLAG_COL),
            load_ts,
        )
    )

    return df_new, df_updated, df_no_change
