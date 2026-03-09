from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config import ROW_HASH_COL


def validate_inputs(df_prev: DataFrame, df_curr: DataFrame, key_cols: List[str]) -> None:
    """
    Validate that inputs are non-empty DataFrames with the required key columns.
    Raises ValueError on any validation failure.
    """
    if not isinstance(df_prev, DataFrame):
        raise ValueError("df_prev must be a Spark DataFrame.")
    if not isinstance(df_curr, DataFrame):
        raise ValueError("df_curr must be a Spark DataFrame.")
    if not key_cols:
        raise ValueError("key_cols must be a non-empty list.")

    prev_cols = set(df_prev.columns)
    curr_cols = set(df_curr.columns)

    missing_prev = [c for c in key_cols if c not in prev_cols]
    missing_curr = [c for c in key_cols if c not in curr_cols]

    if missing_prev:
        raise ValueError(f"df_prev is missing key columns: {missing_prev}")
    if missing_curr:
        raise ValueError(f"df_curr is missing key columns: {missing_curr}")


def get_non_key_cols(df: DataFrame, key_cols: List[str]) -> List[str]:
    """Return all columns in df that are not key columns."""
    return [c for c in df.columns if c not in key_cols]


def compute_row_hash(df: DataFrame, non_key_cols: List[str]) -> DataFrame:
    """
    Add a SHA-256 hash column of all non-key columns to the DataFrame.
    NULLs are represented as '__NULL__' to distinguish them from empty strings.
    """
    hash_input = [
        F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
        for c in non_key_cols
    ]
    return df.withColumn(ROW_HASH_COL, F.sha2(F.concat_ws("|", *hash_input), 256))
