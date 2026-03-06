"""
test_cdc.py — Unit tests for the CDC module.

Run with:  python -m pytest tests/test_cdc.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from config import CDC_FLAG_INSERT, CDC_FLAG_UPDATE, CDC_FLAG_NO_CHANGE, CDC_FLAG_COL
from cdc_module import run_cdc
from tests.sample_data import (
    fixture_inserts,
    fixture_mixed,
    fixture_nulls,
    fixture_composite_key,
)


def rows_as_set(df, cols):
    """Return a set of tuples for the given columns (for order-independent comparison)."""
    return set(df.select(cols).rdd.map(tuple).collect())


# ── Test 1: Pure inserts ─────────────────────────────────────────────────────
class TestInserts:
    def setup_method(self):
        df_prev, df_curr, key_cols = fixture_inserts()
        self.df_new, self.df_updated, self.df_no_change = run_cdc(df_prev, df_curr, key_cols)

    def test_insert_count(self):
        assert self.df_new.count() == 2

    def test_insert_flag(self):
        flags = {r[CDC_FLAG_COL] for r in self.df_new.select(CDC_FLAG_COL).collect()}
        assert flags == {CDC_FLAG_INSERT}

    def test_update_empty(self):
        assert self.df_updated.count() == 0

    def test_no_change_empty(self):
        assert self.df_no_change.count() == 0

    def test_insert_ids(self):
        assert rows_as_set(self.df_new, ["id"]) == {(1,), (2,)}


# ── Test 2: Mixed scenario ───────────────────────────────────────────────────
class TestMixed:
    def setup_method(self):
        df_prev, df_curr, key_cols = fixture_mixed()
        self.df_new, self.df_updated, self.df_no_change = run_cdc(df_prev, df_curr, key_cols)

    def test_insert_count(self):
        assert self.df_new.count() == 1

    def test_insert_id(self):
        assert rows_as_set(self.df_new, ["id"]) == {(3,)}

    def test_update_count(self):
        assert self.df_updated.count() == 1

    def test_update_id(self):
        assert rows_as_set(self.df_updated, ["id"]) == {(2,)}

    def test_update_salary(self):
        row = self.df_updated.select("salary").first()
        assert row["salary"] == 75000

    def test_no_change_count(self):
        assert self.df_no_change.count() == 1

    def test_no_change_id(self):
        assert rows_as_set(self.df_no_change, ["id"]) == {(1,)}

    def test_flags(self):
        assert rows_as_set(self.df_new, [CDC_FLAG_COL]) == {(CDC_FLAG_INSERT,)}
        assert rows_as_set(self.df_updated, [CDC_FLAG_COL]) == {(CDC_FLAG_UPDATE,)}
        assert rows_as_set(self.df_no_change, [CDC_FLAG_COL]) == {(CDC_FLAG_NO_CHANGE,)}


# ── Test 3: NULL handling ────────────────────────────────────────────────────
class TestNulls:
    def setup_method(self):
        df_prev, df_curr, key_cols = fixture_nulls()
        self.df_new, self.df_updated, self.df_no_change = run_cdc(df_prev, df_curr, key_cols)

    def test_null_to_value_is_update(self):
        assert self.df_updated.count() == 1
        assert rows_as_set(self.df_updated, ["id"]) == {(1,)}

    def test_null_to_null_is_no_change(self):
        assert self.df_no_change.count() == 1
        assert rows_as_set(self.df_no_change, ["id"]) == {(2,)}

    def test_no_inserts(self):
        assert self.df_new.count() == 0


# ── Test 4: Composite key ────────────────────────────────────────────────────
class TestCompositeKey:
    def setup_method(self):
        df_prev, df_curr, key_cols = fixture_composite_key()
        self.df_new, self.df_updated, self.df_no_change = run_cdc(df_prev, df_curr, key_cols)

    def test_insert_count(self):
        assert self.df_new.count() == 1

    def test_insert_keys(self):
        assert rows_as_set(self.df_new, ["dept_id", "emp_id"]) == {(2, 1)}

    def test_update_count(self):
        assert self.df_updated.count() == 1

    def test_update_keys(self):
        assert rows_as_set(self.df_updated, ["dept_id", "emp_id"]) == {(1, 1)}

    def test_no_change_count(self):
        assert self.df_no_change.count() == 1

    def test_no_change_keys(self):
        assert rows_as_set(self.df_no_change, ["dept_id", "emp_id"]) == {(1, 2)}


# ── Test 5: Validation errors ────────────────────────────────────────────────
class TestValidation:
    def setup_method(self):
        from tests.sample_data import get_spark
        spark = get_spark()
        self.df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

    def test_missing_key_col(self):
        with pytest.raises(ValueError, match="missing key columns"):
            run_cdc(self.df, self.df, ["nonexistent_col"])

    def test_empty_key_cols(self):
        with pytest.raises(ValueError, match="non-empty"):
            run_cdc(self.df, self.df, [])

    def test_non_dataframe_input(self):
        with pytest.raises(ValueError, match="DataFrame"):
            run_cdc("not_a_df", self.df, ["id"])
