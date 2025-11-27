# test_json_extract.py

import importlib
import pytest
import math
from numbers import Number
import polars as pl

from zarr_fuse.airflow.json_extract import json_extract

# ---------- Helper: compare Polars DataFrames without pl.testing ----------

def assert_pl_equal(left: pl.DataFrame, right: pl.DataFrame, *, rel=1e-9, abs_=1e-12):
    """
    Compare two Polars DataFrames:
      - same columns in the same order
      - same number of rows
      - per-cell equality with numeric tolerance for floats
    """
    assert isinstance(left, pl.DataFrame)
    assert isinstance(right, pl.DataFrame)

    assert left.columns == right.columns
    assert left.height == right.height

    lrows = left.rows()
    rrows = right.rows()

    for i, (lr, rr) in enumerate(zip(lrows, rrows)):
        assert len(lr) == len(rr)
        for j, (x, y) in enumerate(zip(lr, rr)):
            if x is None or y is None:
                assert x is None and y is None
                continue
            # numeric tolerance
            if isinstance(x, Number) and isinstance(y, Number):
                if isinstance(x, bool) or isinstance(y, bool):
                    # treat bools as non-floats
                    assert x == y
                else:
                    assert math.isclose(float(x), float(y), rel_tol=rel, abs_tol=abs_)
            else:
                assert x == y

def test_extract_without_placeholders():
    # Pattern has no placeholders; expect exactly one row if paths resolve.
    data = {
        "the-key": [
            {
                "foo": {"t": "2024-01-01T00:00:00Z", "v": 1.23},
                "bar": {"t": "2024-01-01T01:00:00Z", "v": 4.56},
            }
        ]
    }

    pattern = "/the-key/0/foo"  # no placeholders
    columns = {
        "time": "/the-key/0/foo/t",
        "group": "/the-key/0/foo/t",  # just reusing a path to show non-placeholder column works
        "value": "/the-key/0/foo/v",
    }

    out = json_extract(data, pattern, columns)

    expected = pl.DataFrame(
        {
            "time": ["2024-01-01T00:00:00Z"],
            "group": ["2024-01-01T00:00:00Z"],
            "value": [1.23],
        }
    )

    assert_pl_equal(out, expected)


def test_extract_with_placeholders():
    # Pattern expands over list indices and dict keys; columns use both paths and literal placeholders.
    data = {
        "the-key": [
            {
                "foo": {"t": "2024-01-01T00:00:00Z", "v": 1.23},
                "bar": {"t": "2024-01-01T01:00:00Z", "v": 4.56},
            },
            {
                "foo": {"t": "2024-01-02T00:00:00Z", "v": 7.89},
            },
        ]
    }

    pattern = "/the-key/{idx}/{key2}"
    columns = {
        "time": "/the-key/{idx}/{key2}/t",
        "group": "{key2}",
        "value": "/the-key/{idx}/{key2}/v",
        "idx": "{idx}",  # unused column; just to show literal placeholder works
    }

    out = json_extract(data, pattern, columns)

    # Construct expected in the traversal order:
    # - list index 0: dict iteration order 'foo', then 'bar'
    # - list index 1: dict key 'foo'
    expected = pl.DataFrame(
        {
            "time": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-02T00:00:00Z",
            ],
            "group": ["foo", "bar", "foo"],
            "value": [1.23, 4.56, 7.89],
            "idx": ['0', '0', '1'],
        }
    )

    assert_pl_equal(out, expected)


def test_extract_with_escaped_paths_separate_only():
    """
    Separate test for escaped-character paths.
    This test is ONLY executed if the implementation supports escaping (e.g., exposes _tokenize_path).
    Otherwise it is skipped. Do not use escaped paths in other tests.
    """

    # Example that requires escape handling:
    # pattern '/key//{{1}}/{any}' should match dictionary under the literal key 'key/{1}'
    data = {
        "key/{1}": {
            "A": {"t": "2025-01-01T00:00:00Z", "v": 1},
            "B": {"t": "2025-01-01T01:00:00Z", "v": 2},
        }
    }

    pattern = "/key//{{1}}/{any}"
    columns = {
        "time": "/key//{{1}}/{any}/t",
        "group": "{any}",
        "value": "/key//{{1}}/{any}/v",
    }

    out = json_extract(data, pattern, columns)

    expected = pl.DataFrame(
        {
            "time": ["2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"],
            "group": ["A", "B"],
            "value": [1, 2],
        }
    )

    assert_pl_equal(out, expected)
