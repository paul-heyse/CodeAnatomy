"""Unit tests for value coercion helpers."""

import pyarrow as pa
import pytest

from datafusion_engine.arrow.coercion import coerce_to_recordbatch_reader
from utils.value_coercion import (
    CoercionError,
    coerce_bool,
    coerce_float,
    coerce_int,
    coerce_mapping_list,
    coerce_str,
    coerce_str_list,
    coerce_str_tuple,
    raise_for_bool,
    raise_for_float,
    raise_for_int,
    raise_for_str,
)

INT_SAMPLE = 3
INT_FLOAT_SAMPLE = 3.7
INT_STRING_SAMPLE = " 42 "
INT_STRING_EXPECTED = 42
FLOAT_SAMPLE = 1.5
FLOAT_STRING_SAMPLE = " 2.5 "
FLOAT_STRING_EXPECTED = 2.5
RAISE_INT_SAMPLE = "5"
RAISE_INT_EXPECTED = 5
RAISE_FLOAT_SAMPLE = "1.25"
RAISE_FLOAT_EXPECTED = 1.25


def test_coerce_int() -> None:
    """Return expected values for int coercion."""
    assert coerce_int(None) is None
    assert coerce_int(value=True) is None
    assert coerce_int(INT_SAMPLE) == INT_SAMPLE
    assert coerce_int(INT_FLOAT_SAMPLE) == INT_SAMPLE
    assert coerce_int(INT_STRING_SAMPLE) == INT_STRING_EXPECTED
    assert coerce_int("abc") is None


def test_coerce_float() -> None:
    """Return expected values for float coercion."""
    assert coerce_float(None) is None
    assert coerce_float(value=False) is None
    assert coerce_float(1) == pytest.approx(1.0)
    assert coerce_float(FLOAT_SAMPLE) == FLOAT_SAMPLE
    assert coerce_float(FLOAT_STRING_SAMPLE) == FLOAT_STRING_EXPECTED
    assert coerce_float("bad") is None


def test_coerce_bool() -> None:
    """Return expected values for bool coercion."""
    assert coerce_bool(None) is None
    assert coerce_bool(value=True) is True
    assert coerce_bool(0) is False
    assert coerce_bool(2) is True
    assert coerce_bool(" yes ") is True
    assert coerce_bool("off") is False
    assert coerce_bool("maybe") is None


def test_coerce_str() -> None:
    """Return expected values for string coercion."""
    assert coerce_str(None) is None
    assert coerce_str(123) == "123"


def test_coerce_str_list_and_tuple() -> None:
    """Return expected values for string list/tuple coercion."""
    assert coerce_str_list(" ") == []
    assert coerce_str_list("foo") == ["foo"]
    assert coerce_str_list(["a", " ", 2]) == ["a", "2"]
    assert coerce_str_tuple(("a", "", "b")) == ("a", "b")


def test_coerce_mapping_list() -> None:
    """Return expected values for mapping list coercion."""
    assert coerce_mapping_list(None) is None
    assert coerce_mapping_list([{"a": 1}, {"b": 2}]) == [
        {"a": 1},
        {"b": 2},
    ]
    assert coerce_mapping_list(["x"]) == []


def test_coerce_to_recordbatch_reader() -> None:
    """Return expected readers for supported Arrow inputs."""
    table = pa.table({"a": [1, 2]})
    reader = coerce_to_recordbatch_reader(table)
    assert reader is not None
    assert reader.schema == table.schema

    batch = table.to_batches()[0]
    reader_from_batch = coerce_to_recordbatch_reader(batch)
    assert reader_from_batch is not None
    assert reader_from_batch.schema == batch.schema

    reader_from_batches = coerce_to_recordbatch_reader([batch])
    assert reader_from_batches is not None
    assert reader_from_batches.schema == batch.schema

    assert coerce_to_recordbatch_reader(None) is None


def test_raise_for_int() -> None:
    """Raise CoercionError for invalid int coercions."""
    assert raise_for_int(RAISE_INT_SAMPLE) == RAISE_INT_EXPECTED
    with pytest.raises(CoercionError):
        raise_for_int("bad")


def test_raise_for_float() -> None:
    """Raise CoercionError for invalid float coercions."""
    assert raise_for_float(RAISE_FLOAT_SAMPLE) == RAISE_FLOAT_EXPECTED
    with pytest.raises(CoercionError):
        raise_for_float("bad")


def test_raise_for_bool() -> None:
    """Raise CoercionError for invalid bool coercions."""
    assert raise_for_bool("true") is True
    with pytest.raises(CoercionError):
        raise_for_bool("bad")


def test_raise_for_str() -> None:
    """Raise CoercionError for invalid string coercions."""
    assert raise_for_str(123) == "123"
    with pytest.raises(CoercionError):
        raise_for_str(None)
