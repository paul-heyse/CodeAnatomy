"""Unit tests for incremental exported definition indexing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from arrowdsl.core.ids import prefixed_hash_id
from incremental.exports import build_exported_defs_index
from incremental.runtime import IncrementalRuntime
from tests.utils import values_as_list

_QNAME_TYPE = pa.list_(pa.struct([("name", pa.string()), ("source", pa.string())]))


def test_build_exported_defs_index_hashes_qnames() -> None:
    """Top-level defs should emit stable qname hashes."""
    cst_defs = _cst_defs_table()
    runtime = _runtime_or_skip()
    result = build_exported_defs_index(cst_defs, runtime=runtime)

    expected_qnames = ["pkg.mod.foo", "pkg.mod.foo.Inner"]
    assert result.num_rows == len(expected_qnames)
    assert values_as_list(result["def_id"]) == ["def_root", "def_root"]
    assert values_as_list(result["qname"]) == expected_qnames

    expected_ids = prefixed_hash_id([pa.array(expected_qnames, type=pa.string())], prefix="qname")
    assert values_as_list(result["qname_id"]) == values_as_list(expected_ids)


def test_build_exported_defs_index_attaches_symbols() -> None:
    """Symbols from rel_def_symbol should attach to exported defs."""
    cst_defs = _cst_defs_table()
    runtime = _runtime_or_skip()
    rel_def_symbol = pa.table(
        {
            "def_id": pa.array(["def_root"], type=pa.string()),
            "path": pa.array(["src/mod.py"], type=pa.string()),
            "symbol": pa.array(["sym1"], type=pa.string()),
            "symbol_roles": pa.array([1], type=pa.int32()),
        }
    )

    result = build_exported_defs_index(
        cst_defs,
        runtime=runtime,
        rel_def_symbol=rel_def_symbol,
    )

    assert values_as_list(result["symbol"]) == ["sym1", "sym1"]
    assert values_as_list(result["symbol_roles"]) == [1, 1]


def _cst_defs_table() -> pa.Table:
    qnames = pa.array(
        [
            [
                {"name": "pkg.mod.foo", "source": "qualified"},
                {"name": "pkg.mod.foo.Inner", "source": "qualified"},
            ],
            [
                {"name": "pkg.mod.nested", "source": "qualified"},
            ],
        ],
        type=_QNAME_TYPE,
    )
    return pa.table(
        {
            "file_id": pa.array(["file_1", "file_1"], type=pa.string()),
            "path": pa.array(["src/mod.py", "src/mod.py"], type=pa.string()),
            "def_id": pa.array(["def_root", "def_nested"], type=pa.string()),
            "def_kind_norm": pa.array(["function", "function"], type=pa.string()),
            "name": pa.array(["foo", "nested"], type=pa.string()),
            "container_def_id": pa.array([None, "def_root"], type=pa.string()),
            "qnames": qnames,
        }
    )


def _runtime_or_skip() -> IncrementalRuntime:
    try:
        runtime = IncrementalRuntime.build()
        _ = runtime.ibis_backend()
    except ImportError as exc:
        pytest.skip(str(exc))
    else:
        return runtime
    msg = "Incremental runtime unavailable."
    raise RuntimeError(msg)
