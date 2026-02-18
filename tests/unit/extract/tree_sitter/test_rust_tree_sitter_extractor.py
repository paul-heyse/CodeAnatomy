"""Tests for tree-sitter Rust-bridge extraction routing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.extensions import datafusion_ext
from extract.coordination.context import FileContext
from extract.extractors.tree_sitter.builders import (
    TreeSitterExtractOptions,
    _extract_ts_file_row,
    _parser,
)

EXPECTED_NODE_COUNT = 2


def test_extract_ts_file_row_uses_bridge_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bridge payload should short-circuit Python tree walk when present."""
    source = "def foo():\n    return 1\n"
    file_ctx = FileContext(
        file_id="file-1",
        path="foo.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=source,
        data=source.encode("utf-8"),
    )

    options = TreeSitterExtractOptions(
        include_nodes=True,
        include_edges=False,
        include_errors=False,
        include_missing=False,
        include_captures=False,
        include_defs=False,
        include_calls=False,
        include_imports=False,
        include_docstrings=False,
        include_stats=True,
    )

    def _bridge(_source: str, _file_path: str) -> pa.RecordBatch:
        source_bytes = source.encode("utf-8")
        return pa.record_batch(
            [
                pa.array(["module", "function_definition"]),
                pa.array([0, 0], type=pa.int64()),
                pa.array([len(source_bytes), 10], type=pa.int64()),
                pa.array([None, 0], type=pa.int64()),
                pa.array(["foo.py", "foo.py"]),
            ],
            names=["node_type", "bstart", "bend", "parent_id", "file"],
        )

    monkeypatch.setattr(datafusion_ext, "extract_tree_sitter_batch", _bridge, raising=False)

    row = _extract_ts_file_row(
        file_ctx,
        parser=_parser(options),
        cache=None,
        options=options,
        query_pack=None,
    )

    assert row is not None
    nodes = row.get("nodes")
    assert isinstance(nodes, list)
    assert len(nodes) == EXPECTED_NODE_COUNT
    assert nodes[1]["kind"] == "function_definition"


def test_extract_ts_file_row_raises_when_bridge_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hard-cutover path should fail when bridge payload is unavailable."""
    source = "def foo():\n    return 1\n"
    file_ctx = FileContext(
        file_id="file-1",
        path="foo.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=source,
        data=source.encode("utf-8"),
    )
    options = TreeSitterExtractOptions()

    monkeypatch.setattr(
        datafusion_ext, "extract_tree_sitter_batch", lambda *_args: None, raising=False
    )

    with pytest.raises(RuntimeError, match="extract_tree_sitter_batch bridge"):
        _ = _extract_ts_file_row(
            file_ctx,
            parser=_parser(options),
            cache=None,
            options=options,
            query_pack=None,
        )
