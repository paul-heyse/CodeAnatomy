"""Tests for tree-sitter Rust-bridge extraction routing."""

from __future__ import annotations

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

    def _bridge(_source: str, _file_path: str) -> dict[str, object]:
        return {
            "repo": "",
            "path": "foo.py",
            "file_id": "file-1",
            "file_sha256": None,
            "nodes": [
                {"kind": "module", "bstart": 0, "bend": len(source.encode("utf-8"))},
                {"kind": "function_definition", "bstart": 0, "bend": 10},
            ],
            "edges": [],
            "errors": [],
            "missing": [],
            "captures": [],
            "defs": [],
            "calls": [],
            "imports": [],
            "docstrings": [],
            "stats": {"node_count": 2},
            "attrs": [],
        }

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


def test_extract_ts_file_row_preserves_bridge_nested_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bridge mapping payload should preserve nested query-derived structures."""
    source = "import os\n\ndef foo():\n    return bar()\n"
    file_ctx = FileContext(
        file_id="file-1",
        path="foo.py",
        abs_path=None,
        file_sha256="abc",
        encoding="utf-8",
        text=source,
        data=source.encode("utf-8"),
    )
    options = TreeSitterExtractOptions()

    def _bridge(_source: str, _file_path: str, _payload: dict[str, object]) -> dict[str, object]:
        return {
            "repo": "repo-x",
            "path": "foo.py",
            "file_id": "file-1",
            "file_sha256": "abc",
            "nodes": [],
            "edges": [],
            "errors": [],
            "missing": [],
            "captures": [{"capture_id": "c1", "query_name": "defs", "capture_name": "def.node"}],
            "defs": [{"node_id": "n1", "kind": "function_definition", "name": "foo"}],
            "calls": [{"node_id": "n2", "callee_kind": "identifier", "callee_text": "bar"}],
            "imports": [{"node_id": "n3", "kind": "Import", "name": "os"}],
            "docstrings": [],
            "stats": {"node_count": 3},
            "attrs": {"language_name": "python"},
        }

    monkeypatch.setattr(datafusion_ext, "extract_tree_sitter_batch", _bridge, raising=False)
    row = _extract_ts_file_row(
        file_ctx,
        parser=_parser(options),
        cache=None,
        options=options,
        query_pack=None,
    )

    assert row is not None
    assert row["repo"] == "repo-x"
    assert isinstance(row.get("captures"), list)
    assert isinstance(row.get("defs"), list)
    assert isinstance(row.get("calls"), list)
    assert isinstance(row.get("imports"), list)


def test_extract_ts_file_row_sends_include_option_toggles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bridge request payload should carry include_* option toggles."""
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
        include_nodes=False,
        include_edges=False,
        include_errors=False,
        include_missing=False,
        include_captures=False,
        include_defs=True,
        include_calls=False,
        include_imports=False,
        include_docstrings=False,
        include_stats=False,
    )
    captured: dict[str, object] = {}

    def _bridge(_source: str, _file_path: str, payload: dict[str, object]) -> dict[str, object]:
        captured["payload"] = payload
        return {
            "repo": "",
            "path": "foo.py",
            "file_id": "file-1",
            "file_sha256": None,
            "nodes": [],
            "edges": [],
            "errors": [],
            "missing": [],
            "captures": [],
            "defs": [{"node_id": "n1", "kind": "function_definition", "name": "foo"}],
            "calls": [],
            "imports": [],
            "docstrings": [],
            "stats": None,
            "attrs": [],
        }

    monkeypatch.setattr(datafusion_ext, "extract_tree_sitter_batch", _bridge, raising=False)
    row = _extract_ts_file_row(
        file_ctx,
        parser=_parser(options),
        cache=None,
        options=options,
        query_pack=None,
    )

    assert row is not None
    bridge_payload = captured["payload"]
    assert isinstance(bridge_payload, dict)
    bridge_options = bridge_payload.get("options")
    assert isinstance(bridge_options, dict)
    assert bridge_options["include_nodes"] is False
    assert bridge_options["include_defs"] is True
    assert bridge_options["include_stats"] is False
