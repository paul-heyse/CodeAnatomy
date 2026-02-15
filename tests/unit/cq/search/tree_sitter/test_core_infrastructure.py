"""Tests for tree-sitter infrastructure utilities."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.tree_sitter.core.infrastructure import (
    ParserControlSettingsV1,
    apply_parser_controls,
    build_stream_reader,
    parse_streaming_source,
    parser_controls_from_env,
    run_file_lanes_parallel,
)

# ── Parallel Execution Tests ──


def _double(x: int) -> int:
    """Double one integer input.

    Returns:
    -------
    int
        Doubled value.
    """
    return x * 2


def _identity(x: int) -> int:
    """Return one integer input unchanged.

    Returns:
    -------
    int
        Original value.
    """
    return x


def _add_five(x: int) -> int:
    """Add five to one integer input.

    Returns:
    -------
    int
        Incremented value.
    """
    return x + 5


def test_run_file_lanes_parallel_preserves_order() -> None:
    jobs = [1, 2, 3, 4, 5]
    results = run_file_lanes_parallel(jobs, worker=_double, max_workers=2)
    assert results == [2, 4, 6, 8, 10]


def test_run_file_lanes_parallel_handles_empty_jobs() -> None:
    results = run_file_lanes_parallel([], worker=_identity, max_workers=2)
    assert results == []


def test_run_file_lanes_parallel_falls_back_to_sequential_when_max_workers_is_1() -> None:
    jobs = [10, 20, 30]
    results = run_file_lanes_parallel(jobs, worker=_add_five, max_workers=1)
    assert results == [15, 25, 35]


# ── Streaming Source Tests ──


class _Parser:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def parse(
        self,
        source: object,
        old_tree: object = None,
        encoding: str | None = None,
    ) -> str:
        self.calls.append((source, old_tree, encoding))
        if callable(source):
            return "tree_from_stream"
        return "tree_from_bytes"


def test_build_stream_reader_chunks_data() -> None:
    reader = build_stream_reader(b"abcdef", chunk_size=2)
    assert reader(0, (0, 0)) == b"ab"
    assert reader(2, (0, 2)) == b"cd"
    assert reader(10, (0, 0)) == b""


def test_build_stream_reader_handles_boundary_conditions() -> None:
    reader = build_stream_reader(b"test", chunk_size=10)
    assert reader(0, (0, 0)) == b"test"
    assert reader(-1, (0, 0)) == b""


def test_parse_streaming_source_prefers_stream_callback() -> None:
    parser = _Parser()
    tree = parse_streaming_source(parser, b"hello world")
    assert tree == "tree_from_stream"
    assert parser.calls


# ── Parser Controls Tests ──


class _MockParser:
    def __init__(self) -> None:
        self.reset_calls = 0
        self.dot_paths: list[str] = []
        self.logger = None

    def reset(self) -> None:
        self.reset_calls += 1

    def print_dot_graphs(self, path: str) -> None:
        self.dot_paths.append(path)


def test_apply_parser_controls_respects_flags(tmp_path: Path) -> None:
    parser = _MockParser()
    settings = ParserControlSettingsV1(
        reset_before_parse=True,
        enable_logger=True,
        dot_graph_dir=str(tmp_path),
    )
    apply_parser_controls(parser, settings)
    assert parser.reset_calls == 1
    assert parser.logger is not None
    assert parser.dot_paths


def test_apply_parser_controls_skips_when_flags_false() -> None:
    parser = _MockParser()
    settings = ParserControlSettingsV1(
        reset_before_parse=False,
        enable_logger=False,
        dot_graph_dir=None,
    )
    apply_parser_controls(parser, settings)
    assert parser.reset_calls == 0
    assert parser.logger is None
    assert not parser.dot_paths


def test_parser_controls_from_env_defaults_to_false(monkeypatch: object) -> None:
    import pytest

    if not hasattr(monkeypatch, "delenv"):
        pytest.skip("monkeypatch fixture required")

    mp = monkeypatch

    mp.delenv("CQ_TREE_SITTER_PARSER_RESET", raising=False)
    mp.delenv("CQ_TREE_SITTER_PARSER_LOGGER", raising=False)
    mp.delenv("CQ_TREE_SITTER_DOT_GRAPH_DIR", raising=False)

    settings = parser_controls_from_env()
    assert settings.reset_before_parse is False
    assert settings.enable_logger is False
    assert settings.dot_graph_dir is None


def test_parser_controls_from_env_respects_flags(monkeypatch: object, tmp_path: Path) -> None:
    import pytest

    if not hasattr(monkeypatch, "setenv"):
        pytest.skip("monkeypatch fixture required")

    mp = monkeypatch

    mp.setenv("CQ_TREE_SITTER_PARSER_RESET", "1")
    mp.setenv("CQ_TREE_SITTER_PARSER_LOGGER", "1")
    mp.setenv("CQ_TREE_SITTER_DOT_GRAPH_DIR", str(tmp_path))

    settings = parser_controls_from_env()
    assert settings.reset_before_parse is True
    assert settings.enable_logger is True
    assert settings.dot_graph_dir == str(tmp_path)
