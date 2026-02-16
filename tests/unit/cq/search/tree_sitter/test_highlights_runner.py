"""Tests for highlights query-pack execution."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest
from tools.cq.search.tree_sitter.contracts.core_models import QueryWindowV1
from tools.cq.search.tree_sitter.core import highlights_runner as runner

EXPECTED_HIGHLIGHT_TOKENS = 2
PYTHON_ABI_VERSION = 15


@dataclass(frozen=True)
class _Node:
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]


def test_run_highlights_pack_extracts_and_sorts_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test run highlights pack extracts and sorts tokens."""
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        runner,
        "load_language_registry",
        lambda _language: SimpleNamespace(
            grammar_name="python",
            semantic_version=(0, 25, 0),
            abi_version=15,
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_distribution_query_source",
        lambda _language, _pack_name: "(identifier) @function",
    )
    monkeypatch.setattr(runner, "compile_query", lambda **_kwargs: object())

    def _run_matches(
        *_args: object, **kwargs: object
    ) -> tuple[list[tuple[int, dict[str, list[_Node]]]], object]:
        seen["windows"] = kwargs["windows"]
        return (
            [
                (0, {"@function": [_Node(8, 12, (0, 8))]}),
                (1, {"keyword": [_Node(0, 2, (0, 0))]}),
            ],
            object(),
        )

    monkeypatch.setattr(runner, "run_bounded_query_matches", _run_matches)

    windows = (QueryWindowV1(start_byte=0, end_byte=20),)
    result = runner.run_highlights_pack(
        language="python",
        root=cast("Any", object()),
        source_bytes=b"fn demo() {}",
        windows=windows,
    )
    assert seen["windows"] == windows
    assert result.token_count == EXPECTED_HIGHLIGHT_TOKENS
    assert tuple(token.capture_name for token in result.tokens) == ("keyword", "function")
    assert result.grammar_name == "python"
    assert result.semantic_version == (0, 25, 0)
    assert result.abi_version == PYTHON_ABI_VERSION


def test_run_highlights_pack_returns_empty_when_distribution_pack_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test run highlights pack returns empty when distribution pack missing."""
    monkeypatch.setattr(
        runner,
        "load_language_registry",
        lambda _language: SimpleNamespace(
            grammar_name="rust",
            semantic_version=(0, 24, 0),
            abi_version=14,
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_distribution_query_source",
        lambda _language, _pack_name: None,
    )

    result = runner.run_highlights_pack(
        language="rust",
        root=cast("Any", object()),
        source_bytes=b"",
        windows=(QueryWindowV1(start_byte=0, end_byte=1),),
    )
    assert result.token_count == 0
    assert result.tokens == ()
    assert result.grammar_name == "rust"


def test_run_highlights_pack_degrades_gracefully_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test run highlights pack degrades gracefully on runtime error."""
    monkeypatch.setattr(
        runner,
        "load_language_registry",
        lambda _language: SimpleNamespace(
            grammar_name="python",
            semantic_version=(0, 25, 0),
            abi_version=15,
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_distribution_query_source",
        lambda _language, _pack_name: "(identifier) @function",
    )

    def _raise(**_kwargs: object) -> object:
        msg = "compile failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(runner, "compile_query", _raise)
    result = runner.run_highlights_pack(
        language="python",
        root=cast("Any", object()),
        source_bytes=b"",
        windows=(QueryWindowV1(start_byte=0, end_byte=1),),
    )
    assert result.token_count == 0
    assert result.tokens == ()
