"""Tests for ast-grep metavariable extraction helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.astgrep.metavar_extract import extract_match_metavars

_EXPECTED_CAPTURED_NODE_COUNT = 2


@dataclass(frozen=True)
class _Point:
    line: int = 1
    column: int = 0


@dataclass(frozen=True)
class _Range:
    start: _Point = _Point()
    end: _Point = _Point()


class _FakeNode:
    def __init__(self, text: str, *, kind: str = "identifier") -> None:
        self._text = text
        self._kind = kind

    def text(self) -> str:
        return self._text

    def kind(self) -> str:
        return self._kind

    @staticmethod
    def range() -> _Range:
        """Return a stable fake source range."""
        return _Range()


class _FakeMatch:
    def __init__(self) -> None:
        self._single: dict[str, _FakeNode] = {}
        self._multi: dict[str, list[_FakeNode]] = {}

    def bind_single(self, name: str, node: _FakeNode) -> None:
        self._single[name] = node

    def bind_multi(self, name: str, nodes: list[_FakeNode]) -> None:
        self._multi[name] = nodes

    def get_match(self, meta_var: str) -> _FakeNode | None:
        return self._single.get(meta_var)

    def get_multiple_matches(self, meta_var: str) -> list[_FakeNode]:
        return self._multi.get(meta_var, [])


def test_extract_match_metavars_single_capture_keys() -> None:
    """Single captures emit both bare and `$`-prefixed keys."""
    match = _FakeMatch()
    match.bind_single("X", _FakeNode("target"))

    payload = extract_match_metavars(
        match,
        metavar_names=("X",),
        variadic_names=frozenset(),
        include_multi=False,
    )

    assert payload == {"X": "target", "$X": "target"}


def test_extract_match_metavars_variadic_skips_separator_nodes() -> None:
    """Variadic captures omit separator tokens from node payloads."""
    match = _FakeMatch()
    match.bind_multi("ARGS", [_FakeNode("a"), _FakeNode(",", kind=","), _FakeNode("b")])

    payload = extract_match_metavars(
        match,
        metavar_names=("ARGS",),
        variadic_names=frozenset({"ARGS"}),
        include_multi=True,
    )

    assert "$$$ARGS" in payload
    multi = payload["$$$ARGS"]
    assert isinstance(multi, dict)
    assert multi["text"] == "a, b"
    assert isinstance(multi["nodes"], list)
    assert len(multi["nodes"]) == _EXPECTED_CAPTURED_NODE_COUNT
