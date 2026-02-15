"""Tests for shared tree-sitter language runtime helpers."""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from typing import Any, cast

import pytest
from tools.cq.search.tree_sitter.core import language_runtime


def test_load_language_raises_when_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(language_runtime, "load_tree_sitter_language", lambda _language: None)
    with pytest.raises(RuntimeError, match="tree-sitter language unavailable"):
        language_runtime.load_language("python")


def test_make_parser_constructs_parser_with_language(monkeypatch: pytest.MonkeyPatch) -> None:
    @dataclass
    class _Parser:
        language: object

    fake_tree_sitter = cast("Any", types.ModuleType("tree_sitter"))
    fake_tree_sitter.Parser = _Parser
    monkeypatch.setitem(sys.modules, "tree_sitter", fake_tree_sitter)
    monkeypatch.setattr(language_runtime, "load_language", lambda _language: "LANG")

    parser = language_runtime.make_parser("python")
    assert parser.language == "LANG"
