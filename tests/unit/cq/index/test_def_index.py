"""Tests for def-index parameter contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from tools.cq.index.def_index import DefIndex, ParamInfo


def test_param_info_is_frozen() -> None:
    """ParamInfo instances are immutable after construction."""
    info = ParamInfo(name="arg")
    kind_attr = "kind"
    with pytest.raises(FrozenInstanceError):
        setattr(info, kind_attr, "KEYWORD_ONLY")


def test_def_index_build_sets_parameter_kinds_without_mutation(tmp_path: Path) -> None:
    """Parameter kinds are set at construction time for all Python parameter categories."""
    source = """
def foo(a, /, b=1, *args, c, **kwargs):
    return a + b
""".strip()
    (tmp_path / "sample.py").write_text(source + "\n", encoding="utf-8")

    index = DefIndex.build(tmp_path)
    matches = list(index.find_function_by_name("foo"))
    assert len(matches) == 1
    kinds = [param.kind for param in matches[0].params]
    assert kinds == [
        "POSITIONAL_ONLY",
        "POSITIONAL_OR_KEYWORD",
        "VAR_POSITIONAL",
        "KEYWORD_ONLY",
        "VAR_KEYWORD",
    ]
