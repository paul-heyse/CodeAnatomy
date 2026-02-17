"""Tests for classifier runtime single-file scan behavior."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.astgrep.sgpy_scanner import RuleSpec
from tools.cq.core.types import QueryLanguage
from tools.cq.search.pipeline import classifier_runtime


def test_get_record_context_disables_prefilter_for_single_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Disable prefilter when running single-file record-context queries."""
    cache_context = classifier_runtime.ClassifierCacheContext()
    cache_context.clear()
    file_path = tmp_path / "a.py"
    file_path.write_text("def foo():\n    return 1\n", encoding="utf-8")

    monkeypatch.setattr(
        classifier_runtime,
        "get_rules_for_types",
        lambda *_args, **_kwargs: (
            RuleSpec(
                rule_id="py_def_function",
                record_type="def",
                kind="function",
                config={"rule": {"pattern": "def $F($$$ARGS):"}},
            ),
        ),
    )

    observed: dict[str, object] = {}

    def _fake_scan(
        _files: list[Path],
        _rules: tuple[RuleSpec, ...],
        _root: Path,
        *,
        lang: QueryLanguage = "python",
        prefilter: bool = True,
    ) -> list[object]:
        _ = lang
        observed["prefilter"] = prefilter
        return []

    monkeypatch.setattr(classifier_runtime, "scan_files", _fake_scan)
    classifier_runtime.get_record_context(
        file_path,
        tmp_path,
        lang="python",
        cache_context=cache_context,
    )
    assert observed["prefilter"] is False
