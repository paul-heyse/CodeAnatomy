"""Tests for relational batching in cq run."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.cli_app.context import CliContext
from tools.cq.query import batch_spans
from tools.cq.run.runner import execute_run_plan
from tools.cq.run.spec import QStep, RunPlan


def test_relational_batching_parses_each_file_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Ensure relational batching parses each file once across multiple queries."""
    (tmp_path / "alpha.py").write_text(
        "def outer_alpha():\n    def inner_alpha():\n        return 1\n    return inner_alpha()\n",
        encoding="utf-8",
    )
    (tmp_path / "beta.py").write_text(
        "def outer_beta():\n    def inner_beta():\n        return 2\n    return inner_beta()\n",
        encoding="utf-8",
    )

    parse_calls = 0
    real_sgroot = batch_spans.SgRoot

    def counting_sgroot(src: str, language: str) -> object:
        nonlocal parse_calls
        parse_calls += 1
        return real_sgroot(src, language)

    monkeypatch.setattr(batch_spans, "SgRoot", counting_sgroot)

    ctx = CliContext.build(argv=["cq", "run"], root=tmp_path)
    plan = RunPlan(
        steps=(
            QStep(query="entity=function inside='def outer_alpha' lang=python"),
            QStep(query="entity=function inside='def outer_beta' lang=python"),
        )
    )
    result = execute_run_plan(plan, ctx)

    assert parse_calls == 2
    files = {
        Path(finding.anchor.file).name
        for finding in result.key_findings
        if finding.anchor is not None
    }
    assert {"alpha.py", "beta.py"}.issubset(files)
