"""Tests for macro result builder."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from tools.cq.core.schema import Finding, Section
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.result_builder import MacroResultBuilder


class _Toolchain:
    @staticmethod
    def to_dict() -> dict[str, str | None]:
        return {}


def test_macro_result_builder_populates_summary_sections_and_ids(tmp_path: Path) -> None:
    """Builder should accumulate summary/findings and assign finding IDs on build."""
    builder = MacroResultBuilder(
        "calls",
        root=Path(tmp_path),
        argv=["cq", "calls", "target"],
        tc=cast("Toolchain", _Toolchain()),
        started_ms=0.0,
    )
    builder.set_summary(query="target", total_sites=2)
    builder.add_findings([Finding(category="summary", message="Found 2 sites")])
    builder.add_section(Section(title="Details"))

    result = builder.build()

    assert result.summary["query"] == "target"
    assert result.sections[0].title == "Details"
    assert result.key_findings[0].stable_id is not None
