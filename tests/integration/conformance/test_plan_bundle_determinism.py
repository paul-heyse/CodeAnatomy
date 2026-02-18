"""Deterministic plan-bundle conformance checks."""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pytest

from tests.harness.profiles import ConformanceBackendConfig, conformance_profile

_ELAPSED_COMPUTE_RE = re.compile(r"elapsed_compute=[^,]+")


def _normalize_explain_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        plan = row.get("plan")
        if isinstance(plan, str):
            plan = _ELAPSED_COMPUTE_RE.sub("elapsed_compute=<elapsed>", plan)
        normalized.append(
            {
                "plan_type": row.get("plan_type"),
                "plan": plan,
            }
        )
    return normalized


@pytest.mark.integration
def test_plan_bundle_explain_traces_are_deterministic(
    tmp_path: Path,
    conformance_backend_config: ConformanceBackendConfig,
) -> None:
    """Repeated capture should preserve deterministic explain traces per backend lane."""
    profile = conformance_profile()
    ctx = profile.session_context()
    ctx.from_arrow(pa.table({"id": [1, 2, 3]}), name="dataset")

    verbose_sql = "EXPLAIN VERBOSE SELECT id FROM dataset WHERE id > 1"
    analyze_sql = "EXPLAIN ANALYZE SELECT id FROM dataset WHERE id > 1"
    first_verbose = ctx.sql(verbose_sql).to_arrow_table().to_pylist()
    second_verbose = ctx.sql(verbose_sql).to_arrow_table().to_pylist()
    first_analyze = ctx.sql(analyze_sql).to_arrow_table().to_pylist()
    second_analyze = ctx.sql(analyze_sql).to_arrow_table().to_pylist()

    backend_context = conformance_backend_config.artifact_context(
        table_name="plan_bundle_determinism",
        root_dir=tmp_path,
    )

    assert _normalize_explain_rows(first_verbose) == _normalize_explain_rows(second_verbose)
    assert _normalize_explain_rows(first_analyze) == _normalize_explain_rows(second_analyze)
    assert backend_context["backend"] == conformance_backend_config.kind
    assert isinstance(backend_context["table_uri"], str)
