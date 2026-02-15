"""Tests for LDMD writer output contracts."""

from __future__ import annotations

import msgspec
from tools.cq.core.front_door_insight import (
    FrontDoorInsightV1,
    InsightArtifactRefsV1,
    InsightTargetV1,
)
from tools.cq.core.schema import CqResult, Finding, Section, mk_result, mk_runmeta
from tools.cq.ldmd.writer import render_ldmd_from_cq_result


def _sample_result() -> CqResult:
    run = mk_runmeta(
        macro="q",
        argv=["cq", "q", "entity=function name=foo"],
        root="/repo",
        started_ms=0.0,
        toolchain={"python": "3.13"},
    )
    result = mk_result(run)
    result.summary = {
        "query": "foo",
        "mode": "identifier",
        "front_door_insight": msgspec.to_builtins(
            FrontDoorInsightV1(
                source="search",
                target=InsightTargetV1(symbol="foo", kind="function"),
                artifact_refs=InsightArtifactRefsV1(
                    diagnostics=".cq/artifacts/diag.json",
                    telemetry=".cq/artifacts/diag.json",
                ),
            )
        ),
        "python_semantic_diagnostics": [{"message": "diag"}],
        "cross_language_diagnostics": [{"code": "ML001"}],
    }
    result.key_findings = [
        Finding(category="definition", message=f"finding-{index}") for index in range(1, 8)
    ]
    result.sections = [
        Section(
            title="Definitions",
            findings=[
                Finding(category="context", message=f"context-{index}") for index in range(1, 8)
            ],
        )
    ]
    return result


def test_render_ldmd_from_cq_result_has_balanced_markers() -> None:
    content = render_ldmd_from_cq_result(_sample_result())
    begin_count = content.count("<!--LDMD:BEGIN")
    end_count = content.count("<!--LDMD:END")
    assert begin_count > 0
    assert begin_count == end_count


def test_render_ldmd_from_cq_result_preview_body_split() -> None:
    content = render_ldmd_from_cq_result(_sample_result())
    assert '<!--LDMD:BEGIN id="key_findings_tldr"' in content
    assert '<!--LDMD:BEGIN id="key_findings_body"' in content
    assert "finding-6" in content


def test_render_ldmd_from_cq_result_has_stable_section_ids() -> None:
    content = render_ldmd_from_cq_result(_sample_result())
    assert '<!--LDMD:BEGIN id="section_0"' in content
    assert '<!--LDMD:BEGIN id="section_0_tldr"' in content
    assert '<!--LDMD:BEGIN id="section_0_body"' in content


def test_render_ldmd_from_cq_result_uses_artifact_only_diagnostics() -> None:
    content = render_ldmd_from_cq_result(_sample_result())
    assert "Diagnostic Artifacts" in content
    assert "offloaded_keys:" in content
    assert ".cq/artifacts/diag.json" in content
    assert '"message": "diag"' not in content
