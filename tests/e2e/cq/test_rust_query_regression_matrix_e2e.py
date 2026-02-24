"""Rust CQ regression matrix covering audited contract deficiencies."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pytest
from tools.cq.core.schema import CqResult


@dataclass(frozen=True, slots=True)
class MatrixCase:
    """One Rust matrix command and expected contract checks."""

    case_id: str
    argv: tuple[str, ...]
    checks: Mapping[str, object]


def _load_matrix_cases() -> tuple[MatrixCase, ...]:
    fixture_path = (
        Path(__file__).resolve().parent / "fixtures" / "rust_query_regression_matrix_cases.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        msg = "Matrix fixture must be a JSON list."
        raise TypeError(msg)

    cases: list[MatrixCase] = []
    for item in payload:
        if not isinstance(item, Mapping):
            msg = "Matrix case entry must be a JSON object."
            raise TypeError(msg)
        case_id = item.get("id")
        argv = item.get("argv")
        checks = item.get("checks", {})
        if not isinstance(case_id, str):
            msg = "Matrix case `id` must be a string."
            raise TypeError(msg)
        if not isinstance(argv, list) or not all(isinstance(arg, str) for arg in argv):
            msg = f"Matrix case `{case_id}` has invalid argv."
            raise TypeError(msg)
        if not isinstance(checks, Mapping):
            msg = f"Matrix case `{case_id}` has invalid checks."
            raise TypeError(msg)
        cases.append(MatrixCase(case_id=case_id, argv=tuple(argv), checks=checks))
    return tuple(cases)


MATRIX_CASES = _load_matrix_cases()


def _as_mapping(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _as_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _assert_case(  # noqa: C901, PLR0912, PLR0914, PLR0915
    case: MatrixCase, result: CqResult
) -> None:
    checks = case.checks
    summary = result.summary

    summary_mode = checks.get("summary_mode")
    if isinstance(summary_mode, str):
        assert summary.get("mode") == summary_mode

    summary_lang_scope = checks.get("summary_lang_scope")
    if isinstance(summary_lang_scope, str):
        assert summary.get("lang_scope") == summary_lang_scope

    summary_variant = checks.get("summary_variant")
    if isinstance(summary_variant, str):
        assert summary.get("summary_variant") == summary_variant

    lint_status = checks.get("query_pack_lint_status")
    if isinstance(lint_status, str):
        lint_payload = _as_mapping(summary.get("query_pack_lint"))
        assert lint_payload is not None
        assert lint_payload.get("status") == lint_status

    min_total_matches = _as_int(checks.get("min_total_matches"))
    if min_total_matches is not None:
        total_matches = _as_int(summary.get("total_matches"))
        assert total_matches is not None
        assert total_matches >= min_total_matches

    if checks.get("require_total_matches_equals_matches") is True:
        total_matches = _as_int(summary.get("total_matches"))
        matches = _as_int(summary.get("matches"))
        assert total_matches is not None
        assert matches is not None
        assert total_matches == matches

    if checks.get("require_non_empty_sections") is True:
        assert len(result.sections) > 0

    required_section_titles = checks.get("required_section_titles")
    if isinstance(required_section_titles, list):
        titles = {section.title for section in result.sections}
        for title in required_section_titles:
            if isinstance(title, str):
                assert title in titles

    key_messages = [finding.message for finding in result.key_findings]
    required_key_findings = checks.get("required_key_findings")
    if isinstance(required_key_findings, list):
        for fragment in required_key_findings:
            if isinstance(fragment, str):
                assert any(fragment in message for message in key_messages)

    forbidden_key_findings = checks.get("forbidden_key_findings")
    if isinstance(forbidden_key_findings, list):
        for fragment in forbidden_key_findings:
            if isinstance(fragment, str):
                assert not any(fragment in message for message in key_messages)

    allowed_categories = checks.get("allowed_key_finding_categories")
    if isinstance(allowed_categories, list):
        allowed = {value for value in allowed_categories if isinstance(value, str)}
        assert len(result.key_findings) > 0
        assert all(finding.category in allowed for finding in result.key_findings)

    expected_evidence_count = _as_int(checks.get("expected_evidence_count"))
    if expected_evidence_count is not None:
        assert len(result.evidence) == expected_evidence_count

    expected_section_count = _as_int(checks.get("expected_section_count"))
    if expected_section_count is not None:
        assert len(result.sections) == expected_section_count

    language_partition = checks.get("language_partition")
    if isinstance(language_partition, str):
        languages_payload = _as_mapping(summary.get("languages"))
        assert languages_payload is not None
        partition = _as_mapping(languages_payload.get(language_partition))
        assert partition is not None
        if checks.get("require_partition_total_matches_equals_matches") is True:
            partition_total = _as_int(partition.get("total_matches"))
            partition_matches = _as_int(partition.get("matches"))
            assert partition_total is not None
            assert partition_matches is not None
            assert partition_total == partition_matches
        if checks.get("require_partition_total_matches_equals_summary") is True:
            partition_total = _as_int(partition.get("total_matches"))
            total_matches = _as_int(summary.get("total_matches"))
            assert partition_total is not None
            assert total_matches is not None
            assert partition_total == total_matches

    step_index = _as_int(checks.get("step_index"))
    if step_index is not None:
        steps = summary.get("steps")
        assert isinstance(steps, Sequence)
        assert 0 <= step_index < len(steps)
        step_id = steps[step_index]
        assert isinstance(step_id, str)
        step_summaries = _as_mapping(summary.get("step_summaries"))
        assert step_summaries is not None
        step_summary = _as_mapping(step_summaries.get(step_id))
        assert step_summary is not None

        step_lang_scope = checks.get("step_lang_scope")
        if isinstance(step_lang_scope, str):
            assert step_summary.get("lang_scope") == step_lang_scope

        step_languages_only = checks.get("step_languages_only")
        if isinstance(step_languages_only, list):
            expected_languages = sorted(
                lang for lang in step_languages_only if isinstance(lang, str)
            )
            step_languages = _as_mapping(step_summary.get("languages"))
            assert step_languages is not None
            assert sorted(step_languages.keys()) == expected_languages

    target_resolution_kind = checks.get("summary_target_resolution_kind")
    if isinstance(target_resolution_kind, str):
        assert summary.get("target_resolution_kind") == target_resolution_kind

    target_resolution_ambiguous = checks.get("summary_target_resolution_ambiguous")
    if isinstance(target_resolution_ambiguous, bool):
        assert summary.get("target_resolution_ambiguous") is target_resolution_ambiguous

    target_file_suffix = checks.get("target_file_suffix")
    if isinstance(target_file_suffix, str):
        target_file = summary.get("target_file")
        assert isinstance(target_file, str)
        assert target_file.endswith(target_file_suffix)


@pytest.mark.e2e
@pytest.mark.requires_ast_grep
@pytest.mark.parametrize("case", MATRIX_CASES, ids=lambda case: case.case_id)
def test_rust_matrix_cases(
    run_cq_result: Callable[..., CqResult],
    case: MatrixCase,
) -> None:
    """Exercise Rust matrix commands and assert remediated output contracts."""
    result = run_cq_result(list(case.argv))
    _assert_case(case, result)
