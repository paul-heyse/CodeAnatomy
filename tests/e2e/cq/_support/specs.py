"""Spec assertion helpers for CQ e2e golden tests."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from tools.cq.core.schema import CqResult, Finding


def iter_findings(result: CqResult) -> Iterable[Finding]:
    """Yield all findings from a CQ result."""
    yield from result.key_findings
    yield from result.evidence
    for section in result.sections:
        yield from section.findings


def assert_result_matches_spec(result: CqResult, spec: dict[str, Any]) -> None:
    """Assert a CqResult satisfies a declarative golden spec."""
    findings = list(iter_findings(result))
    _assert_min_findings(findings=findings, spec=spec)
    _assert_required_categories(findings=findings, spec=spec)
    _assert_required_messages(findings=findings, spec=spec)
    _assert_required_files(findings=findings, spec=spec)
    _assert_required_section_titles(result=result, spec=spec)
    _assert_summary_mode(result=result, spec=spec)
    _assert_summary_lang_scope(result=result, spec=spec)


def _assert_min_findings(*, findings: list[Finding], spec: dict[str, Any]) -> None:
    min_findings = spec.get("min_findings")
    if isinstance(min_findings, int):
        assert len(findings) >= min_findings, (
            f"Expected at least {min_findings} findings, got {len(findings)}"
        )


def _assert_required_categories(*, findings: list[Finding], spec: dict[str, Any]) -> None:
    required_categories = spec.get("required_categories", [])
    if isinstance(required_categories, list):
        present = {finding.category for finding in findings}
        for category in required_categories:
            if isinstance(category, str):
                assert category in present, f"Missing required category: {category}"


def _assert_required_messages(*, findings: list[Finding], spec: dict[str, Any]) -> None:
    required_messages = spec.get("required_messages", [])
    if isinstance(required_messages, list):
        all_messages = [finding.message for finding in findings]
        for fragment in required_messages:
            if isinstance(fragment, str):
                assert any(fragment in message for message in all_messages), (
                    f"No finding message contains fragment: {fragment!r}"
                )


def _assert_required_files(*, findings: list[Finding], spec: dict[str, Any]) -> None:
    required_files = spec.get("required_files", [])
    if isinstance(required_files, list):
        anchored_files = [
            finding.anchor.file
            for finding in findings
            if finding.anchor is not None and isinstance(finding.anchor.file, str)
        ]
        file_hints = list(anchored_files)
        file_hints.extend(
            finding.message for finding in findings if isinstance(finding.message, str)
        )
        file_hints.extend(str(finding.details.data) for finding in findings)
        for file_fragment in required_files:
            if isinstance(file_fragment, str):
                assert any(file_fragment in hint for hint in file_hints), (
                    f"No finding anchor/message/detail contains file fragment: {file_fragment!r}"
                )


def _assert_required_section_titles(*, result: CqResult, spec: dict[str, Any]) -> None:
    required_section_titles = spec.get("required_section_titles", [])
    if isinstance(required_section_titles, list):
        present_titles = {section.title for section in result.sections}
        for title in required_section_titles:
            if isinstance(title, str):
                assert title in present_titles, f"Missing required section title: {title!r}"


def _assert_summary_mode(*, result: CqResult, spec: dict[str, Any]) -> None:
    summary_mode = spec.get("summary_mode")
    if isinstance(summary_mode, str):
        assert result.summary.get("mode") == summary_mode, (
            f"Expected summary.mode={summary_mode!r}, got {result.summary.get('mode')!r}"
        )


def _assert_summary_lang_scope(*, result: CqResult, spec: dict[str, Any]) -> None:
    summary_lang_scope = spec.get("summary_lang_scope")
    if isinstance(summary_lang_scope, str):
        assert result.summary.get("lang_scope") == summary_lang_scope, (
            "Expected summary.lang_scope="
            f"{summary_lang_scope!r}, got {result.summary.get('lang_scope')!r}"
        )
