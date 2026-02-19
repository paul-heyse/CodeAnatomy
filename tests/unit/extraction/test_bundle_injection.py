"""Extraction bundle-injection tests for request-envelope orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from extraction import orchestrator as orchestrator_mod
from extraction.contracts import RunExtractionRequestV1
from extraction.options import ExtractionRunOptions

if TYPE_CHECKING:
    from extract.session import ExtractSession
    from extraction.runtime_profile import RuntimeProfileSpec


def test_run_extraction_uses_execution_bundle_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """run_extraction should honor a valid execution bundle override."""
    captured: dict[str, object] = {}
    override = orchestrator_mod._ExtractExecutionBundle(  # noqa: SLF001
        runtime_spec=cast("RuntimeProfileSpec", object()),
        extract_session=cast("ExtractSession", object()),
    )

    def _run_repo_scan_with_fallback(
        *,
        execution_bundle: object,
        **_kwargs: object,
    ) -> None:
        captured["bundle"] = execution_bundle

    monkeypatch.setattr(
        orchestrator_mod,
        "_run_repo_scan_with_fallback",
        _run_repo_scan_with_fallback,
    )
    monkeypatch.setattr(
        orchestrator_mod,
        "normalize_extraction_options",
        lambda *_args, **_kwargs: ExtractionRunOptions(),
    )

    result = orchestrator_mod.run_extraction(
        RunExtractionRequestV1(
            repo_root=str(tmp_path),
            work_dir=str(tmp_path / "work"),
            execution_bundle_override=override,
        )
    )

    assert captured["bundle"] is override
    assert result.delta_locations == {}


def test_run_extraction_rejects_invalid_execution_bundle_override(tmp_path: Path) -> None:
    """run_extraction should reject overrides that are not bundle instances."""
    with pytest.raises(TypeError, match="execution_bundle_override"):
        orchestrator_mod.run_extraction(
            RunExtractionRequestV1(
                repo_root=str(tmp_path),
                work_dir=str(tmp_path / "work"),
                execution_bundle_override={"not": "a bundle"},
            )
        )
