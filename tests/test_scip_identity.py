"""Tests for SCIP identity resolution helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from extract import scip_identity


def test_resolve_scip_identity_overrides_default_namespace() -> None:
    """Defaults to the org prefix when overrides are provided for name/version."""
    identity = scip_identity.resolve_scip_identity(
        Path(),
        project_name_override="owner/repo",
        project_version_override="deadbeef",
        project_namespace_override=None,
    )
    assert identity.project_name == "owner/repo"
    assert identity.project_version == "deadbeef"
    assert identity.project_namespace == scip_identity.DEFAULT_ORG_PREFIX


def test_resolve_scip_identity_from_gh(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Resolves identity via gh when overrides are missing."""

    def fake_run_gh(_repo_root: Path, args: list[str]) -> str:
        if args[:2] == ["repo", "view"]:
            return "org/example"
        if args[0] == "api":
            return "cafebabe"
        msg = "Unexpected gh args"
        raise AssertionError(msg)

    monkeypatch.setattr(scip_identity, "_run_gh", fake_run_gh)
    identity = scip_identity.resolve_scip_identity(
        tmp_path,
        project_name_override=None,
        project_version_override=None,
        project_namespace_override="github.com/org",
    )
    assert identity.project_name == "org/example"
    assert identity.project_version == "cafebabe"
    assert identity.project_namespace == "github.com/org"
