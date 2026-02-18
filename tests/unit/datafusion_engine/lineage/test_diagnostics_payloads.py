"""Tests for lineage diagnostics payload builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import pytest

from datafusion_engine.lineage.diagnostics_payloads import (
    rust_udf_snapshot_payload,
    view_fingerprint_payload,
    view_udf_parity_payload,
)

_EXPECTED_TOTAL_VIEWS = 2
_EXPECTED_TOTAL_UDFS = 2


@dataclass(frozen=True)
class _Bundle:
    plan_fingerprint: str
    plan_identity_hash: str | None = None


@dataclass(frozen=True)
class _Node:
    name: str
    required_udfs: tuple[str, ...] = ()
    plan_bundle: _Bundle | None = None


def test_view_fingerprint_payload_uses_bundle_fingerprint() -> None:
    """Fingerprint payload should preserve per-view plan fingerprint values."""
    rows = view_fingerprint_payload(
        view_nodes=(
            _Node(name="v1", plan_bundle=_Bundle(plan_fingerprint="fp1")),
            _Node(name="v2", plan_bundle=None),
        )
    )
    row_payload = cast("list[dict[str, object]]", rows["rows"])

    assert rows["total_views"] == _EXPECTED_TOTAL_VIEWS
    assert row_payload[0]["plan_fingerprint"] == "fp1"
    assert row_payload[1]["plan_fingerprint"] is None


def test_view_udf_parity_payload_reports_missing_snapshot_udfs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parity payload should flag views missing UDFs in snapshot coverage."""
    monkeypatch.setattr(
        "datafusion_engine.udf.extension_core.udf_names_from_snapshot",
        lambda _snapshot: frozenset({"udf_present"}),
    )

    payload = view_udf_parity_payload(
        snapshot={"status": "ok"},
        view_nodes=(
            _Node(name="v1", required_udfs=("udf_present",)),
            _Node(name="v2", required_udfs=("udf_missing",)),
        ),
        ctx=None,
    )

    assert payload["views_with_requirements"] == _EXPECTED_TOTAL_VIEWS
    assert payload["views_missing_snapshot_udfs"] == 1


def test_rust_udf_snapshot_payload_summarizes_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot payload should aggregate UDF family and metadata counts."""
    monkeypatch.setattr(
        "datafusion_engine.udf.extension_core.rust_udf_snapshot_hash",
        lambda _snapshot: "snapshot_hash",
    )
    monkeypatch.setattr(
        "datafusion_engine.udf.extension_core.udf_names_from_snapshot",
        lambda _snapshot: frozenset({"a", "b"}),
    )

    class _ManifestResult:
        @property
        def manifest(self) -> dict[str, list[str]]:
            return {"plugins": ["ext"]}

    monkeypatch.setattr(
        "datafusion_engine.extensions.plugin_manifest.resolve_plugin_manifest",
        lambda _module: _ManifestResult(),
    )

    payload = rust_udf_snapshot_payload(
        {
            "scalar": ["a"],
            "aggregate": ["b"],
            "aliases": {"x": "a"},
            "signature_inputs": {"a": ["i64"]},
            "return_types": {"a": "i64"},
            "parameter_names": {"a": ["x"]},
            "volatility": {"a": "immutable"},
            "rewrite_tags": {"a": ["tag"]},
        }
    )

    assert payload["snapshot_hash"] == "snapshot_hash"
    assert payload["total_udfs"] == _EXPECTED_TOTAL_UDFS
    assert payload["scalar_udfs"] == 1
    assert payload["aggregate_udfs"] == 1
    assert payload["aliases"] == 1
