"""Tests for Rust lineage bridge integration."""

from __future__ import annotations

import pytest

from datafusion_engine.lineage import reporting
from datafusion_engine.plan import rust_bundle_bridge


def test_extract_lineage_from_plan_decodes_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rust bridge decodes JSON payload into mapping output."""

    def _fake_extract_lineage_json(_plan: object) -> str:
        return (
            '{"scans":[{"dataset_name":"events","projected_columns":["id"],'
            '"pushed_filters":["id > 1"]}],"required_columns_by_dataset":{"events":["id"]},'
            '"filters":["id > 1"],"required_udfs":[],"required_rewrite_tags":[]}'
        )

    monkeypatch.setattr(
        "datafusion_engine.extensions.datafusion_ext.extract_lineage_json",
        _fake_extract_lineage_json,
    )

    payload = rust_bundle_bridge.extract_lineage_from_plan(object())
    assert payload["required_columns_by_dataset"] == {"events": ["id"]}
    assert payload["filters"] == ["id > 1"]


def test_extract_lineage_from_plan_requires_string_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rust bridge raises when extension payload is not JSON text."""

    def _fake_extract_lineage_json(_plan: object) -> object:
        return 123

    monkeypatch.setattr(
        "datafusion_engine.extensions.datafusion_ext.extract_lineage_json",
        _fake_extract_lineage_json,
    )

    with pytest.raises(TypeError, match="non-string"):
        _ = rust_bundle_bridge.extract_lineage_from_plan(object())


def test_reporting_extract_lineage_uses_bridge_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lineage reporting maps Rust bridge payload to LineageReport."""
    monkeypatch.setattr(
        reporting,
        "extract_lineage_from_plan",
        lambda _plan: {
            "scans": [
                {
                    "dataset_name": "events",
                    "projected_columns": ["id"],
                    "pushed_filters": ["id > 1"],
                }
            ],
            "required_columns_by_dataset": {"events": ["id"]},
            "filters": ["id > 1"],
            "required_udfs": ["udf_a"],
            "required_rewrite_tags": ["tag_a"],
        },
    )

    report = reporting.extract_lineage(object())

    assert report.referenced_tables == ("events",)
    assert report.required_columns_by_dataset == {"events": ("id",)}
    assert report.required_udfs == ("udf_a",)
    assert report.required_rewrite_tags == ("tag_a",)
