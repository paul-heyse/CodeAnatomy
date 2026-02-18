"""Conformance matrix checks for pushdown-contract status semantics."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datafusion_engine.dataset.registration_provider import _provider_pushdown_contract
from tests.harness.profiles import ConformanceBackendConfig


class _MatrixProvider:
    def __init__(self, projection: object, predicate: object, limit: object) -> None:
        self._projection = projection
        self._predicate = predicate
        self._limit = limit

    def supports_projection_pushdown(self) -> object:
        return self._projection

    def supports_filters_pushdown(self, _filters: object | None = None) -> object:
        return self._predicate

    def supports_limit_pushdown(self) -> object:
        return self._limit


@pytest.mark.integration
@pytest.mark.parametrize(
    ("projection", "predicate", "limit", "expected_counts"),
    [
        (True, ["Exact"], True, {"exact": 3, "inexact": 0, "unsupported": 0}),
        ("Inexact", ["Inexact", "Exact"], False, {"exact": 0, "inexact": 2, "unsupported": 1}),
        (False, ["Unsupported"], False, {"exact": 0, "inexact": 0, "unsupported": 3}),
    ],
)
def test_pushdown_contract_status_matrix(
    tmp_path: Path,
    conformance_backend_config: ConformanceBackendConfig,
    projection: object,
    predicate: object,
    limit: object,
    expected_counts: dict[str, int],
) -> None:
    """Pushdown-contract counters preserve exact/inexact/unsupported semantics."""
    provider = _MatrixProvider(projection=projection, predicate=predicate, limit=limit)

    payload = _provider_pushdown_contract(provider)
    backend_context = conformance_backend_config.artifact_context(
        table_name="pushdown_contract",
        root_dir=tmp_path,
    )
    artifact_payload = {
        "backend": backend_context["backend"],
        "table_uri": backend_context["table_uri"],
        "storage_options": backend_context["storage_options"],
        "counts": payload["counts"],
        "statuses": payload["statuses"],
    }
    artifact_path = tmp_path / "pushdown_contract_matrix.json"
    artifact_path.write_text(
        json.dumps(artifact_payload, indent=2, sort_keys=True), encoding="utf-8"
    )

    assert payload["counts"] == expected_counts
    assert artifact_payload["backend"] == conformance_backend_config.kind
    assert isinstance(artifact_payload["table_uri"], str)
