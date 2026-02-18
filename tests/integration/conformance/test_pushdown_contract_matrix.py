"""Conformance matrix checks for pushdown-contract status semantics."""

from __future__ import annotations

import pytest

from datafusion_engine.dataset.registration_provider import _provider_pushdown_contract


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
    conformance_backend: str,
    projection: object,
    predicate: object,
    limit: object,
    expected_counts: dict[str, int],
) -> None:
    """Pushdown-contract counters preserve exact/inexact/unsupported semantics."""
    _ = conformance_backend
    provider = _MatrixProvider(projection=projection, predicate=predicate, limit=limit)

    payload = _provider_pushdown_contract(provider)

    assert payload["counts"] == expected_counts
