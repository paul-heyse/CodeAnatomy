"""Tests for pushdown-contract normalization and metadata capture."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.dataset.registration_provider import (
    _normalize_pushdown_status,
    _provider_pushdown_contract,
    update_table_provider_pushdown_contract,
)
from datafusion_engine.tables.metadata import (
    TableProviderMetadata,
    record_table_provider_metadata,
    table_provider_metadata,
)


class _Provider:
    def supports_projection_pushdown(self) -> bool:
        return True

    def supports_filters_pushdown(self, _filters: object | None = None) -> list[str]:
        return ["Exact", "Inexact"]

    def supports_limit_pushdown(self) -> bool:
        return False


def test_normalize_pushdown_status_truth_table() -> None:
    """Status normalization preserves exact/inexact/unsupported semantics."""
    assert _normalize_pushdown_status(True) == "exact"
    assert _normalize_pushdown_status(False) == "unsupported"
    assert _normalize_pushdown_status("Exact") == "exact"
    assert _normalize_pushdown_status("Inexact") == "inexact"
    assert _normalize_pushdown_status("Unsupported") == "unsupported"


def test_provider_pushdown_contract_counts() -> None:
    """Provider contract captures status vector and aggregate counts."""
    payload = _provider_pushdown_contract(_Provider())

    assert payload["statuses"]["projection_pushdown"] == "exact"
    assert payload["statuses"]["predicate_pushdown"] == "inexact"
    assert payload["statuses"]["limit_pushdown"] == "unsupported"
    assert payload["counts"] == {"exact": 1, "inexact": 1, "unsupported": 1}
    assert payload["all_exact"] is False
    assert payload["has_inexact"] is True
    assert payload["has_unsupported"] is True


def test_update_table_provider_pushdown_contract_persists_metadata() -> None:
    """Pushdown-contract payload should be persisted into provider metadata."""
    ctx = SessionContext()
    record_table_provider_metadata(ctx, metadata=TableProviderMetadata(table_name="events"))

    update_table_provider_pushdown_contract(
        ctx,
        name="events",
        provider=_Provider(),
    )

    metadata = table_provider_metadata(ctx, table_name="events")
    assert metadata is not None
    assert metadata.pushdown_contract is not None
    assert metadata.pushdown_contract["statuses"]["predicate_pushdown"] == "inexact"
