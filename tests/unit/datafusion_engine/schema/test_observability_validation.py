"""Tests for observability schema/runtime validation adapters."""

from __future__ import annotations

import pyarrow as pa
import pytest
from datafusion import SessionContext

from datafusion_engine.schema import observability_validation


def test_validate_observability_schema_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schema validation delegates to metadata validator."""
    calls: list[pa.Schema] = []

    def _fake_validate(schema: pa.Schema) -> None:
        calls.append(schema)

    monkeypatch.setattr(observability_validation, "validate_schema_metadata", _fake_validate)
    schema = pa.schema([])
    observability_validation.validate_observability_schema(schema)
    assert calls == [schema]


def test_validate_observability_runtime_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime validation delegates to semantic/runtime checks."""
    calls: list[str] = []
    monkeypatch.setattr(
        observability_validation, "validate_semantic_types", lambda _ctx: calls.append("semantic")
    )
    monkeypatch.setattr(
        observability_validation,
        "validate_required_engine_functions",
        lambda _ctx: calls.append("required"),
    )
    monkeypatch.setattr(
        observability_validation,
        "validate_udf_info_schema_parity",
        lambda _ctx: calls.append("udf"),
    )

    observability_validation.validate_observability_runtime(SessionContext())
    assert calls == ["semantic", "required", "udf"]
