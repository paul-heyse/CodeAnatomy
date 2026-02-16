"""Tests for CDF-driven output resolution helpers."""

from __future__ import annotations

from semantics.cdf_resolution import outputs_from_changed_inputs, resolve_cdf_location
from semantics.registry import SEMANTIC_MODEL


class _ResolverStub:
    def __init__(self) -> None:
        self._locations = {
            "canonical": object(),
            "source": object(),
        }

    def location(self, name: str) -> object | None:
        return self._locations.get(name)


def test_resolve_cdf_location_prefers_canonical_name() -> None:
    """Canonical location is preferred over source fallback."""
    resolver = _ResolverStub()

    location = resolve_cdf_location(
        canonical="canonical",
        source="source",
        dataset_resolver=resolver,
    )

    assert location is resolver.location("canonical")


def test_resolve_cdf_location_skips_line_index() -> None:
    """file_line_index_v1 is not treated as a CDF input."""
    resolver = _ResolverStub()

    assert (
        resolve_cdf_location(
            canonical="file_line_index_v1",
            source="source",
            dataset_resolver=resolver,
        )
        is None
    )


def test_outputs_from_changed_inputs_empty_when_no_changes() -> None:
    """No changed inputs yields no impacted outputs."""
    assert outputs_from_changed_inputs(set(), model=SEMANTIC_MODEL) == set()
