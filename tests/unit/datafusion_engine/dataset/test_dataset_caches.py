"""Tests for dataset cache helpers."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.dataset.registration_core import DatasetCaches, cached_dataset_names


def test_cached_dataset_names_uses_injected_caches() -> None:
    """Cached dataset names are read from provided cache container."""
    ctx_one = SessionContext()
    ctx_two = SessionContext()
    caches = DatasetCaches()

    caches.cached_datasets[ctx_one] = {"alpha", "beta"}
    caches.cached_datasets[ctx_two] = {"gamma"}

    assert cached_dataset_names(ctx_one, caches=caches) == ("alpha", "beta")
    assert cached_dataset_names(ctx_two, caches=caches) == ("gamma",)


def test_dataset_caches_are_isolated_instances() -> None:
    """Separate DatasetCaches instances do not share cache state."""
    ctx = SessionContext()
    first = DatasetCaches()
    second = DatasetCaches()

    first.cached_datasets[ctx] = {"only_first"}

    assert cached_dataset_names(ctx, caches=first) == ("only_first",)
    assert cached_dataset_names(ctx, caches=second) == ()
