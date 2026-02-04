"""RegistryFacade rollback semantics tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.catalog.provider_registry import RegistrationMetadata
from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
from datafusion_engine.registry_facade import RegistryFacade
from utils.registry_protocol import MutableRegistry

if TYPE_CHECKING:
    from datafusion import DataFrame


class FailingProviderRegistry(MutableRegistry[str, RegistrationMetadata]):
    """Provider registry that can fail after registering a location."""

    def __init__(self, *, fail: bool) -> None:
        super().__init__()
        self._fail = fail

    def register_location(
        self,
        *,
        name: str,
        location: DatasetLocation,
        overwrite: bool = False,
        cache_policy: object | None = None,
    ) -> DataFrame:
        """Register a dataset location, optionally raising to simulate failure.

        Returns
        -------
        DataFrame
            Placeholder provider object for tests.

        Raises
        ------
        RuntimeError
            Raised when the registry is configured to fail.
        """
        _ = (cache_policy, location)
        metadata = RegistrationMetadata(
            table_name=name,
            registration_time_ms=0,
            table_spec_hash="test",
            udf_snapshot_hash=None,
            delta_version=None,
        )
        self.register(name, metadata, overwrite=overwrite)
        if self._fail:
            msg = "boom"
            raise RuntimeError(msg)
        return cast("DataFrame", object())


def test_registry_facade_rolls_back_on_provider_failure() -> None:
    """Ensure registries roll back when provider registration fails."""
    dataset_catalog = DatasetCatalog()
    provider_registry = FailingProviderRegistry(fail=True)
    facade = RegistryFacade(
        dataset_catalog=dataset_catalog,
        provider_registry=provider_registry,
    )
    location = DatasetLocation(path="/tmp/datasets/events", format="delta")

    with pytest.raises(RuntimeError, match="boom"):
        facade.register_dataset_df(
            name="events",
            location=location,
            overwrite=True,
        )

    assert "events" not in dataset_catalog
    assert "events" not in provider_registry
