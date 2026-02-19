"""Cache manager contract mapping tests."""

from __future__ import annotations

from datafusion_engine.session.cache_manager_contract import (
    CacheManagerContractV1,
    cache_manager_contract_from_settings,
)

_TTL_SECONDS = 120


def test_cache_manager_contract_defaults() -> None:
    """Default contract payload has disabled cache manager and no limits."""
    contract = CacheManagerContractV1()
    assert contract.payload() == {
        "enabled": False,
        "metadata_cache_limit_bytes": None,
        "list_files_cache_limit_bytes": None,
        "list_files_cache_ttl_seconds": None,
    }


def test_cache_manager_contract_parses_runtime_settings() -> None:
    """Runtime settings are parsed into typed cache manager contract fields."""
    contract = cache_manager_contract_from_settings(
        enabled=True,
        settings={
            "datafusion.runtime.metadata_cache_limit": "64mb",
            "datafusion.runtime.list_files_cache_limit": "32mb",
            "datafusion.runtime.list_files_cache_ttl": "2m",
        },
    )
    assert contract.enabled is True
    assert contract.metadata_cache_limit_bytes == 64 * 1024 * 1024
    assert contract.list_files_cache_limit_bytes == 32 * 1024 * 1024
    assert contract.list_files_cache_ttl_seconds == _TTL_SECONDS
