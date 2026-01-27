"""Cache lineage correctness tests for strict causal policies."""

from __future__ import annotations

import logging
import sqlite3
import sys
import uuid
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType

from hamilton import driver
from hamilton.caching.stores.file import FileResultStore
from hamilton.caching.stores.sqlite import SQLiteMetadataStore
from hamilton.function_modifiers import dataloader

from hamilton_pipeline.cache_lineage import _lineage_rows_from_metadata_store

logging.getLogger("hamilton").setLevel(logging.ERROR)

_FIRST_RUN_VALUE = 1
_EXPECTED_RECOMPUTE_COUNT = 2


def _make_cache_module(counter: dict[str, int]) -> ModuleType:
    module_name = f"hamilton_cache_test_{uuid.uuid4().hex}"
    module = ModuleType(module_name)
    sys.modules[module_name] = module

    @dataloader()
    def external_value() -> tuple[int, dict[str, object]]:
        counter["value"] += 1
        return counter["value"], {"kind": "external"}

    def derived_value(external_value: int) -> int:
        return external_value

    external_value.__module__ = module_name
    derived_value.__module__ = module_name
    module.__dict__["external_value"] = external_value
    module.__dict__["derived_value"] = derived_value
    module.__dict__["__all__"] = ["external_value", "derived_value"]
    return module


def _metadata_db_path(meta_path: Path) -> Path:
    return meta_path / "metadata_store.db" if meta_path.is_dir() else meta_path


def _latest_run_id(meta_path: Path) -> str:
    db_path = _metadata_db_path(meta_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("select run_id from run_ids order by id desc limit 1").fetchone()
    if row is None:
        msg = "Cache metadata store does not contain any runs."
        raise ValueError(msg)
    return str(row[0])


def _driver_with_cache(module: ModuleType, cache_dir: Path) -> driver.Driver:
    cache_dir.mkdir(parents=True, exist_ok=True)
    results_dir = cache_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    meta_store = SQLiteMetadataStore(path=str(cache_dir / "meta.sqlite"))
    result_store = FileResultStore(path=str(results_dir))
    return (
        driver.Builder()
        .allow_module_overrides()
        .with_modules(module)
        .with_cache(
            metadata_store=meta_store,
            result_store=result_store,
            default_behavior="default",
            default_loader_behavior="recompute",
            default_saver_behavior="default",
            log_to_file=False,
        )
        .build()
    )


def test_external_dataloaders_recompute_even_with_cache(tmp_path: Path) -> None:
    """Dataloader nodes must recompute across runs under strict policies."""
    counter = {"value": 0}
    module = _make_cache_module(counter)
    driver_instance = _driver_with_cache(module, tmp_path / "cache")
    first = driver_instance.execute(["derived_value"])["derived_value"]
    second = driver_instance.execute(["derived_value"])["derived_value"]
    assert first == _FIRST_RUN_VALUE
    assert second == _EXPECTED_RECOMPUTE_COUNT
    assert counter["value"] == _EXPECTED_RECOMPUTE_COUNT


def test_metadata_store_lineage_rows_are_deterministic_and_decodable(
    tmp_path: Path,
) -> None:
    """Metadata-store lineage rows should be deterministic and decodable."""
    counter = {"value": 0}
    module = _make_cache_module(counter)
    cache_dir = tmp_path / "lineage_cache"
    driver_instance = _driver_with_cache(module, cache_dir)
    driver_instance.execute(["derived_value"])
    run_id = _latest_run_id(cache_dir / "meta.sqlite")
    rows_a, errors_a = _lineage_rows_from_metadata_store(
        cache=driver_instance.cache,
        run_id=run_id,
        plan_signature="plan:test",
    )
    rows_b, errors_b = _lineage_rows_from_metadata_store(
        cache=driver_instance.cache,
        run_id=run_id,
        plan_signature="plan:test",
    )
    assert errors_a == 0
    assert errors_b == 0
    assert rows_a
    assert rows_a == rows_b
    node_names: list[str] = []
    for row in rows_a:
        node_name = row.get("node_name")
        if isinstance(node_name, str):
            node_names.append(node_name)
    assert node_names == sorted(node_names)
    for row in rows_a:
        dependencies = row.get("dependencies_data_versions")
        assert isinstance(dependencies, Mapping)
        for key, value in dependencies.items():
            assert isinstance(key, str)
            assert isinstance(value, str)
