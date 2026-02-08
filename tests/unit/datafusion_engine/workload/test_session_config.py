"""Test workload-to-session-config mapping."""

from __future__ import annotations

from datafusion_engine.workload.classifier import (
    WorkloadClass,
    session_config_for_workload,
)


class TestSessionConfigForWorkload:
    """Test session_config_for_workload returns valid config dicts."""

    def test_returns_mapping(self) -> None:
        """Return type is a mapping of string key-value pairs."""
        config = session_config_for_workload(WorkloadClass.BATCH_INGEST)
        assert isinstance(config, dict)
        for key, value in config.items():
            assert isinstance(key, str), f"Key {key!r} is not a string"
            assert isinstance(value, str), f"Value for {key!r} is not a string"

    def test_batch_ingest_has_high_partitions(self) -> None:
        """Batch ingest workload should request high target partitions."""
        config = session_config_for_workload(WorkloadClass.BATCH_INGEST)
        key = "datafusion.execution.target_partitions"
        assert key in config
        assert int(config[key]) >= 4

    def test_interactive_query_has_low_partitions(self) -> None:
        """Interactive query workload should request few target partitions."""
        config = session_config_for_workload(WorkloadClass.INTERACTIVE_QUERY)
        key = "datafusion.execution.target_partitions"
        assert key in config
        assert int(config[key]) <= 4

    def test_compile_replay_has_single_partition(self) -> None:
        """Compile replay workload should use a single partition."""
        config = session_config_for_workload(WorkloadClass.COMPILE_REPLAY)
        key = "datafusion.execution.target_partitions"
        assert key in config
        assert int(config[key]) == 1

    def test_incremental_update_has_moderate_partitions(self) -> None:
        """Incremental update workload should use moderate partitions."""
        config = session_config_for_workload(WorkloadClass.INCREMENTAL_UPDATE)
        key = "datafusion.execution.target_partitions"
        assert key in config
        partitions = int(config[key])
        assert 2 <= partitions <= 8

    def test_batch_size_present(self) -> None:
        """All workload configs include a batch size setting."""
        for wc in WorkloadClass:
            config = session_config_for_workload(wc)
            key = "datafusion.execution.batch_size"
            assert key in config, f"{wc} missing {key}"
            assert int(config[key]) > 0

    def test_repartition_keys_are_lowercase_booleans(self) -> None:
        """Repartition settings are lowercase boolean strings."""
        config = session_config_for_workload(WorkloadClass.BATCH_INGEST)
        agg_key = "datafusion.optimizer.repartition_aggregations"
        scan_key = "datafusion.execution.repartition_file_scans"
        if agg_key in config:
            assert config[agg_key] in {"true", "false"}
        if scan_key in config:
            assert config[scan_key] in {"true", "false"}

    def test_batch_enables_repartition(self) -> None:
        """Batch ingest enables repartition for aggregations and file scans."""
        config = session_config_for_workload(WorkloadClass.BATCH_INGEST)
        assert config.get("datafusion.optimizer.repartition_aggregations") == "true"
        assert config.get("datafusion.execution.repartition_file_scans") == "true"

    def test_interactive_disables_repartition(self) -> None:
        """Interactive query disables repartition to reduce latency."""
        config = session_config_for_workload(WorkloadClass.INTERACTIVE_QUERY)
        assert config.get("datafusion.optimizer.repartition_aggregations") == "false"
        assert config.get("datafusion.execution.repartition_file_scans") == "false"

    def test_sort_spill_reservation_present(self) -> None:
        """All workload configs include sort spill reservation bytes."""
        for wc in WorkloadClass:
            config = session_config_for_workload(wc)
            key = "datafusion.execution.sort_spill_reservation_bytes"
            assert key in config, f"{wc} missing {key}"
            assert int(config[key]) > 0

    def test_memory_fraction_present(self) -> None:
        """All workload configs include advisory memory fraction."""
        for wc in WorkloadClass:
            config = session_config_for_workload(wc)
            key = "codeanatomy.workload.memory_fraction"
            assert key in config, f"{wc} missing {key}"
            fraction = float(config[key])
            assert 0.0 < fraction <= 1.0

    def test_batch_has_higher_memory_than_interactive(self) -> None:
        """Batch ingest requests higher memory fraction than interactive."""
        batch = session_config_for_workload(WorkloadClass.BATCH_INGEST)
        interactive = session_config_for_workload(WorkloadClass.INTERACTIVE_QUERY)
        batch_frac = float(batch["codeanatomy.workload.memory_fraction"])
        interactive_frac = float(interactive["codeanatomy.workload.memory_fraction"])
        assert batch_frac > interactive_frac

    def test_every_class_produces_config(self) -> None:
        """Every WorkloadClass produces a non-empty config dictionary."""
        for wc in WorkloadClass:
            config = session_config_for_workload(wc)
            assert len(config) > 0, f"{wc} produced empty config"

    def test_all_keys_use_namespaced_prefixes(self) -> None:
        """All config keys use either datafusion.* or codeanatomy.* namespace."""
        for wc in WorkloadClass:
            config = session_config_for_workload(wc)
            for key in config:
                assert key.startswith(("datafusion.", "codeanatomy.")), (
                    f"Key {key!r} does not use a namespaced prefix"
                )
