"""Hamilton materializers for CodeAnatomy telemetry and artifact capture."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hamilton.function_modifiers import source
from hamilton.io import materialization
from hamilton.io.data_adapters import DataSaver
from hamilton.registry import register_adapter

from datafusion_engine.param_tables import ParamTableArtifact
from hamilton_pipeline.modules.outputs import (
    OutputPlanContext,
    OutputRuntimeContext,
    delta_output_specs,
)


@dataclass(frozen=True)
class TableSummarySaver(DataSaver):
    """Capture summary metadata for table-like outputs."""

    output_runtime_context: OutputRuntimeContext
    output_plan_context: OutputPlanContext
    dataset_name: str
    materialized_name: str
    materialization: str = "delta"

    @classmethod
    def applicable_types(cls) -> tuple[type, ...]:
        return (object,)

    @classmethod
    def applies_to(cls, type_: type) -> bool:
        """Return True when this saver can handle the given output type.

        Returns
        -------
        bool
            True when the saver applies to the output type.
        """
        _ = type_
        return True

    @classmethod
    def name(cls) -> str:
        return "codeanatomy_table_summary"

    def save_data(self, data: object) -> dict[str, Any]:
        schema_names = _schema_names(data)
        output_dir = _output_dir(self.output_runtime_context)
        path = str(Path(output_dir) / self.dataset_name) if output_dir is not None else None
        return {
            "dataset_name": self.dataset_name,
            "materialization": self.materialization,
            "materialized_name": self.materialized_name,
            "path": path,
            "rows": _row_count(data),
            "columns": schema_names,
            "column_count": len(schema_names),
            "plan_signature": self.output_plan_context.plan_signature,
            "run_id": self.output_plan_context.run_id,
        }


@dataclass(frozen=True)
class ParamTableSummarySaver(DataSaver):
    """Capture summary metadata for parameter tables."""

    output_runtime_context: OutputRuntimeContext
    materialization: str = "delta"

    @classmethod
    def applicable_types(cls) -> tuple[type, ...]:
        return (object,)

    @classmethod
    def applies_to(cls, type_: type) -> bool:
        """Return True when this saver can handle the given output type.

        Returns
        -------
        bool
            True when the saver applies to the output type.
        """
        _ = type_
        return True

    @classmethod
    def name(cls) -> str:
        return "codeanatomy_param_table_summary"

    def save_data(self, data: object) -> dict[str, Any]:
        if not isinstance(data, Mapping):
            msg = "ParamTableSummarySaver expected a mapping of param table artifacts."
            raise TypeError(msg)
        output_dir = _param_output_dir(self.output_runtime_context)
        tables: list[dict[str, object]] = []
        for logical_name, artifact in data.items():
            if not isinstance(artifact, ParamTableArtifact):
                continue
            entry: dict[str, object] = {
                "logical_name": logical_name,
                "rows": int(artifact.rows),
                "schema_identity_hash": artifact.schema_identity_hash,
                "signature": artifact.signature,
            }
            if output_dir is not None:
                entry["path"] = str(output_dir / logical_name)
            tables.append(entry)
        return {
            "materialization": self.materialization,
            "table_count": len(tables),
            "tables": tables,
        }


register_adapter(TableSummarySaver)
register_adapter(ParamTableSummarySaver)


def build_hamilton_materializers() -> list[materialization.MaterializerFactory]:
    """Return materializers used for Hamilton UI artifact capture.

    Returns
    -------
    list[materialization.MaterializerFactory]
        Materializer factories for table and parameter outputs.
    """
    return [
        *[
            materialization.to.codeanatomy_table_summary(
                id=f"materialize_{spec.dataset_name}",
                dependencies=[spec.table_node],
                output_runtime_context=source("output_runtime_context"),
                output_plan_context=source("output_plan_context"),
                dataset_name=spec.dataset_name,
                materialized_name=spec.materialized_name,
                materialization=spec.materialization,
            )
            for spec in delta_output_specs()
        ],
        materialization.to.codeanatomy_param_table_summary(
            id="materialize_param_tables",
            dependencies=["param_table_artifacts"],
            output_runtime_context=source("output_runtime_context"),
            materialization="delta",
        ),
    ]


def _row_count(value: object) -> int:
    rows = getattr(value, "num_rows", None)
    if isinstance(rows, bool):
        return 0
    if isinstance(rows, int):
        return rows
    return 0


def _schema_names(value: object) -> list[str]:
    schema = getattr(value, "schema", None)
    names = getattr(schema, "names", None)
    if isinstance(names, list):
        return [str(item) for item in names]
    if isinstance(names, tuple):
        return [str(item) for item in names]
    return []


def _output_dir(runtime_context: OutputRuntimeContext) -> str | None:
    output_config = runtime_context.output_config
    base = output_config.output_dir or output_config.work_dir
    if not base:
        return None
    return str(base)


def _param_output_dir(runtime_context: OutputRuntimeContext) -> Path | None:
    output_config = runtime_context.output_config
    base = output_config.work_dir or output_config.output_dir
    if not base:
        return None
    return Path(base) / "params"


__all__ = ["build_hamilton_materializers"]
