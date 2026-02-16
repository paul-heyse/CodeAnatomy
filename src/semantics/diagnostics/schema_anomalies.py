"""Schema anomaly diagnostics for semantic output contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.schema.introspection import table_names_snapshot
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span
from semantics.diagnostics.quality_metrics import _empty_table

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

_SCHEMA_ANOMALY_SCHEMA: tuple[tuple[str, pa.DataType], ...] = (
    ("view_name", pa.string()),
    ("violation_type", pa.string()),
    ("column_name", pa.string()),
    ("detail", pa.string()),
)


def build_schema_anomalies_view(ctx: SessionContext) -> DataFrame:
    """Build schema anomaly diagnostics view.

    Returns:
        DataFrame: Schema anomaly rows for available semantic output datasets.
    """
    from datafusion_engine.schema.catalog_contracts import contract_violations_for_schema
    from datafusion_engine.schema.contracts import schema_contract_from_dataset_spec
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
    from semantics.catalog.dataset_specs import dataset_specs

    with stage_span(
        "semantics.build_schema_anomalies_view",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
    ):
        available = table_names_snapshot(ctx)
        rows: list[dict[str, object]] = []
        for spec in dataset_specs():
            from schema_spec.contracts import dataset_spec_name

            name = dataset_spec_name(spec)
            if name not in available:
                continue
            df = ctx.table(name)
            schema = arrow_schema_from_df(df)
            contract = schema_contract_from_dataset_spec(name=name, spec=spec)
            violations = contract_violations_for_schema(contract=contract, schema=schema)
            rows.extend(
                [
                    {
                        "view_name": name,
                        "violation_type": violation.violation_type.value,
                        "column_name": violation.column_name,
                        "detail": str(violation),
                    }
                    for violation in violations
                ]
            )
        if not rows:
            return _empty_table(ctx, _SCHEMA_ANOMALY_SCHEMA)
        table = pa.Table.from_pylist(rows, schema=pa.schema(_SCHEMA_ANOMALY_SCHEMA))
        return ctx.from_arrow(table)


__all__ = ["build_schema_anomalies_view"]
