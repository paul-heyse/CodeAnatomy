"""Contract-driven rule synthesis helpers."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from arrowdsl.io.ipc import ipc_table
from relspec.rules.definitions import RuleDefinition
from relspec.rules.spec_tables import rule_definitions_from_table
from relspec.schema_context import RelspecSchemaContext

RULE_DEFINITIONS_META_KEY = b"relspec.rule_definitions_ipc"


def rules_from_contracts(context: RelspecSchemaContext) -> tuple[RuleDefinition, ...]:
    """Return rule definitions derived from dataset schema metadata.

    Parameters
    ----------
    context
        DataFusion schema context to scan for metadata-driven rules.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Rule definitions extracted from dataset metadata.

    Raises
    ------
    KeyError
        Raised when a dataset schema cannot be resolved from DataFusion.
    """
    collected: list[RuleDefinition] = []
    for name in context.dataset_names():
        schema = context.dataset_schema(name)
        if schema is None:
            msg = f"Contract rule extraction missing schema for {name!r}."
            raise KeyError(msg)
        meta = schema.metadata or {}
        collected.extend(_rules_from_metadata(meta, dataset_name=name))
    return tuple(collected)


def _rules_from_metadata(
    meta: Mapping[bytes, bytes],
    *,
    dataset_name: str,
) -> tuple[RuleDefinition, ...]:
    payload = meta.get(RULE_DEFINITIONS_META_KEY)
    if payload is None:
        return ()
    try:
        table = ipc_table(payload)
    except (OSError, ValueError, pa.ArrowInvalid) as exc:
        msg = f"Failed to decode contract rule definitions for {dataset_name!r}."
        raise ValueError(msg) from exc
    try:
        rules = rule_definitions_from_table(table)
    except (TypeError, ValueError) as exc:
        msg = f"Failed to parse contract rule definitions for {dataset_name!r}."
        raise ValueError(msg) from exc
    return tuple(rules)


__all__ = ["RULE_DEFINITIONS_META_KEY", "rules_from_contracts"]
