"""Relationship output metadata contracts for view-driven pipelines."""

from __future__ import annotations

from functools import cache
from typing import Final

from arrow_utils.core.ordering import OrderingLevel
from arrow_utils.schema.metadata import (
    SchemaMetadataSpec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.schema_registry import SCHEMA_META_NAME, SCHEMA_META_VERSION
from relspec.view_defs import (
    REL_CALLSITE_QNAME_OUTPUT,
    REL_CALLSITE_SYMBOL_OUTPUT,
    REL_DEF_SYMBOL_OUTPUT,
    REL_IMPORT_SYMBOL_OUTPUT,
    REL_NAME_SYMBOL_OUTPUT,
)
from relspec.view_defs import (
    RELATION_OUTPUT_NAME as RELATION_OUTPUT_VIEW,
)

REL_NAME_SYMBOL_NAME: Final[str] = REL_NAME_SYMBOL_OUTPUT
REL_IMPORT_SYMBOL_NAME: Final[str] = REL_IMPORT_SYMBOL_OUTPUT
REL_DEF_SYMBOL_NAME: Final[str] = REL_DEF_SYMBOL_OUTPUT
REL_CALLSITE_SYMBOL_NAME: Final[str] = REL_CALLSITE_SYMBOL_OUTPUT
REL_CALLSITE_QNAME_NAME: Final[str] = REL_CALLSITE_QNAME_OUTPUT
RELATION_OUTPUT_NAME: Final[str] = RELATION_OUTPUT_VIEW

RELATION_OUTPUT_ORDERING_KEYS: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("bstart", "ascending"),
    ("bend", "ascending"),
    ("edge_owner_file_id", "ascending"),
    ("src", "ascending"),
    ("dst", "ascending"),
    ("task_priority", "ascending"),
    ("task_name", "ascending"),
)


def _schema_version_from_name(name: str) -> int | None:
    _, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return int(suffix)
    return None


def _schema_metadata(name: str) -> dict[bytes, bytes]:
    metadata: dict[bytes, bytes] = {SCHEMA_META_NAME: name.encode("utf-8")}
    version = _schema_version_from_name(name)
    if version is not None:
        metadata[SCHEMA_META_VERSION] = str(version).encode("utf-8")
    return metadata


def _metadata_spec(name: str) -> SchemaMetadataSpec:
    return SchemaMetadataSpec(schema_metadata=_schema_metadata(name))


def _metadata_spec_with_ordering(
    name: str,
    *,
    ordering_keys: tuple[tuple[str, str], ...],
) -> SchemaMetadataSpec:
    base = _metadata_spec(name)
    ordering = ordering_metadata_spec(
        OrderingLevel.EXPLICIT,
        keys=ordering_keys,
    )
    return merge_metadata_specs(base, ordering)


@cache
def rel_name_symbol_metadata_spec() -> SchemaMetadataSpec:
    """Metadata spec for name-symbol relationship outputs.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for name-symbol outputs.
    """
    return _metadata_spec(REL_NAME_SYMBOL_NAME)


@cache
def rel_import_symbol_metadata_spec() -> SchemaMetadataSpec:
    """Metadata spec for import-symbol relationship outputs.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for import-symbol outputs.
    """
    return _metadata_spec(REL_IMPORT_SYMBOL_NAME)


@cache
def rel_def_symbol_metadata_spec() -> SchemaMetadataSpec:
    """Metadata spec for def-symbol relationship outputs.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for def-symbol outputs.
    """
    return _metadata_spec(REL_DEF_SYMBOL_NAME)


@cache
def rel_callsite_symbol_metadata_spec() -> SchemaMetadataSpec:
    """Metadata spec for callsite-symbol relationship outputs.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for callsite-symbol outputs.
    """
    return _metadata_spec(REL_CALLSITE_SYMBOL_NAME)


@cache
def rel_callsite_qname_metadata_spec() -> SchemaMetadataSpec:
    """Metadata spec for callsite-qname relationship outputs.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for callsite-qname outputs.
    """
    return _metadata_spec(REL_CALLSITE_QNAME_NAME)


@cache
def relation_output_metadata_spec() -> SchemaMetadataSpec:
    """Metadata spec for canonical relationship outputs.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for relation output rows.
    """
    return _metadata_spec_with_ordering(
        RELATION_OUTPUT_NAME,
        ordering_keys=RELATION_OUTPUT_ORDERING_KEYS,
    )


def relspec_metadata_spec(name: str) -> SchemaMetadataSpec:
    """Return the metadata spec for a relspec output view.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for the requested relspec output.

    Raises
    ------
    KeyError
        Raised when the relspec output name is unknown.
    """
    specs: dict[str, SchemaMetadataSpec] = {
        REL_NAME_SYMBOL_NAME: rel_name_symbol_metadata_spec(),
        REL_IMPORT_SYMBOL_NAME: rel_import_symbol_metadata_spec(),
        REL_DEF_SYMBOL_NAME: rel_def_symbol_metadata_spec(),
        REL_CALLSITE_SYMBOL_NAME: rel_callsite_symbol_metadata_spec(),
        REL_CALLSITE_QNAME_NAME: rel_callsite_qname_metadata_spec(),
        RELATION_OUTPUT_NAME: relation_output_metadata_spec(),
    }
    resolved = specs.get(name)
    if resolved is None:
        msg = f"Unknown relspec output: {name!r}."
        raise KeyError(msg)
    return resolved


__all__ = [
    "RELATION_OUTPUT_NAME",
    "RELATION_OUTPUT_ORDERING_KEYS",
    "REL_CALLSITE_QNAME_NAME",
    "REL_CALLSITE_SYMBOL_NAME",
    "REL_DEF_SYMBOL_NAME",
    "REL_IMPORT_SYMBOL_NAME",
    "REL_NAME_SYMBOL_NAME",
    "rel_callsite_qname_metadata_spec",
    "rel_callsite_symbol_metadata_spec",
    "rel_def_symbol_metadata_spec",
    "rel_import_symbol_metadata_spec",
    "rel_name_symbol_metadata_spec",
    "relation_output_metadata_spec",
    "relspec_metadata_spec",
]
